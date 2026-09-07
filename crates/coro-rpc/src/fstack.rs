//! Experimental single-thread F-Stack/DPDK server transport (Linux, IPv4).
//!
//! F-Stack descriptors are **not** OS descriptors: never register them with Tokio
//! epoll or close them with libc. All native calls stay on the initializing thread.
//! See `docs/dpdk.md` for native build, isolation and benchmark limitations.

use std::ffi::{CString, c_char, c_int, c_ulong, c_void};
use std::future::{Future, poll_fn};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures_util::Stream;
use futures_util::stream::FuturesUnordered;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::RpcServer;

static INITIALIZED: AtomicBool = AtomicBool::new(false);
const TICK_BUDGET: usize = 64;

/// Configuration for one polling core. Native EAL/port/TCP options live in the INI.
#[derive(Debug, Clone)]
pub struct Config {
    pub ini: PathBuf,
    /// Bounds accepted connections (and the cost of the experimental scan reactor).
    pub max_connections: usize,
}

impl Config {
    pub fn new(ini: impl Into<PathBuf>) -> Self {
        Self {
            ini: ini.into(),
            max_connections: 1024,
        }
    }
}

// Function signatures match the Linux-facing ff_api.h, not FreeBSD's socket ABI.
struct Api {
    init: unsafe extern "C" fn(*const c_char) -> c_int,
    run: unsafe extern "C" fn(unsafe extern "C" fn(*mut c_void) -> c_int, *mut c_void),
    stop: unsafe extern "C" fn(),
    socket: unsafe extern "C" fn(c_int, c_int, c_int) -> c_int,
    ioctl: unsafe extern "C" fn(c_int, c_ulong, ...) -> c_int,
    bind: unsafe extern "C" fn(c_int, *const libc::sockaddr, libc::socklen_t) -> c_int,
    listen: unsafe extern "C" fn(c_int, c_int) -> c_int,
    accept: unsafe extern "C" fn(c_int, *mut libc::sockaddr, *mut libc::socklen_t) -> c_int,
    setsockopt: unsafe extern "C" fn(c_int, c_int, c_int, *const c_void, libc::socklen_t) -> c_int,
    close: unsafe extern "C" fn(c_int) -> c_int,
    read: unsafe extern "C" fn(c_int, *mut c_void, usize) -> isize,
    write: unsafe extern "C" fn(c_int, *const c_void, usize) -> isize,
    shutdown: unsafe extern "C" fn(c_int, c_int) -> c_int,
}

/// Thread-affine native backend. Load and run it on a dedicated application thread.
pub struct FStack {
    api: Rc<Api>,
}

impl FStack {
    /// Load the native library produced by `interop/fstack/build.sh`.
    ///
    /// # Safety
    /// The library must be trusted and implement the bundled shim ABI and Linux
    /// ff_api.h ABI. Loading arbitrary shared objects executes native code.
    /// No other code in the process may initialize or call F-Stack/DPDK.
    pub unsafe fn load(library: impl AsRef<Path>) -> io::Result<Self> {
        // SAFETY: the caller guarantees the library's provenance and ABI.
        let library =
            unsafe { libloading::Library::new(library.as_ref()) }.map_err(io::Error::other)?;
        unsafe {
            let abi: libloading::Symbol<unsafe extern "C" fn() -> u32> = library
                .get(b"cakemaster_fstack_abi\0")
                .map_err(io::Error::other)?;
            if abi() != 1 {
                return Err(io::Error::other("unsupported F-Stack shim ABI"));
            }
            macro_rules! symbol {
                ($name:literal) => {
                    *library
                        .get(concat!($name, "\0").as_bytes())
                        .map_err(io::Error::other)?
                };
            }
            let api = Api {
                init: symbol!("cakemaster_fstack_init"),
                run: symbol!("ff_run"),
                stop: symbol!("ff_stop_run"),
                socket: symbol!("ff_socket"),
                ioctl: symbol!("ff_ioctl"),
                bind: symbol!("ff_bind"),
                listen: symbol!("ff_listen"),
                accept: symbol!("ff_accept"),
                setsockopt: symbol!("ff_setsockopt"),
                close: symbol!("ff_close"),
                read: symbol!("ff_read"),
                write: symbol!("ff_write"),
                shutdown: symbol!("ff_shutdown"),
            };
            // F-Stack has no complete teardown API. DPDK threads/TLS can reference
            // this code even after ff_run stops; keep the library loaded for life.
            std::mem::forget(library);
            Ok(Self { api: Rc::new(api) })
        }
    }

    /// Block this thread in ff_run, using the existing RPC driver on a local Tokio
    /// runtime. Must be called outside a Tokio runtime. Supports one run per process;
    /// F-Stack itself can exit the process if EAL/INI initialization fails.
    ///
    /// Shutdown drops all connections/handlers before returning. Background tasks
    /// spawned by handlers must not access native sockets. Only the server side
    /// uses DPDK; existing Rust/C++ TCP clients remain unchanged.
    pub fn run(
        self,
        config: Config,
        server: RpcServer,
        address: SocketAddrV4,
        shutdown: impl Future<Output = ()>,
    ) -> io::Result<()> {
        if config.max_connections == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_connections must be positive",
            ));
        }
        use std::os::unix::ffi::OsStrExt;
        let ini = CString::new(config.ini.as_os_str().as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        if INITIALIZED.swap(true, Ordering::AcqRel) {
            return Err(io::Error::other(
                "F-Stack may only be initialized once per process",
            ));
        }
        // SAFETY: all calls, socket ownership and the synchronous callback remain
        // on this thread; the shim rejects multi-core/thread-mode configurations.
        unsafe { cvt((self.api.init)(ini.as_ptr()))? };
        let handler = server.into_connection_handler();
        let listener = Socket::listen(self.api.clone(), address)?;
        let mut shutdown = std::pin::pin!(shutdown);
        let mut connections = FuturesUnordered::new();
        let application = poll_fn(move |cx| {
            if shutdown.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(()));
            }
            for _ in 0..TICK_BUDGET {
                if connections.len() >= config.max_connections {
                    break;
                }
                match listener.accept(handler.config().tcp_nodelay) {
                    Ok((socket, peer)) => connections.push(handler.serve(socket, peer)),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Poll::Ready(Err(error)),
                }
            }
            for _ in 0..TICK_BUDGET {
                match Pin::new(&mut connections).poll_next(cx) {
                    Poll::Ready(Some(Err(error))) => {
                        log::warn!(target: "coro_rpc::fstack", error:% = error; "RPC connection failed");
                    }
                    Poll::Ready(Some(Ok(()))) => {}
                    Poll::Pending | Poll::Ready(None) => break,
                }
            }
            Poll::Pending
        });
        let mut callback = Callback {
            runtime: &runtime,
            application: Some(Box::pin(application)),
            stop: self.api.stop,
            result: None,
            panic: None,
        };
        unsafe { (self.api.run)(on_tick, (&mut callback as *mut Callback<'_>).cast()) };
        if let Some(panic) = callback.panic {
            std::panic::resume_unwind(panic);
        }
        callback.result.unwrap_or(Ok(()))
    }
}

struct Callback<'a> {
    runtime: &'a tokio::runtime::Runtime,
    application: Option<Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>>,
    stop: unsafe extern "C" fn(),
    result: Option<io::Result<()>>,
    panic: Option<Box<dyn std::any::Any + Send>>,
}

unsafe extern "C" fn on_tick(argument: *mut c_void) -> c_int {
    // SAFETY: ff_run calls synchronously on the owning thread and does not retain
    // the pointer after returning. Rust panics must not unwind through C frames.
    let callback = unsafe { &mut *argument.cast::<Callback<'_>>() };
    if callback.result.is_some() || callback.panic.is_some() {
        return 0;
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        callback.runtime.block_on(async {
            let result =
                poll_fn(|cx| Poll::Ready(callback.application.as_mut().unwrap().as_mut().poll(cx)))
                    .await;
            // Yield AFTER polling: handlers may exhaust Tokio's cooperative
            // budget and defer their wakers. Returning from block_on immediately
            // would discard those deferred wakes and stall large pipelines.
            // This also gives timers/signals a nonblocking reactor turn.
            tokio::task::yield_now().await;
            result
        })
    })) {
        Ok(Poll::Pending) => return 0,
        Ok(Poll::Ready(result)) => callback.result = Some(result),
        Err(panic) => callback.panic = Some(panic),
    }
    // ff_run may call rte_eal_cleanup before returning. All sockets and handler
    // futures must therefore be dropped HERE, before stopping the native loop.
    let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _entered = callback.runtime.enter();
        drop(callback.application.take());
    }));
    if let Err(panic) = cleanup {
        callback.panic = Some(panic);
    }
    unsafe { (callback.stop)() };
    0
}

// Rc makes descriptors and their connection futures !Send and !Sync. No raw fd
// escapes this module; Drop always uses ff_close on the owning thread.
struct Socket {
    fd: c_int,
    api: Rc<Api>,
}

impl Socket {
    fn nonblocking(&self) -> io::Result<()> {
        let enabled: c_int = 1;
        unsafe { cvt((self.api.ioctl)(self.fd, libc::FIONBIO, &enabled))? };
        Ok(())
    }

    fn listen(api: Rc<Api>, address: SocketAddrV4) -> io::Result<Self> {
        let fd = unsafe { cvt((api.socket)(libc::AF_INET, libc::SOCK_STREAM, 0))? };
        let socket = Self { fd, api };
        socket.nonblocking()?;
        let address = sockaddr(address);
        unsafe {
            cvt((socket.api.bind)(
                fd,
                (&address as *const libc::sockaddr_in).cast(),
                std::mem::size_of_val(&address) as libc::socklen_t,
            ))?;
            cvt((socket.api.listen)(fd, 1024))?;
        }
        Ok(socket)
    }

    fn accept(&self, nodelay: bool) -> io::Result<(Self, SocketAddr)> {
        let mut address = sockaddr(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
        let mut length = std::mem::size_of_val(&address) as libc::socklen_t;
        let fd = unsafe {
            cvt((self.api.accept)(
                self.fd,
                (&mut address as *mut libc::sockaddr_in).cast(),
                &mut length,
            ))?
        };
        let socket = Self {
            fd,
            api: self.api.clone(),
        };
        socket.nonblocking()?;
        let enabled = c_int::from(nodelay);
        unsafe {
            cvt((self.api.setsockopt)(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&enabled as *const c_int).cast(),
                std::mem::size_of_val(&enabled) as libc::socklen_t,
            ))?;
        }
        if address.sin_family != libc::AF_INET as libc::sa_family_t
            || length as usize != std::mem::size_of_val(&address)
        {
            return Err(io::Error::other("invalid IPv4 peer address from F-Stack"));
        }
        Ok((
            socket,
            SocketAddrV4::new(
                Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(address.sin_port),
            )
            .into(),
        ))
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { (self.api.close)(self.fd) };
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let target = buffer.initialize_unfilled();
        let result = unsafe { (self.api.read)(self.fd, target.as_mut_ptr().cast(), target.len()) };
        match poll_io(result, cx) {
            Poll::Ready(Ok(count)) => {
                buffer.advance(count);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = unsafe { (self.api.write)(self.fd, buffer.as_ptr().cast(), buffer.len()) };
        poll_io(result, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // ff_write copies into TCP sendspace. Packet TX is driven by ff_run.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(unsafe { cvt((self.api.shutdown)(self.fd, libc::SHUT_WR)).map(|_| ()) })
    }
}

fn sockaddr(address: SocketAddrV4) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: address.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(address.ip().octets()),
        },
        sin_zero: [0; 8],
    }
}

fn cvt(result: c_int) -> io::Result<c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn poll_io(result: isize, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
    if result >= 0 {
        return Poll::Ready(Ok(result as usize));
    }
    let error = io::Error::last_os_error();
    if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) {
        // Deliberately simple scan reactor: reschedule only for the next bounded
        // RPC poll/DPDK tick, never loop on EAGAIN inside a socket operation.
        cx.waker().wake_by_ref();
        Poll::Pending
    } else {
        Poll::Ready(Err(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_sockaddr_uses_network_byte_order() {
        let address = sockaddr("192.0.2.17:19092".parse().unwrap());
        assert_eq!(address.sin_addr.s_addr.to_ne_bytes(), [192, 0, 2, 17]);
        assert_eq!(address.sin_port.to_ne_bytes(), 19092_u16.to_be_bytes());
    }

    unsafe extern "C" fn stop_noop() {}

    #[test]
    fn tick_preserves_cooperative_wakes_and_drives_timers() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut callback = Callback {
            runtime: &runtime,
            application: Some(Box::pin(async {
                futures_util::future::join_all((0..256).map(|_| async {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }))
                .await;
                Ok(())
            })),
            stop: stop_noop,
            result: None,
            panic: None,
        };
        let started = std::time::Instant::now();
        while callback.result.is_none() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(2),
                "deferred wakers were lost"
            );
            unsafe {
                on_tick((&mut callback as *mut Callback<'_>).cast());
            }
        }
        assert!(callback.result.unwrap().is_ok());
        assert!(callback.application.is_none());
    }

    #[test]
    fn callback_contains_panics_and_drops_application_before_stopping() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let marker = Rc::new(());
        let owned = marker.clone();
        let mut callback = Callback {
            runtime: &runtime,
            application: Some(Box::pin(async move {
                let _owned = owned;
                panic!("handler panic");
            })),
            stop: stop_noop,
            result: None,
            panic: None,
        };
        unsafe {
            on_tick((&mut callback as *mut Callback<'_>).cast());
        }
        assert!(callback.panic.is_some());
        assert!(callback.application.is_none());
        assert_eq!(Rc::strong_count(&marker), 1);
    }

    #[test]
    fn missing_library_is_an_error() {
        assert!(unsafe { FStack::load("/nonexistent/cakemaster-fstack.so") }.is_err());
    }
}
