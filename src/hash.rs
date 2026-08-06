//! The 32-bit MD5 projection used by yalantinglibs.

use md5::{Digest, Md5};

/// Returns the same 32-bit value as
/// `struct_pack::MD5::MD5Hash32Constexpr`.
pub(crate) fn md5_hash32(input: &[u8]) -> u32 {
    let digest = Md5::digest(input);
    u32::from_be_bytes(digest[..4].try_into().expect("MD5 has at least four bytes"))
}

/// Calculates a coro_rpc route ID from the exact qualified C++ function name.
///
/// A global C++ function named `echo` uses `function_id("echo")`; a namespaced
/// function normally uses a name such as `my_namespace::echo`.
pub fn function_id(qualified_function_name: &str) -> u32 {
    md5_hash32(qualified_function_name.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_upstream_function_ids() {
        assert_eq!(function_id("echo"), 0xcbb1_1ed8);
        assert_eq!(function_id("add"), 0x34ec_78fc);
    }
}
