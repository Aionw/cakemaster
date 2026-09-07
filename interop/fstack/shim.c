// Linux-facing ABI guard for coro-rpc's experimental F-Stack transport.
#include <errno.h>
#include <rte_eal.h>
#include <rte_lcore.h>
#include <stdint.h>

#include "ff_api.h"
#include "ff_config.h"

uint32_t cakemaster_fstack_abi(void) { return 1; }

int cakemaster_fstack_init(const char *config) {
    char *argv[] = {"coro-rpc-fstack",     "--conf",      (char *)config,
                    "--proc-type=primary", "--proc-id=0", NULL};
    int result = ff_init((int)(sizeof(argv) / sizeof(argv[0])) - 1, argv);
    if (result < 0)
        return result;
    // Rust's callback and sockets are thread-affine, not shared across lcores.
    if (ff_global_cfg.dpdk.nb_procs != 1 || ff_global_cfg.dpdk.thread_mode ||
        rte_lcore_count() != 1 || rte_lcore_id() != rte_get_main_lcore() ||
        rte_eal_process_type() != RTE_PROC_PRIMARY) {
        errno = ENOTSUP;
        return -1;
    }
    return 0;
}
