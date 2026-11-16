use std::ffi::CString;
use std::sync::Once;
use switch_dpdk_sys::*;

static EAL_INIT: Once = Once::new();
static mut EAL_RESULT: bool = false;

fn ensure_eal_init() -> bool {
    EAL_INIT.call_once(|| {
        let cpu = switch_dpdk_sys::only_allowed_cpu().unwrap_or_else(|error| panic!("{error}"));
        let lcores = format!("--lcores=0@{cpu}");
        let storage = ["test", "--no-pci", "--no-huge", lcores.as_str()]
            .into_iter()
            .map(|arg| CString::new(arg).unwrap().into_bytes_with_nul())
            .collect::<Vec<_>>();
        let storage = Box::leak(storage.into_boxed_slice());
        let mut argv: Vec<*mut std::ffi::c_char> = storage
            .iter_mut()
            .map(|arg| arg.as_mut_ptr().cast())
            .collect();

        let ret = unsafe { rte_eal_init(argv.len() as i32, argv.as_mut_ptr()) };
        unsafe { EAL_RESULT = ret >= 0 };
        if ret < 0 {
            eprintln!("EAL init failed with code {}", ret);
        }
    });
    unsafe { EAL_RESULT }
}

#[switch_test_support::pinned_test]
fn test_pool_create_and_alloc() {
    assert!(ensure_eal_init(), "EAL init failed");

    let pool_name = CString::new("test_pool").unwrap();
    let pool = unsafe {
        rte_pktmbuf_pool_create(
            pool_name.as_ptr(),
            1024,  // n
            256,   // cache_size
            8,     // priv_size: 8 bytes for cache pointer
            RTE_MBUF_DEFAULT_BUF_SIZE as u16,  // data_room_size
            rte_socket_id() as i32,  // socket_id
        )
    };

    assert!(!pool.is_null(), "Failed to create mempool");

    // Get the cache for the EAL owner on this persistent test worker
    let cache = unsafe { switch_get_pool_cache(pool) };
    assert!(!cache.is_null(), "Failed to get pool cache");

    // Initialize cache pointers on all mbufs in the pool
    unsafe { switch_pool_init_cache_ptrs(pool, cache) };

    // Allocate an mbuf using fast path (sets cache pointer in priv_data)
    let mbuf = unsafe { switch_pktmbuf_alloc_fast(pool, cache) };
    assert!(!mbuf.is_null(), "Failed to allocate mbuf");

    // Test our inline methods
    let m = unsafe { &mut *mbuf };

    // Check initial state
    assert_eq!(m.refcnt(), 1);
    assert_eq!(m.data_off(), RTE_PKTMBUF_HEADROOM as u16);
    assert_eq!(m.data_len(), 0);
    assert_eq!(m.pkt_len(), 0);

    // Test refcount manipulation
    m.refcnt_inc();
    assert_eq!(m.refcnt(), 2);
    assert!(!m.refcnt_dec());  // Should return false (not zero)
    assert_eq!(m.refcnt(), 1);

    // Test prepend
    m.set_data_len(100);
    m.set_pkt_len(100);
    let ptr = m.prepend(20);
    assert!(ptr.is_some());
    assert_eq!(m.data_off(), RTE_PKTMBUF_HEADROOM as u16 - 20);
    assert_eq!(m.data_len(), 120);
    assert_eq!(m.pkt_len(), 120);

    // Test adjust (trim front)
    assert!(m.adjust(10));
    assert_eq!(m.data_off(), RTE_PKTMBUF_HEADROOM as u16 - 10);
    assert_eq!(m.data_len(), 110);

    // Test trim (trim back)
    assert!(m.trim(10));
    assert_eq!(m.data_len(), 100);

    // Free the mbuf (uses cache pointer set during alloc)
    unsafe { rte_pktmbuf_free(mbuf) };

    // Check pool usage
    let in_use = unsafe { rte_mempool_in_use_count(pool) };
    assert_eq!(in_use, 0, "Pool should have no mbufs in use after free");

    // Free the pool
    unsafe { rte_mempool_free(pool) };
}

/// rte_eth_read_clock (an experimental symbol) links, and a PMD without a clock (net_ring, like
/// net_tap) refuses it: switch-nic then logs `phc_check: unsupported`.
#[switch_test_support::pinned_test]
fn read_clock_without_a_clock() {
    assert!(ensure_eal_init(), "EAL init failed");
    let mut clock = 0;
    assert_eq!(unsafe { rte_eth_read_clock(u16::MAX, &mut clock) }, -libc::ENODEV);
    let rings: Vec<_> = [c"read_clock_rx", c"read_clock_tx"]
        .iter()
        .map(|name| unsafe { rte_ring_create(name.as_ptr(), 8, -1, RING_F_SP_ENQ | RING_F_SC_DEQ) })
        .collect();
    assert!(rings.iter().all(|ring| !ring.is_null()));
    let port = unsafe { rte_eth_from_rings(c"read_clock".as_ptr(), &rings[0], 1, &rings[1], 1, 0) };
    assert!(port >= 0, "rte_eth_from_rings: {port}");
    let port = port as u16;
    assert_eq!(unsafe { rte_eth_read_clock(port, &mut clock) }, -(ENOTSUP as i32));
    assert_eq!(unsafe { rte_eth_dev_close(port) }, 0);
    for ring in rings {
        unsafe { rte_ring_free(ring) };
    }
}
