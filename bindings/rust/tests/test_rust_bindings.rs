use awp_rs::{AsyncWorkerPool, AwpError, AwpRingMode, PoolBuilder};
use std::ffi::CStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
struct TradeEvent {
    price: f64,
    qty: f64,
    order_id: u64,
}

#[test]
fn test_pool_builder_and_zero_alloc_submit() {
    let processed = Arc::new(AtomicUsize::new(0));
    let proc_clone = processed.clone();

    let pool = PoolBuilder::new()
        .workers(4)
        .queue_capacity(256)
        .ring_mode(AwpRingMode::Mpsc)
        .supervisor(false)
        .build(move |frame| {
            assert_eq!(frame.feed(), "binance_trades");
            assert_eq!(frame.symbol(), "BTCUSDT");
            assert_eq!(frame.payload(), b"zero_alloc_payload");
            proc_clone.fetch_add(1, Ordering::Release);
            0
        })
        .expect("Failed to build pool");

    for _ in 0..100 {
        pool.submit("binance_trades", "BTCUSDT", b"zero_alloc_payload", 0)
            .expect("Submit failed");
    }

    let mut waited = 0;
    while processed.load(Ordering::Acquire) < 100 && waited < 100 {
        thread::sleep(Duration::from_millis(10));
        waited += 1;
    }

    assert_eq!(processed.load(Ordering::Acquire), 100);
}

#[test]
fn test_submit_cstr_and_submit_keyed() {
    let processed = Arc::new(AtomicUsize::new(0));
    let proc_clone = processed.clone();

    let pool = AsyncWorkerPool::new(4, 256, AwpRingMode::Mpsc, move |frame| {
        assert_eq!(frame.symbol(), "ETHUSDT");
        proc_clone.fetch_add(1, Ordering::Release);
        0
    })
    .expect("Failed to create pool");

    let feed_cstr = CStr::from_bytes_with_nul(b"quotes\0").unwrap();
    let sym_cstr = CStr::from_bytes_with_nul(b"ETHUSDT\0").unwrap();

    // 1. Submit with CStr
    pool.submit_cstr(feed_cstr, sym_cstr, b"cstr_payload", 0)
        .expect("submit_cstr failed");

    // 2. Submit with Keyed Hash
    pool.submit_keyed(12345, "quotes", "ETHUSDT", b"keyed_payload", 0)
        .expect("submit_keyed failed");

    let mut waited = 0;
    while processed.load(Ordering::Acquire) < 2 && waited < 100 {
        thread::sleep(Duration::from_millis(10));
        waited += 1;
    }

    assert_eq!(processed.load(Ordering::Acquire), 2);
}

#[test]
fn test_zero_copy_claim_and_typed_struct() {
    let processed = Arc::new(AtomicUsize::new(0));
    let proc_clone = processed.clone();

    let pool = AsyncWorkerPool::new(4, 256, AwpRingMode::Mpsc, move |frame| {
        assert_eq!(frame.feed(), "okx_futures");
        assert_eq!(frame.symbol(), "SOLUSDT");

        let trade = frame
            .payload_as::<TradeEvent>()
            .expect("Failed to cast payload as TradeEvent");
        assert_eq!(trade.price, 145.50);
        assert_eq!(trade.qty, 10.0);
        assert_eq!(trade.order_id, 998877);

        proc_clone.fetch_add(1, Ordering::Release);
        0
    })
    .expect("Failed to create pool");

    for _ in 0..50 {
        let mut guard = loop {
            match pool.claim(0) {
                Ok(g) => break g,
                Err(_) => thread::yield_now(),
            }
        };

        guard.set_feed("okx_futures").unwrap();
        guard.set_symbol("SOLUSDT").unwrap();

        let event = TradeEvent {
            price: 145.50,
            qty: 10.0,
            order_id: 998877,
        };

        guard.write_struct(&event).unwrap();
        guard.commit().expect("Commit failed");
    }

    let mut waited = 0;
    while processed.load(Ordering::Acquire) < 50 && waited < 100 {
        thread::sleep(Duration::from_millis(10));
        waited += 1;
    }

    assert_eq!(processed.load(Ordering::Acquire), 50);
}

#[test]
fn test_error_handling_too_big() {
    let pool = AsyncWorkerPool::new(2, 64, AwpRingMode::Mpsc, |_| 0).unwrap();
    let long_feed = "a".repeat(128); // Max is 64
    let err = pool.submit(&long_feed, "BTC", b"ok", 0).unwrap_err();
    assert_eq!(err, AwpError::TooBig);
}
