use awp_rs::{AsyncWorkerPool, AwpRingMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_pool_submit_and_process() {
    let processed = Arc::new(AtomicUsize::new(0));
    let proc_clone = processed.clone();

    let pool = AsyncWorkerPool::new(4, 256, AwpRingMode::Mpsc, move |frame| {
        let payload = frame.payload();
        assert_eq!(payload, b"hello_hft");
        proc_clone.fetch_add(1, Ordering::Release);
        0
    })
    .expect("Failed to create pool");

    for _ in 0..100 {
        pool.submit("trades", "BTCUSDT", b"hello_hft", 0)
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
fn test_zero_copy_claim_and_commit() {
    let processed = Arc::new(AtomicUsize::new(0));
    let proc_clone = processed.clone();

    let pool = AsyncWorkerPool::new(4, 256, AwpRingMode::Mpsc, move |frame| {
        let payload = frame.payload();
        assert_eq!(payload.len(), 64);
        assert_eq!(payload[0], 42);
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

        let buf = guard.payload_mut();
        buf[..64].fill(42);
        guard.set_payload_len(64);
        guard.commit().expect("Commit failed");
    }

    let mut waited = 0;
    while processed.load(Ordering::Acquire) < 50 && waited < 100 {
        thread::sleep(Duration::from_millis(10));
        waited += 1;
    }

    assert_eq!(processed.load(Ordering::Acquire), 50);
}
