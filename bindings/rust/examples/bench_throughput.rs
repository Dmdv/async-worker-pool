use awp_rs::{AsyncWorkerPool, AwpRingMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    println!("=== AWP Rust FFI Benchmark (1,000,000 Messages) ===");

    let num_messages: usize = 1_000_000;
    let workers: u32 = 32;
    let queue_capacity: u32 = 4096;

    let processed = Arc::new(AtomicUsize::new(0));
    let proc_clone = processed.clone();

    let pool = AsyncWorkerPool::new(workers, queue_capacity, AwpRingMode::Mpsc, move |_frame| {
        proc_clone.fetch_add(1, Ordering::Relaxed);
        0
    })
    .expect("Failed to create worker pool");

    println!("Pool initialized with {} workers. Starting dispatch...", workers);

    let start = Instant::now();

    for i in 0..num_messages {
        let shard = (i % (workers as usize)) as u32;
        let mut guard = loop {
            match pool.claim(shard) {
                Ok(g) => break g,
                Err(_) => std::thread::yield_now(),
            }
        };

        let buf = guard.payload_mut();
        buf[0] = (i & 0xFF) as u8;
        buf[1] = ((i >> 8) & 0xFF) as u8;
        guard.set_payload_len(16);
        let _ = guard.commit();
    }

    // Wait for all messages to drain
    while processed.load(Ordering::Acquire) < num_messages {
        std::thread::yield_now();
    }

    let elapsed = start.elapsed();
    let total_secs = elapsed.as_secs_f64();
    let rps = (num_messages as f64) / total_secs;
    let avg_latency_ns = (elapsed.as_nanos() as f64) / (num_messages as f64);

    println!("--------------------------------------------------");
    println!("Total Messages Processed: {}", num_messages);
    println!("Elapsed Time:             {:.2} ms", elapsed.as_secs_f64() * 1000.0);
    println!("Throughput:               {:.2} M msg/sec ({:.0} msg/sec)", rps / 1_000_000.0, rps);
    println!("Mean Latency:             {:.2} ns/msg", avg_latency_ns);
    println!("==================================================");
}
