//! Standalone concurrency stress-test program for `MpmcRingBuffer`.
//!
//! Run with:  cargo run --bin queue_verify          (debug)
//!            cargo run --release --bin queue_verify (recommended)
//!
//! Each scenario prints `PASS` or `FAIL <reason>`; the process exits with a
//! non-zero status if any scenario fails. `cargo test` is intentionally not
//! used.

use mpmc_ringbuf::{MpmcRingBuffer, PushFull};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

type TestResult = Result<(), String>;

/// Runs one MPMC scenario: `producers` threads push unique ids taken from a
/// shared ticket counter `[0, total)`, `consumers` threads pop until exactly
/// `total` elements have been consumed. Returns the multiset of popped ids
/// plus the final stats. Verifies per-element uniqueness afterwards.
fn run_mpmc(
    queue: Arc<MpmcRingBuffer<usize>>,
    total: usize,
    producers: usize,
    consumers: usize,
) -> (Vec<usize>, mpmc_ringbuf::Stats) {
    let ticket = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for _ in 0..producers {
        let queue = Arc::clone(&queue);
        let ticket = Arc::clone(&ticket);
        handles.push(thread::spawn(move || loop {
            let id = ticket.fetch_add(1, Ordering::Relaxed);
            if id >= total {
                break;
            }
            // Never block forever: on Full we retry with a yield.
            while let Err(PushFull(value)) = queue.push(id) {
                let _ = value;
                thread::yield_now();
            }
        }));
    }

    let mut consumer_handles = Vec::new();
    for _ in 0..consumers {
        let queue = Arc::clone(&queue);
        let consumed = Arc::clone(&consumed);
        consumer_handles.push(thread::spawn(move || {
            let mut local = Vec::new();
            while consumed.load(Ordering::Acquire) < total {
                match queue.pop() {
                    Some(value) => {
                        local.push(value);
                        consumed.fetch_add(1, Ordering::AcqRel);
                    }
                    None => thread::yield_now(),
                }
            }
            local
        }));
    }

    for handle in handles {
        handle.join().expect("producer thread panicked");
    }
    let mut all = Vec::with_capacity(total);
    for handle in consumer_handles {
        all.extend(handle.join().expect("consumer thread panicked"));
    }
    (all, queue.stats())
}

/// Checks that `items` is exactly the set `0..total`: no duplicates, no
/// losses, no fabricated elements.
fn verify_exact_set(mut items: Vec<usize>, total: usize) -> TestResult {
    if items.len() != total {
        return Err(format!(
            "expected {total} elements, consumer(s) collected {}",
            items.len()
        ));
    }
    items.sort_unstable();
    for (expected, actual) in items.iter().enumerate() {
        if *actual != expected {
            return Err(format!(
                "set mismatch at sorted index {expected}: found {actual} \
                 (duplicate, loss or fabricated element)"
            ));
        }
    }
    Ok(())
}

fn scenario_capacity_one() -> TestResult {
    let queue = Arc::new(MpmcRingBuffer::new(1));
    let total = 100_000;
    let (items, stats) = run_mpmc(Arc::clone(&queue), total, 2, 2);
    verify_exact_set(items, total)?;
    if stats.push_ok != total as u64 || stats.pop_ok != total as u64 {
        return Err(format!(
            "stats mismatch: push_ok={} pop_ok={} expected {total}",
            stats.push_ok, stats.pop_ok
        ));
    }
    if queue.len_approx() != 0 {
        return Err("queue not empty at end".into());
    }
    Ok(())
}

fn scenario_small_capacity() -> TestResult {
    let queue = Arc::new(MpmcRingBuffer::new(4));
    let total = 300_000;
    let (items, stats) = run_mpmc(Arc::clone(&queue), total, 4, 4);
    verify_exact_set(items, total)?;
    if stats.push_ok != total as u64 || stats.pop_ok != total as u64 {
        return Err("stats mismatch".into());
    }
    Ok(())
}

fn scenario_high_contention() -> TestResult {
    let queue = Arc::new(MpmcRingBuffer::new(1024));
    let total = 500_000;
    let (items, stats) = run_mpmc(Arc::clone(&queue), total, 8, 8);
    verify_exact_set(items, total)?;
    if stats.push_ok != total as u64 || stats.pop_ok != total as u64 {
        return Err("stats mismatch".into());
    }
    Ok(())
}

fn scenario_full_status() -> TestResult {
    let queue = MpmcRingBuffer::new(3);
    queue.push(10).map_err(|_| "push 1 failed")?;
    queue.push(11).map_err(|_| "push 2 failed")?;
    queue.push(12).map_err(|_| "push 3 failed")?;
    match queue.push(13) {
        Err(PushFull(value)) if value == 13 => {}
        other => return Err(format!("expected PushFull(13), got {other:?}")),
    }
    if queue.len_approx() != 3 {
        return Err(format!("len_approx expected 3, got {}", queue.len_approx()));
    }
    // One slot frees up -> push must succeed again.
    if queue.pop() != Some(10) {
        return Err("expected FIFO pop of 10".into());
    }
    queue.push(13).map_err(|_| "push after pop failed")?;
    // FIFO order of the remaining elements.
    if queue.pop() != Some(11) || queue.pop() != Some(12) || queue.pop() != Some(13) {
        return Err("FIFO order violated".into());
    }
    let stats = queue.stats();
    if stats.push_ok != 4 || stats.push_full != 1 || stats.pop_ok != 4 {
        return Err(format!("stats mismatch: {stats:?}"));
    }
    Ok(())
}

fn scenario_empty_status() -> TestResult {
    let queue: MpmcRingBuffer<u32> = MpmcRingBuffer::new(2);
    if queue.pop().is_some() {
        return Err("pop on empty queue must return None".into());
    }
    queue.push(7).map_err(|_| "push failed")?;
    if queue.pop() != Some(7) {
        return Err("expected Some(7)".into());
    }
    if queue.pop().is_some() {
        return Err("pop on drained queue must return None".into());
    }
    let stats = queue.stats();
    if stats.pop_empty != 2 || stats.pop_ok != 1 {
        return Err(format!("stats mismatch: {stats:?}"));
    }
    if !queue.is_empty_approx() {
        return Err("is_empty_approx should be true".into());
    }
    Ok(())
}

fn scenario_long_run_wrap() -> TestResult {
    // Capacity 8 with enough traffic that every slot is recycled hundreds of
    // thousands of times: exercises head/tail growth and slot reuse.
    let queue = Arc::new(MpmcRingBuffer::new(8));
    let total = 2_000_000;
    let (items, stats) = run_mpmc(Arc::clone(&queue), total, 4, 4);
    verify_exact_set(items, total)?;
    if stats.push_ok != total as u64 || stats.pop_ok != total as u64 {
        return Err("stats mismatch".into());
    }
    Ok(())
}

fn scenario_counter_wrap() -> TestResult {
    // Start head/tail just below usize::MAX so the counters actually wrap
    // past 2^64-1 during the run; slot sequence numbers must stay coherent.
    let offset = usize::MAX - 50;
    let queue = Arc::new(MpmcRingBuffer::with_counter_offset(16, offset));
    let total = 200_000;
    let (items, stats) = run_mpmc(Arc::clone(&queue), total, 4, 4);
    verify_exact_set(items, total)?;
    if stats.push_ok != total as u64 || stats.pop_ok != total as u64 {
        return Err("stats mismatch".into());
    }
    if queue.len_approx() != 0 {
        return Err("queue not empty at end".into());
    }
    Ok(())
}

fn scenario_drop_unconsumed() -> TestResult {
    // Counts live instances to detect leaks and double drops.
    struct Tracker {
        counter: Arc<AtomicUsize>,
    }
    impl Tracker {
        fn new(counter: &Arc<AtomicUsize>) -> Self {
            counter.fetch_add(1, Ordering::SeqCst);
            Tracker { counter: Arc::clone(counter) }
        }
    }
    impl Drop for Tracker {
        fn drop(&mut self) {
            self.counter.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let live = Arc::new(AtomicUsize::new(0));
    {
        let queue = MpmcRingBuffer::new(16);
        let mut popped = Vec::new();
        for _ in 0..10 {
            queue
                .push(Tracker::new(&live))
                .map_err(|_| "push failed")?;
        }
        for _ in 0..4 {
            popped.push(queue.pop().ok_or("pop failed")?);
        }
        if live.load(Ordering::SeqCst) != 10 {
            return Err("expected 10 live trackers before queue drop".into());
        }
        drop(queue); // must drop exactly the 6 unconsumed elements
        if live.load(Ordering::SeqCst) != 4 {
            return Err(format!(
                "expected 4 live trackers after queue drop, got {}",
                live.load(Ordering::SeqCst)
            ));
        }
        drop(popped);
        if live.load(Ordering::SeqCst) != 0 {
            return Err("leak or double drop detected".into());
        }
    }
    Ok(())
}

fn scenario_len_and_stats_sanity() -> TestResult {
    let queue = MpmcRingBuffer::new(5);
    if queue.capacity() != 5 {
        return Err("capacity() mismatch".into());
    }
    for i in 0..5 {
        queue.push(i).map_err(|_| "push failed")?;
        if queue.len_approx() != i + 1 {
            return Err(format!("len_approx expected {}, got {}", i + 1, queue.len_approx()));
        }
    }
    if queue.push(99).is_ok() {
        return Err("push to full queue must fail".into());
    }
    for expected_len in (0..5).rev() {
        if queue.pop().is_none() {
            return Err("pop failed".into());
        }
        if queue.len_approx() != expected_len {
            return Err(format!(
                "len_approx expected {expected_len}, got {}",
                queue.len_approx()
            ));
        }
    }
    let stats = queue.stats();
    if stats.push_ok != 5 || stats.push_full != 1 || stats.pop_ok != 5 {
        return Err(format!("stats mismatch: {stats:?}"));
    }
    Ok(())
}

fn main() -> ExitCode {
    let scenarios: Vec<(&str, fn() -> TestResult)> = vec![
        ("capacity_1_mpmc", scenario_capacity_one),
        ("small_capacity_4_mpmc", scenario_small_capacity),
        ("high_contention_8x8_cap1024", scenario_high_contention),
        ("full_returns_pushfull_status", scenario_full_status),
        ("empty_returns_none_status", scenario_empty_status),
        ("long_run_slot_reuse", scenario_long_run_wrap),
        ("counter_wrap_usize_max", scenario_counter_wrap),
        ("drop_unconsumed_elements", scenario_drop_unconsumed),
        ("len_and_stats_sanity", scenario_len_and_stats_sanity),
    ];

    println!("queue_verify: {} scenarios", scenarios.len());
    let mut failures = 0;
    for (name, scenario) in &scenarios {
        let start = Instant::now();
        match scenario() {
            Ok(()) => println!("PASS {name} ({:?})", start.elapsed()),
            Err(reason) => {
                failures += 1;
                println!("FAIL {name}: {reason}");
            }
        }
    }
    if failures == 0 {
        println!("ALL {} SCENARIOS PASSED", scenarios.len());
        ExitCode::SUCCESS
    } else {
        println!("{failures}/{} SCENARIOS FAILED", scenarios.len());
        ExitCode::FAILURE
    }
}
