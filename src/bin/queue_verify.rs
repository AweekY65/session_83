//! 独立并发压力测试程序（不依赖 cargo test）。
//!
//! 运行方式：
//!     cargo run --bin queue_verify            # 调试构建
//!     cargo run --release --bin queue_verify  # 推荐，压力场景更快
//!
//! 逐项打印 [PASS] / [FAIL]，全部通过时进程退出码为 0，否则为 1。

use mpmc_ring::{Full, Queue};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

type TestResult = Result<(), String>;

/// 通用 MPMC 压力场景：
/// `n_producers` 个生产者各自生成 `per_producer` 个全局唯一序号
/// （producer_id * per_producer + i），`n_consumers` 个消费者收集全部元素，
/// 最终验证“不重复、不丢失、不凭空产生”，并校验统计计数。
fn run_mpmc(capacity: usize, n_producers: usize, n_consumers: usize, per_producer: usize) -> TestResult {
    let total = n_producers * per_producer;
    let queue = Arc::new(Queue::new(capacity));
    let consumed = Arc::new(AtomicUsize::new(0));

    let mut producers = Vec::new();
    for p in 0..n_producers {
        let queue = Arc::clone(&queue);
        producers.push(thread::spawn(move || {
            for i in 0..per_producer {
                let mut value = (p * per_producer + i) as u64;
                // 队列满时重试（交还的元素继续推送），直到成功。
                loop {
                    match queue.push(value) {
                        Ok(()) => break,
                        Err(Full(returned)) => {
                            value = returned;
                            thread::yield_now();
                        }
                    }
                }
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..n_consumers {
        let queue = Arc::clone(&queue);
        let consumed = Arc::clone(&consumed);
        consumers.push(thread::spawn(move || {
            let mut local = Vec::new();
            loop {
                if consumed.load(Ordering::Relaxed) >= total {
                    break;
                }
                match queue.pop() {
                    Some(v) => {
                        consumed.fetch_add(1, Ordering::Relaxed);
                        local.push(v);
                    }
                    None => thread::yield_now(),
                }
            }
            local
        }));
    }

    for h in producers {
        h.join().map_err(|_| "producer thread panicked".to_string())?;
    }
    let mut all = Vec::with_capacity(total);
    for h in consumers {
        let local = h.join().map_err(|_| "consumer thread panicked".to_string())?;
        all.extend(local);
    }

    if all.len() != total {
        return Err(format!("popped {} items, expected {}", all.len(), total));
    }
    let set: HashSet<u64> = all.iter().copied().collect();
    if set.len() != total {
        return Err(format!("duplicate values: {} unique of {}", set.len(), total));
    }
    for v in &all {
        if *v as usize >= total {
            return Err(format!("phantom value {} out of range 0..{}", v, total));
        }
    }
    let missing: Vec<usize> = (0..total).filter(|i| !set.contains(&(*i as u64))).collect();
    if !missing.is_empty() {
        return Err(format!(
            "lost {} values, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        ));
    }

    let stats = queue.stats();
    if stats.push_ok != total as u64 {
        return Err(format!("push_ok = {}, expected {}", stats.push_ok, total));
    }
    if stats.pop_ok != total as u64 {
        return Err(format!("pop_ok = {}, expected {}", stats.pop_ok, total));
    }
    if !queue.is_empty() {
        return Err(format!("queue not empty at end, len = {}", queue.len()));
    }
    Ok(())
}

/// 容量 1 的极端场景。
fn test_capacity_one() -> TestResult {
    run_mpmc(1, 2, 2, 50_000)
}

/// 小容量（非 2 的幂，验证取模映射与槽位复用）。
fn test_small_capacity() -> TestResult {
    run_mpmc(7, 3, 3, 30_000)
}

/// 高竞争：小容量 + 多生产者多消费者。
fn test_high_contention() -> TestResult {
    run_mpmc(4, 8, 8, 50_000)
}

/// 长时间回绕：小容量下大量操作，每个槽位被复用数十万次，
/// 验证 head/tail 计数与槽位序号长期推进后仍无 ABA 类错误。
fn test_long_wraparound() -> TestResult {
    run_mpmc(3, 4, 4, 500_000)
}

/// 队列满语义：精确容量、Full 状态、统计计数、单线程 FIFO 顺序。
fn test_full_queue() -> TestResult {
    let queue = Queue::new(16);
    if queue.capacity() != 16 {
        return Err("capacity() mismatch".into());
    }
    for i in 0..16u64 {
        queue.push(i).map_err(|_| format!("push {} failed on non-full queue", i))?;
    }
    if !queue.is_full() || queue.len() != 16 {
        return Err(format!("expected full queue, len = {}", queue.len()));
    }
    match queue.push(999) {
        Err(Full(v)) if v == 999 => {}
        _ => return Err("push on full queue did not return Full(999)".into()),
    }
    for i in 0..16u64 {
        match queue.pop() {
            Some(v) if v == i => {}
            other => return Err(format!("pop order broken at {}: {:?}", i, other)),
        }
    }
    if queue.pop().is_some() {
        return Err("pop on drained queue returned Some".into());
    }
    let stats = queue.stats();
    if stats.push_ok != 16 || stats.push_full != 1 || stats.pop_ok != 16 || stats.pop_empty != 1 {
        return Err(format!("bad stats: {:?}", stats));
    }
    Ok(())
}

/// 队列空语义：空队列 pop 返回 None 并计入统计。
fn test_empty_queue() -> TestResult {
    let queue: Queue<u64> = Queue::new(8);
    if !queue.is_empty() || queue.len() != 0 {
        return Err("new queue not empty".into());
    }
    if queue.pop().is_some() {
        return Err("pop on empty queue returned Some".into());
    }
    if queue.pop().is_some() {
        return Err("pop on empty queue returned Some (2nd)".into());
    }
    let stats = queue.stats();
    if stats.pop_empty != 2 || stats.pop_ok != 0 || stats.push_ok != 0 {
        return Err(format!("bad stats: {:?}", stats));
    }
    Ok(())
}

/// 析构安全：队列 drop 时必须释放已构造但未消费的元素。
fn test_drop_releases_elements() -> TestResult {
    struct Tracker(Arc<AtomicUsize>);
    impl Drop for Tracker {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let queue = Queue::new(8);
        for _ in 0..6 {
            queue.push(Tracker(Arc::clone(&drops))).map_err(|_| "push failed")?;
        }
        // 取出 2 个并立即析构。
        drop(queue.pop());
        drop(queue.pop());
        if drops.load(Ordering::Relaxed) != 2 {
            return Err("popped elements not dropped".into());
        }
        // 队列离开作用域：剩余 4 个未消费元素必须被析构。
    }
    let n = drops.load(Ordering::Relaxed);
    if n != 6 {
        return Err(format!(
            "drop count = {}, expected 6 (leaked unconsumed elements)",
            n
        ));
    }
    Ok(())
}

fn main() {
    let tests: Vec<(&str, fn() -> TestResult)> = vec![
        ("capacity_one (cap=1, 2P/2C, 100k msgs)", test_capacity_one),
        ("small_capacity (cap=7, 3P/3C, 90k msgs)", test_small_capacity),
        ("high_contention (cap=4, 8P/8C, 400k msgs)", test_high_contention),
        ("full_queue (exact capacity + Full status + FIFO)", test_full_queue),
        ("empty_queue (None status + stats)", test_empty_queue),
        ("long_wraparound (cap=3, 4P/4C, 2M msgs)", test_long_wraparound),
        ("drop_releases_unconsumed", test_drop_releases_elements),
    ];

    println!("queue_verify: {} test cases", tests.len());
    let mut failed = 0;
    for (name, f) in &tests {
        let start = Instant::now();
        match f() {
            Ok(()) => println!("[PASS] {} ({:.2?})", name, start.elapsed()),
            Err(e) => {
                failed += 1;
                println!("[FAIL] {} ({:.2?}) -- {}", name, start.elapsed(), e);
            }
        }
    }
    println!("==== {}/{} passed ====", tests.len() - failed, tests.len());
    std::process::exit(if failed == 0 { 0 } else { 1 });
}
