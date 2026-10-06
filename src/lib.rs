//! 有界多生产者多消费者（MPMC）环形缓冲队列。
//!
//! 核心算法源自 Dmitry Vyukov 的经典有界 MPMC 队列：
//! 每个槽位携带一个单调递增的序号（sequence number），生产者与消费者
//! 通过 CAS 竞争 `head` / `tail` 计数器来认领槽位；槽位序号随位置按
//! `capacity` 为周期推进，从而正确处理计数回绕并避免槽位复用导致的
//! ABA 问题。
//!
//! 与原始算法不同的是，槽位序号采用“翻倍编码”：位置 `pos` 对应的
//! 槽位序号为 `2*pos`（空闲可写）或 `2*pos+1`（已发布可读）。奇偶
//! 分离使“生产者发布”与“消费者释放”写入的值域互不重叠，因此
//! `capacity == 1` 时依然正确（原始编码在该情形下两种状态值相同，
//! 会误判满队列为可写）。
//!
//! 语义：
//! - 容量在构造时固定，任意正整数（不要求 2 的幂）。
//! - [`Queue::push`] 非阻塞：队列已满时返回 [`Full`] 并交还元素。
//! - [`Queue::pop`] 非阻塞：队列为空时返回 `None`。
//! - 每个成功入队的元素恰好被一个消费者取出一次。
//! - 所有状态均位于本进程内存，不依赖任何外部系统。

use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// 缓存行对齐填充，避免 head/tail 与统计计数器之间的伪共享。
#[repr(align(64))]
struct CachePadded<T>(T);

/// 队列已满时 [`Queue::push`] 返回的错误，持有被退回的元素。
pub struct Full<T>(pub T);

impl<T> fmt::Debug for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Full(..)")
    }
}

impl<T> fmt::Display for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("queue is full")
    }
}

impl<T> std::error::Error for Full<T> {}

/// 操作统计快照。计数使用 `Relaxed` 原子自增，不影响并发正确性，
/// 因此读取到的是近似值（在静默时刻读取则为精确值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// 成功入队次数。
    pub push_ok: u64,
    /// 因队列满而失败的入队次数。
    pub push_full: u64,
    /// 成功出队次数。
    pub pop_ok: u64,
    /// 因队列空而失败的出队次数。
    pub pop_empty: u64,
}

struct Slot<T> {
    /// 槽位序号（翻倍编码）：
    /// - `2*pos`   ：槽位空闲，可被位置 `pos` 的生产者认领；
    /// - `2*pos+1` ：槽位持有位置 `pos` 已发布的元素，可被消费者认领。
    /// 初始为 `2*i`（`i` 为槽位下标）；消费者取走位置 `pos` 的元素后
    /// 置为 `2*(pos + capacity)`，即下一周期的空闲态。
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// 有界 MPMC 环形缓冲队列。
pub struct Queue<T> {
    slots: Box<[Slot<T>]>,
    capacity: usize,
    /// 下一个将被取出的位置（消费者认领）。
    head: CachePadded<AtomicUsize>,
    /// 下一个将被写入的位置（生产者认领）。
    tail: CachePadded<AtomicUsize>,
    push_ok: CachePadded<AtomicU64>,
    push_full: CachePadded<AtomicU64>,
    pop_ok: CachePadded<AtomicU64>,
    pop_empty: CachePadded<AtomicU64>,
}

// 安全性：槽位访问由 per-slot 序号的发起/获取（Release/Acquire）配对保护，
// 同一时刻至多一个线程“拥有”某个槽位的数据访问权，符合 `T: Send` 的要求。
unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    /// 创建容量为 `capacity` 的队列。`capacity` 必须大于 0，创建后固定不变。
    ///
    /// # Panics
    /// `capacity == 0` 时 panic。
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        let mut slots = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(Slot {
                seq: AtomicUsize::new(2 * i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        Queue {
            slots: slots.into_boxed_slice(),
            capacity,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            push_ok: CachePadded(AtomicU64::new(0)),
            push_full: CachePadded(AtomicU64::new(0)),
            pop_ok: CachePadded(AtomicU64::new(0)),
            pop_empty: CachePadded(AtomicU64::new(0)),
        }
    }

    /// 队列容量（固定值）。
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 非阻塞入队。队列已满时返回 `Err(Full(value))` 交还元素。
    pub fn push(&self, value: T) -> Result<(), Full<T>> {
        loop {
            let pos = self.tail.0.load(Ordering::Relaxed);
            // pos % capacity < capacity，不会越界。
            let slot = &self.slots[pos % self.capacity];
            let seq = slot.seq.load(Ordering::Acquire);
            // 以有符号差比较，在计数器回绕时仍然正确（真实差值远小于 2^63）。
            let dif = seq.wrapping_sub(pos.wrapping_mul(2)) as isize;
            if dif == 0 {
                // 槽位对本周期空闲，尝试认领位置 pos。
                if self
                    .tail
                    .0
                    .compare_exchange_weak(
                        pos,
                        pos.wrapping_add(1),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // 认领成功：独占该槽位，写入数据后发布。
                    unsafe { (*slot.data.get()).write(value) };
                    slot.seq
                        .store(pos.wrapping_mul(2).wrapping_add(1), Ordering::Release);
                    self.push_ok.0.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                // CAS 失败：其他生产者抢先，重试。
            } else if dif < 0 {
                // 槽位上一周期的元素尚未被消费：队列已满。
                self.push_full.0.fetch_add(1, Ordering::Relaxed);
                return Err(Full(value));
            }
            // dif > 0：tail 已被其他生产者推进，重新加载并重试。
        }
    }

    /// 非阻塞出队。队列为空时返回 `None`。
    pub fn pop(&self) -> Option<T> {
        loop {
            let pos = self.head.0.load(Ordering::Relaxed);
            let slot = &self.slots[pos % self.capacity];
            let seq = slot.seq.load(Ordering::Acquire);
            let dif = seq.wrapping_sub(pos.wrapping_mul(2).wrapping_add(1)) as isize;
            if dif == 0 {
                // 位置 pos 的元素已发布，尝试认领。
                if self
                    .head
                    .0
                    .compare_exchange_weak(
                        pos,
                        pos.wrapping_add(1),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // 认领成功：独占该槽位，读出元素后释放槽位给下一周期。
                    let value = unsafe { (*slot.data.get()).assume_init_read() };
                    slot.seq
                        .store(
                            pos.wrapping_add(self.capacity).wrapping_mul(2),
                            Ordering::Release,
                        );
                    self.pop_ok.0.fetch_add(1, Ordering::Relaxed);
                    return Some(value);
                }
            } else if dif < 0 {
                // 位置 pos 的元素尚未发布：队列为空。
                self.pop_empty.0.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
    }

    /// 近似长度（在途元素数）。并发读写期间为估计值，静默时刻精确。
    pub fn len(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head).min(self.capacity)
    }

    /// 近似判空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 近似判满。
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// 读取操作统计快照。
    pub fn stats(&self) -> Stats {
        Stats {
            push_ok: self.push_ok.0.load(Ordering::Relaxed),
            push_full: self.push_full.0.load(Ordering::Relaxed),
            pop_ok: self.pop_ok.0.load(Ordering::Relaxed),
            pop_empty: self.pop_empty.0.load(Ordering::Relaxed),
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // 持有 &mut self，不存在并发访问；[head, tail) 区间内的槽位
        // 均含有已构造但尚未消费的元素，逐一析构，保证不泄漏。
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        let mut pos = head;
        while pos != tail {
            let slot = &self.slots[pos % self.capacity];
            unsafe { (*slot.data.get()).assume_init_drop() };
            pos = pos.wrapping_add(1);
        }
    }
}
