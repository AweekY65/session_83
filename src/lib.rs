//! A bounded multi-producer multi-consumer (MPMC) ring buffer queue.
//!
//! The core algorithm is a variant of Dmitry Vyukov's bounded MPMC queue: a
//! fixed-size array of slots, each tagged with a sequence stamp derived from
//! the monotonically increasing `head`/`tail` position counters. Producers
//! claim slots by CAS-ing `tail`, consumers claim slots by CAS-ing `head`.
//!
//! Slot `j` at cycle `c` (position `pos = c * capacity + j`) uses two
//! stamps:
//! * `2 * pos`     — the slot is free for the producer at `pos`;
//! * `2 * pos + 1` — the slot holds a published element for the consumer
//!   at `pos`.
//!
//! Unlike the classic `pos` / `pos + 1` encoding, the doubled stamps keep
//! the "free" and "full" states distinct for *every* capacity, including
//! capacity 1 (where `pos + 1 == pos + capacity` would collapse the two
//! states into one and corrupt the protocol). All stamp arithmetic is
//! wrapping, so `head`/`tail` may wrap around `usize::MAX` without ABA
//! issues: each slot's stamp still uniquely identifies its cycle because
//! stamps are compared with wrapping subtraction (see README.md).
//!
//! Everything lives in local process memory: no Redis, no message broker,
//! no external system.

use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Returned by [`MpmcRingBuffer::push`] when the queue is full.
///
/// Carries the value back to the caller so nothing is silently dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushFull<T>(pub T);

impl<T> fmt::Display for PushFull<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "queue is full")
    }
}

/// Snapshot of the operation counters.
///
/// Counters are updated with `Relaxed` ordering *after* the operation has
/// been published, so they are best-effort diagnostics and never influence
/// the correctness of the queue itself.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Successful `push` calls.
    pub push_ok: u64,
    /// `push` calls rejected because the queue was full.
    pub push_full: u64,
    /// Successful `pop` calls.
    pub pop_ok: u64,
    /// `pop` calls that observed an empty queue.
    pub pop_empty: u64,
}

/// Cache-line padding to keep `head` and `tail` on separate cache lines
/// and avoid false sharing between producers and consumers.
#[repr(align(64))]
struct CachePadded<T>(T);

struct Slot<T> {
    /// Sequence stamp of this slot (see the module-level docs):
    /// * stamp `2 * pos` means the slot is free for the producer whose
    ///   claimed position is `pos`;
    /// * stamp `2 * pos + 1` means the slot holds the element published
    ///   for the consumer whose claimed position is `pos`.
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded MPMC ring buffer with fixed capacity.
///
/// * `push` never blocks: it returns [`PushFull`] when the queue is full.
/// * `pop` never blocks: it returns `None` when the queue is empty.
/// * Every successfully pushed element is popped by exactly one consumer,
///   exactly once.
pub struct MpmcRingBuffer<T> {
    capacity: usize,
    slots: Box<[Slot<T>]>,
    /// Next position to pop from. Monotonically increasing (wrapping).
    head: CachePadded<AtomicUsize>,
    /// Next position to push to. Monotonically increasing (wrapping).
    tail: CachePadded<AtomicUsize>,
    push_ok: AtomicU64,
    push_full: AtomicU64,
    pop_ok: AtomicU64,
    pop_empty: AtomicU64,
}

// Safety: all shared mutable state is behind atomics. `Slot::data` is only
// written by the single producer that claimed the slot (via the `tail` CAS)
// before publishing it with a `Release` store to `seq`, and only read by the
// single consumer that claimed the slot (via the `head` CAS) after observing
// the publication with an `Acquire` load of `seq`. Each slot changes hands
// at most once per cycle, so there is no data race on `data`.
unsafe impl<T: Send> Send for MpmcRingBuffer<T> {}
unsafe impl<T: Send> Sync for MpmcRingBuffer<T> {}

/// Stamp meaning "slot is free for the producer at position `pos`".
#[inline]
fn free_stamp(pos: usize) -> usize {
    pos.wrapping_mul(2)
}

/// Stamp meaning "slot holds the element published for position `pos`".
#[inline]
fn full_stamp(pos: usize) -> usize {
    pos.wrapping_mul(2).wrapping_add(1)
}

impl<T> MpmcRingBuffer<T> {
    /// Creates an empty queue that can hold exactly `capacity` elements.
    ///
    /// Any capacity >= 1 is supported (it does not need to be a power of
    /// two; slots are indexed with a modulo).
    ///
    /// # Panics
    /// Panics if `capacity == 0`.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be at least 1");
        let mut slots = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(Slot {
                seq: AtomicUsize::new(free_stamp(i)),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        Self {
            capacity,
            slots: slots.into_boxed_slice(),
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            push_ok: AtomicU64::new(0),
            push_full: AtomicU64::new(0),
            pop_ok: AtomicU64::new(0),
            pop_empty: AtomicU64::new(0),
        }
    }

    /// Test hook: creates an empty queue whose head/tail counters start at
    /// `offset`, so the `usize` wrap-around path can be exercised without
    /// performing 2^64 operations. Slot stamps are initialised consistently
    /// with the shifted counters.
    #[doc(hidden)]
    pub fn with_counter_offset(capacity: usize, offset: usize) -> Self {
        let queue = Self::new(capacity);
        let base = offset % capacity;
        for (i, slot) in queue.slots.iter().enumerate() {
            // Smallest position >= offset that maps to slot i.
            let pos = offset.wrapping_add((i + capacity - base) % capacity);
            slot.seq.store(free_stamp(pos), Ordering::Relaxed);
        }
        queue.head.0.store(offset, Ordering::Relaxed);
        queue.tail.0.store(offset, Ordering::Relaxed);
        queue
    }

    /// The fixed capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Approximate number of elements currently in the queue.
    ///
    /// This is a racy snapshot (`tail - head` clamped to `[0, capacity]`)
    /// intended for monitoring only; never use it to decide whether a
    /// `push`/`pop` will succeed.
    pub fn len_approx(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head).min(self.capacity)
    }

    /// Returns `true` if a racy snapshot suggests the queue is empty.
    pub fn is_empty_approx(&self) -> bool {
        self.len_approx() == 0
    }

    /// Snapshot of the success/failure counters.
    pub fn stats(&self) -> Stats {
        Stats {
            push_ok: self.push_ok.load(Ordering::Relaxed),
            push_full: self.push_full.load(Ordering::Relaxed),
            pop_ok: self.pop_ok.load(Ordering::Relaxed),
            pop_empty: self.pop_empty.load(Ordering::Relaxed),
        }
    }

    /// Attempts to enqueue `value` without blocking.
    ///
    /// Returns `Ok(())` on success, or `Err(PushFull(value))` if the queue
    /// is currently full (the value is handed back to the caller).
    pub fn push(&self, value: T) -> Result<(), PushFull<T>> {
        let mut pos = self.tail.0.load(Ordering::Relaxed);
        loop {
            // `pos % capacity` is always < capacity, so indexing is in
            // bounds by construction.
            let slot = &self.slots[pos % self.capacity];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(free_stamp(pos)) as isize;
            if diff == 0 {
                // Slot is free for this cycle: try to claim the position.
                match self.tail.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We are the only writer of this slot for this cycle.
                        unsafe { (*slot.data.get()).write(value) };
                        // Publish the element: pairs with the consumer's
                        // Acquire load of `seq`.
                        slot.seq.store(full_stamp(pos), Ordering::Release);
                        self.push_ok.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                // Slot still holds unconsumed data from the previous cycle:
                // the queue is full.
                self.push_full.fetch_add(1, Ordering::Relaxed);
                return Err(PushFull(value));
            } else {
                // Another producer claimed this position first; re-read tail.
                pos = self.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Attempts to dequeue one element without blocking.
    ///
    /// Returns `Some(value)` on success, or `None` if the queue is
    /// currently empty.
    pub fn pop(&self) -> Option<T> {
        let mut pos = self.head.0.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[pos % self.capacity];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(full_stamp(pos)) as isize;
            if diff == 0 {
                // Slot holds a published element for this cycle.
                match self.head.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We are the only reader of this slot for this cycle.
                        let value = unsafe { (*slot.data.get()).assume_init_read() };
                        // Mark the slot free for the next cycle: pairs with
                        // the producer's Acquire load of `seq`.
                        slot.seq.store(
                            free_stamp(pos.wrapping_add(self.capacity)),
                            Ordering::Release,
                        );
                        self.pop_ok.fetch_add(1, Ordering::Relaxed);
                        return Some(value);
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                // Producer has not published this slot yet: queue is empty.
                self.pop_empty.fetch_add(1, Ordering::Relaxed);
                return None;
            } else {
                // Another consumer claimed this position first; re-read head.
                pos = self.head.0.load(Ordering::Relaxed);
            }
        }
    }
}

impl<T> Drop for MpmcRingBuffer<T> {
    /// Safely drops every element that was pushed but never popped.
    fn drop(&mut self) {
        // We have `&mut self`, so no other thread can be inside push/pop.
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        let mut pos = head;
        // Invariant: at most `capacity` elements are live, so the wrapping
        // walk from head to tail terminates.
        while pos != tail {
            let slot = &self.slots[pos % self.capacity];
            unsafe { (*slot.data.get()).assume_init_drop() };
            pos = pos.wrapping_add(1);
        }
    }
}
