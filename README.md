# mpmc-ringbuf

纯本地进程内存实现的有界多生产者多消费者（MPMC）Ring Buffer 队列。
不依赖 Redis、消息队列服务或任何外部系统；核心环形缓冲逻辑在
`src/lib.rs` 内完成，仅使用标准库原子类型作为并发原语。

## 并发模型

算法是 Dmitry Vyukov 有界 MPMC 队列的一个变体：

- 队列是固定大小的槽位数组，容量在 `MpmcRingBuffer::new(capacity)` 时
  确定，之后不可改变。
- `head` / `tail` 是两个单调递增（允许回绕）的 `AtomicUsize` 位置计数器，
  分别做了缓存行填充（`#[repr(align(64))]`）以避免伪共享。
- 生产者通过对 `tail` 做 `compare_exchange_weak` 抢占写入位置；消费者
  通过对 `head` 做 CAS 抢占读取位置。CAS 成功的线程即获得该位置在当前
  周期的独占访问权，因此同一元素只会被一个 consumer 取出一次。
- 槽位下标为 `pos % capacity`，恒小于 `capacity`，不会发生数组越界。
- 队列满时 `push` 立即返回 `Err(PushFull(value))`（把元素交还调用方）；
  队列空时 `pop` 立即返回 `None`。两者都不会无限阻塞。

### 槽位序列戳与 ABA 防护

每个槽位携带一个原子序列戳 `seq`。位置 `pos`（对应槽位
`pos % capacity`、周期 `pos / capacity`）使用两个戳：

- `2 * pos`：槽位空闲，可被位置 `pos` 的生产者写入；
- `2 * pos + 1`：槽位持有已发布的元素，可被位置 `pos` 的消费者读取。

生产者发布元素时把戳改为 `2 * pos + 1`；消费者取走元素后把戳改为
`2 * (pos + capacity)`，即下一周期的空闲戳。由于每个槽位的戳唯一标识
其周期，槽位复用和 `head`/`tail` 回绕都不会产生经典 ABA 问题：所有戳
比较都使用 `wrapping_sub` 求差再解释为 `isize`，只要容量远小于
`2^63`（实际约束），回绕后的比较仍然正确。

注意：经典 Vyukov 编码（空闲戳 `pos`、就绪戳 `pos + 1`）在容量为 1 时
会让两个状态坍缩为同一个值（`pos + 1 == pos + capacity`），从而破坏
协议。本实现采用翻倍戳（`2 * pos` / `2 * pos + 1`），使任意容量
（包括 1）都保持状态可区分。

## 内存顺序

- 槽位 `seq` 的加载使用 `Acquire`，存储使用 `Release`：生产者在
  `Release` 存储 `seq` 之前写入数据，消费者在 `Acquire` 加载 `seq`
  之后读取数据，构成 happens-before，保证元素内容可见性；消费者释放
  槽位与生产者重新占用之间同理。
- `head` / `tail` 的 CAS 使用 `Relaxed`：它们只负责位置分配，槽位状态
  的同步完全由 `seq` 的 Acquire/Release 承担。
- 统计计数器（`push_ok` / `push_full` / `pop_ok` / `pop_empty`）在操作
  发布之后以 `Relaxed` 递增，只用于诊断，绝不参与并发控制，因此统计
  逻辑不会破坏队列正确性。

## 容量与统计语义

- `capacity()`：固定容量，支持任意 >= 1 的整数（不要求 2 的幂）。
- `len_approx()`：`tail - head` 的有竞争快照，钳制在 `[0, capacity]`，
  仅供监控，不能用来判断 `push`/`pop` 是否会成功。
- `stats()`：成功/失败操作计数的快照（见上，最终一致）。
- 满/空是瞬时状态：`push` 返回 `PushFull` 或 `pop` 返回 `None` 只代表
  调用时刻的队列状态，调用方可自行重试。

## 资源释放

`Drop` 实现会从 `head` 遍历到 `tail`，对所有已构造但未消费的元素执行
`assume_init_drop`，保证程序退出时不泄漏、不重复释放（由
`drop_unconsumed_elements` 场景用实例计数器验证）。

## 测试方法

不使用 `cargo test`。独立压力测试程序：

```sh
cargo run --bin queue_verify            # debug
cargo run --release --bin queue_verify  # 推荐，更快
```

每个场景逐项打印 `PASS` / `FAIL <原因>`，全部通过时进程退出码为 0，
否则为 1。覆盖场景：

| 场景 | 内容 |
| --- | --- |
| `capacity_1_mpmc` | 容量 1，2 生产者 + 2 消费者，唯一序号集合校验 |
| `small_capacity_4_mpmc` | 小容量 4，4x4 线程 |
| `high_contention_8x8_cap1024` | 高竞争，8 生产者 + 8 消费者，容量 1024 |
| `full_returns_pushfull_status` | 队列满时 `push` 返回 `PushFull` 并交还元素，FIFO 顺序与统计校验 |
| `empty_returns_none_status` | 队列空时 `pop` 返回 `None`，统计校验 |
| `long_run_slot_reuse` | 容量 8 下 200 万元素，槽位长期复用 |
| `counter_wrap_usize_max` | head/tail 从 `usize::MAX - 50` 起步，真实跨越计数器回绕点 |
| `drop_unconsumed_elements` | 未消费元素在队列析构时被精确释放一次 |
| `len_and_stats_sanity` | 容量、近似长度与统计计数的一致性 |

多线程场景使用共享原子发号器生成全局唯一序号，消费者汇总后排序校验
最终集合恰好为 `0..N`，以此证明无重复、无丢失、无凭空产生的数据。
