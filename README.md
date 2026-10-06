# mpmc_ring — 有界多生产者多消费者 Ring Buffer 队列

纯 Rust 标准库实现的有界 MPMC 环形缓冲队列。所有数据结构与测试状态均位于
本地进程内存，不依赖 Redis、消息队列服务或任何外部系统。

## 并发模型

核心算法源自 Dmitry Vyukov 的经典有界 MPMC 队列，在本工程内完整实现：

- 队列由固定容量的槽位数组构成，位置 `pos` 映射到槽位 `pos % capacity`
  （容量为任意正整数，不要求 2 的幂，取模保证不发生数组越界）。
- `head` / `tail` 为单调递增的全局位置计数器，生产者/消费者通过
  `compare_exchange_weak` 认领位置，认领成功的线程独占对应槽位。
- 每个槽位携带一个单调递增的序号 `seq`，用于判定槽位状态并同步数据可见性。
- 无锁（lock-free）：任一线程被挂起不会阻塞其他线程推进；无互斥锁、无条件变量。

### 槽位序号编码（对经典算法的关键修正）

经典 Vyukov 编码中，生产者在位置 `pos` 发布时写 `seq = pos + 1`，消费者释放
槽位时写 `seq = pos + capacity`。当 `capacity == 1` 时两者取值相同，
“满”与“空”状态无法区分，会导致元素被覆盖丢失。

本实现采用**翻倍编码**，使两种状态奇偶分离，对任意 `capacity >= 1` 均正确：

| 槽位序号 `seq` | 含义 |
| --- | --- |
| `2*pos` | 槽位空闲，可被位置 `pos` 的生产者认领 |
| `2*pos+1` | 槽位持有位置 `pos` 已发布的元素，可被消费者认领 |

- 生产者：`seq == 2*pos` 时 CAS 认领 `tail`，写入数据后置 `seq = 2*pos + 1`；
  `seq < 2*pos` 说明上一周期元素尚未被消费，即**队列已满**，返回 `Err(Full(value))`。
- 消费者：`seq == 2*pos + 1` 时 CAS 认领 `head`，读出元素后置
  `seq = 2*(pos + capacity)`（下一周期空闲态）；`seq < 2*pos + 1` 即
  **队列为空**，返回 `None`。

### 回绕与 ABA

- `head` / `tail` / `seq` 均为 `usize` 单调计数器，回绕比较采用
  `wrapping_sub` 后的有符号差（真实差值远小于 2^63，回绕安全）。
- 槽位复用由 per-slot 序号防护：旧周期的槽位状态无法被误认为新周期状态，
  不存在 ABA 问题。`usize` 计数器需约 2^63 次操作才会回绕，实际不可达。

## 内存顺序

- 槽位 `seq`：`Acquire` 加载 / `Release` 存储。生产者的数据写入
  **先于** `seq` 的 Release 发布，消费者 Acquire 读到该 `seq` 后即可见数据，
  构成 happens-before 关系；消费者方向同理。
- `head` / `tail` 的 CAS 使用 `Relaxed`：位置认领的正确性完全由槽位序号
  的 Acquire/Release 配对保证，无需更强顺序。
- 统计计数器（`push_ok` / `push_full` / `pop_ok` / `pop_empty`）与
  `len()` 均使用 `Relaxed`，不参与任何同步，因此统计逻辑不会破坏并发正确性；
  并发期间读到的为近似值，静默时刻为精确值。
- `head` / `tail` 与统计计数器按 64 字节缓存行对齐填充，避免伪共享。

## 容量语义

- 容量在 `Queue::new(capacity)` 时固定（`capacity > 0`，否则 panic），
  运行期不可变更；`capacity()` 返回精确容量。
- `push` / `pop` 均为**非阻塞**：满时 `push` 返回 `Err(Full(value))` 并交还
  元素，空时 `pop` 返回 `None`，不存在无限阻塞。
- 每个成功入队的元素恰好被一个消费者成功取出一次：不重复、不丢失、
  不凭空产生。
- `len()` / `is_empty()` / `is_full()` 为并发环境下的近似视图。
- `Queue` 被 drop 时，析构 `[head, tail)` 区间内所有已构造但未消费的元素，
  不会泄漏（`T: Send` 时可安全跨线程传递与共享）。

## 测试方法

测试不使用 `cargo test`，而是独立的并发压力测试程序，逐项打印 PASS/FAIL，
全部通过时退出码为 0：

```sh
cargo run --bin queue_verify            # 调试构建
cargo run --release --bin queue_verify  # 推荐，压力场景更快
```

覆盖场景（生产者生成全局唯一序号，消费者汇总后校验最终集合与统计计数）：

| 用例 | 说明 |
| --- | --- |
| `capacity_one` | 容量 1 极端场景，2P/2C，10 万条消息 |
| `small_capacity` | 小容量 7（非 2 的幂），3P/3C |
| `high_contention` | 容量 4，8P/8C 高竞争，40 万条消息 |
| `full_queue` | 精确容量语义、满时 `Full` 状态、FIFO 顺序、统计计数 |
| `empty_queue` | 空队列 `pop` 返回 `None` 及统计计数 |
| `long_wraparound` | 容量 3，4P/4C，200 万条消息，槽位复用数十万次验证回绕/ABA |
| `drop_releases_unconsumed` | drop 队列时未消费元素全部被析构 |

## 文件结构

- `src/lib.rs` — 队列实现（`Queue` / `Full` / `Stats`）
- `src/bin/queue_verify.rs` — 独立并发压力测试程序
