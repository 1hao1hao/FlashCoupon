# Outbox 清理与双队列对照实验

日期：2026-11-11。保留默认 `Lua → Stream Outbox → Relay → RocketMQ → Worker → MySQL` 链路，测试直连模式仅替换后台消息传输。旧 [2026-11-09 工件](../evidence/engineering-2026-11-09/) 和 [验收报告](VALIDATION.md) 未覆盖；本次原始数据、配置、逐时内存样本、汇总及校验摘要均在 [新工件目录](../evidence/queue-isolation-2026-11-11/)。

## 实现与安全边界

- [cleanup.lua](../internal/app/lua/cleanup.lua)：每次最多扫描 128 条；在同一 Lua 中检查所有当前消费组的 last-delivered-id 和 PEL，再删除已确认载荷。无 `relay` 组或组数超过 8 则停止清理，保留 unread、待重试及确认丢失消息；允许越过旧 Pending 清理后续已 ACK 条目。
- [outbox.go](../internal/app/outbox.go)：Relay 的独立后台每 100 ms 执行一轮，多个 Relay 可并发检查和清理。没有在 API 增加清理调用，也没有按长度、年龄或单纯已读位置截断。Redis 本身仍串行执行脚本，这不是硬件层的资源隔离。
- [messaging.go](../internal/app/messaging.go)：仍然仅在 MQ `SendOK` 后 ACK，10 秒空闲 Pending 可接管。SQL 成功事实、用户券投影、补偿及对账规则不变；清理不触及库存预留账本。对账可从完整预留重新 XADD，所以旧 Stream ID 不是恢复的唯一依据。
- `TEST_STREAM_WORKER=1` 只支持 Worker/all 角色，以 20 个协程直接读同一消费组，最终 `Deliver` 完成才 ACK，仍使用原 Lua、SQL 事务、投影、幂等及补偿。默认关闭，不与 Relay 混跑，实验切换前排空并停止消费者；不提供在线迁移能力。
- `TEST_DISABLE_OUTBOX_CLEANUP=1` 仅作为清理前后实验的控制开关。库存账本、批次 Stream、死信、消费者元数据没有自动归档；XDEL 不保证等比例降低 Redis RSS。新增指标见 [架构说明](ARCHITECTURE.md)。

## 实验配置与复现

使用 [queue_experiment.py](../scripts/queue_experiment.py)，复用 [claims.js](../tests/k6/claims.js)、[support.py](../scripts/support.py)、现有 HTTP 审计和 Compose。k6 新增可选外部模板参数以便在开抢前注入故障，未修改领取/核销接口和业务规则。

```sh
docker compose -f compose.yaml -f compose.sharding.yaml up -d --build
python scripts/queue_experiment.py --output evidence/my-queue-run
python scripts/queue_experiment.py --output evidence/my-queue-run --summarize
```

脚本串行运行并在结束时恢复默认服务；不会删除业务卷或旧账本。可用 `--only normal|blocked|long-blocked|paused|crash|broker|cleanup` 选场景。同目录已完成结果经一致性和检查状态验证后跳过；完整重跑请换目录，避免覆盖旧结果。仅汇总不运行故障。

| 项目 | 实际设置 |
|---|---|
| 环境 | Docker Engine 29.8.1，28 逻辑 CPU，15.51 GiB 内存；同一开发机 |
| 存储 | 单个物理 MySQL，control 加两优惠券逻辑库，四张用户券表，经 Proxy 访问 |
| 并发 | 每轮 1000 请求、100 VU、库存 1000、每用户限 1；每轮新模板 |
| 后台 | 一个 Worker；MQ 模式通常一个 Relay，另测两个 Relay；MQ 默认消费并发 20，直连模式 20 个协程 |
| 持久性 | Redis AOF always / noeviction；RocketMQ SYNC_FLUSH；没有为数据提速放松正确性 |
| 顺序 | 每场景三轮、按轮交替 MQ/Stream 顺序；模板缓存提前加载，不是冷缓存可用性实验 |
| normal | 相同 Lua 受理后正常发券，两种后台传输对照 |
| blocked / long-blocked | 持有目标模板行锁 12 / 45 秒，其他 SQL 仍可访问；45 秒跨越 30 秒消费超时及对账窗口 |
| paused | `docker pause mysql` 12 秒，代表数据库进程冻结/暂不可响应，不等于真实断网或磁盘损坏 |
| crash | 持有模板锁，开抢约 2 秒后杀 Worker/MQ 模式的 Relay，约 12 秒后释放并重启 |
| broker | 消费者启动后停止 Broker，约 12 秒后请求恢复；直连组同样保持 Broker 停止作为对照 |
| cleanup | MQ 正常发券但关闭清理；记录全部已 ACK 载荷，再只开启 Relay 清理，记录前后 |
| multi-relay | 两个独立 Relay 同组消费并各自后台清理，验证最终唯一发券与排空 |

## 指标口径

HTTP 受理 QPS 仅以成功 200/202 数除以请求阶段窗口；最终发券 TPS 以唯一券数除以开抢至 SQL 事实、用户券投影与 Redis pending 收敛的窗口。k6 每秒观察一次排空，指标包含观测粒度，最终 TPS 在故障场景还包含故障等待。不能把这些数值解释为纯 Worker 的瞬时处理能力或持续容量。

约每 0.5 秒用一次 Redis 快照采集 `used_memory`、Outbox `MEMORY USAGE`/XLEN、PEL、unread，以及本轮 `meta/stock/users/reservations/states/revision` 的内存和预留条数。`MEMORY USAGE` 使用 Redis 默认采样估计口径，用于同口径对比，不是精确逐字节分配测量。Outbox 内存包含消费者组/PEL 元数据，XLEN=0 仍可占内存；Redis 总量还包括缓存、连接缓冲、旧活动及新券缓存，不能全部归因于消息。没有测量或承诺 Redis allocator RSS 的同比例下降。

工件保留请求错误计数、HTTP P95/P99、全过程内存样本、故障动作时间、恢复请求到观察排空时间、最终 issued/projected/pending 和唯一用户数。故障恢复计时不包含前面的 12/45 秒故障注入，但包含重启与轮询等待；不是生产 RTO。短时窗口可以得到超过每秒固定窗口限流数值的 QPS，不代表稳定超过限流设置。

## 实测结果与负结果

完整逐轮数值及各场景三轮中位数由脚本生成：[RESULTS.md](../evidence/queue-isolation-2026-11-11/RESULTS.md)、[summary.json](../evidence/queue-isolation-2026-11-11/summary.json)。以下比较均为中位数。

| 正常负载 | HTTP 受理 QPS | HTTP P95 ms | 最终发券 TPS |
|---|---:|---:|---:|
| Relay + MQ | 4545.45 | 26.37 | 230.20 |
| Stream 直连 | 4672.90 | 25.55 | 230.10 |

正常负载最终 TPS 基本相同，MQ 方案没有受理或延迟优势。模板行锁、控制库事务、Proxy 投影与缓存回读仍是共同成本；新增 MQ 不能解除 SQL 热点，三轮短测也不足以作显著性或最大容量结论。

| SQL 受阻时 | MQ：Stream 条数 / KiB | 直连：Stream 条数 / KiB | 两者本轮预留账本 KiB |
|---|---:|---:|---:|
| 模板锁 12 秒，恢复前 | 0 / 57.78 | 1000 / 308.33 | 509.24 |
| MySQL 暂停 12 秒，恢复前 | 0 / 77.16 | 1000 / 327.47 | 509.24 |
| 模板锁 45 秒，恢复前 | 0 / 138.07 | 2000 / 628.37 | 509.24 |

这支持“Broker 正常时，Relay 能把已转发载荷从 Redis 清走”，不是“SQL 变慢时 Redis 不再增长”。同场景 Redis 总量的峰值增量，锁阻塞时 MQ/直连约 2834/2787 KiB，MySQL 暂停时约 2995/2817 KiB；未得到总内存峰值下降的证据。全过程峰值与恢复前快照不是同一时刻；初始突发仍先全部进入 Redis，后续缓存与运行时开销也会影响总量。

清理开关实验三轮均将 1000 个已 ACK 载荷清为 0；Stream MEMORY USAGE 中位数从 371741 降至 127524 字节，减少 244217 字节（约 238.49 KiB）。本轮预留账本前后均为 513464 字节（约 501.43 KiB），1000 条预留完整保留。Redis 总 used_memory 的变化还受到 Relay 重启、连接和缓存到期影响，不能把其差值全部记为 XDEL 节省；消息清空也不等于 Stream 元数据归零。

45 秒阻塞下，直连的 2000 条是 1000 个业务事件及对账补投形成的重复载荷，不是发出 2000 张券。两种模式最终均恰好 1000 张；故障解除到观察排空中位数约 5.20/5.22 秒，最终 TPS（含 45 秒故障等待）约 20.47/20.48。MQ 在该实验没有排空速度优势。

Broker 停止时，MQ 方案仍可受理，但消息留在 Redis，不能获得转移积压的收益；发送失败、Pending 和对账补投还会增加成本。恢复请求到观察排空中位数为 22.45 秒；直连模式在 Broker 停止期间已完成发券，最终 TPS 约 230.52，MQ 则约 29.68（含故障等待）。不能把这个差异解释为正常条件下的服务能力倍数。

两个 Relay 的三轮实验最终 TPS 中位数为 228.88，未观察到相对一个 Relay 的吞吐提升。全部 42 轮、每轮 1000 张最终唯一发券，合计 42000 张，非预期 HTTP 错误计数为 0，最终 issued/projected 相等、pending=0 且库存一致；没有用放宽审计或忽略漏发来改善结果。

## 验证、代价与真实限制

[outbox_test.go](../internal/app/outbox_test.go) 包含真实 Redis 的清理回归：无组、未读、Pending、多个消费组、乱序 ACK、四个清理器并发、接管后清理、组数超限保护，以及直连 Worker 在业务提交后 ACK 前中断的恢复。确认顺序测试使用受控 Producer 返回值配合真实 Redis，验证发送失败、非 SendOK、确认后 ACK 丢失时保留消息；该测试不冒充真实 Broker 故障。独立 Broker/进程/AOF 恢复使用现有故障脚本和本次实验。

执行 `scripts/verify.py --faults`，以 `EVIDENCE_DIR=evidence/queue-isolation-2026-11-11/verification` 隔离保存新工件：常规测试、真实集成 race、vet、SDK 回归与原 SDK 失败对照通过，24 条 PASS 记录包含父测试/子测试，不是 24 套独立实验。额外回归验证删除 Outbox 载荷后仍可依据预留重新投递并完成发券。原有五类故障（含 Redis AOF、重复核销和补偿）全部通过；最终全局审计 287 个活动，不一致=0、pending=0，三角色健康且指标可访问。证据见 [verification](../evidence/queue-isolation-2026-11-11/verification/)。这是本机容器实跑，不将尚未运行的远端 CI 当作新增验证。

主要代价和未覆盖范围：

- 初始突发仍写两份 Redis 事件数据（预留与 Outbox）；清理只消除队列载荷，不消除持续增长的库存账本，已退出消费者元数据和批次/死信也仍需治理。
- 对账会重投较老 pending，包括已在 MQ 等待的事件；长时间 SQL 故障下既增加 MQ 重复投递，又可能短暂重新增加 Outbox。幂等保障正确性，不消除重复工作的成本。
- Relay 同步发送增加确认和刷盘成本；MQ 重投/重连可使恢复明显慢于故障解除。20 个配置消费协程不保证两模式内部调度完全相同。
- Redis 与 API 仍共享单个主节点的 CPU/内存；有界清理不是资源配额隔离。落后的其他消费组、清理速度不足、Broker 故障会保留消息，无法保证固定内存上限。
- 保留历史数据且没有重置 allocator，消费者元数据随实验轮次增长；交替顺序和新模板减少偏差，但不能完全消除背景对账、缓存过期、JIT 和短测采样误差。没有长稳容量、物理多实例、生产 HA、磁盘损坏或跨机容灾验证。

## 实习简历表述

构建 Redis Stream Outbox→RocketMQ 可靠发券链路，以全消费组 ACK/PEL 原子检查和有界后台清理释放已转发载荷；在数据库阻塞、消息重投及进程故障对照实验中验证最终库存与发券一致性，明确消息积压隔离收益及额外重试成本。
