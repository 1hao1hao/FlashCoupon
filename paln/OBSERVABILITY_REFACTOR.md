# FlashCoupon：消息可靠性、全链路追踪与跨机器压测升级

仓库：https://github.com/1hao1hao/FlashCoupon

你是负责本项目架构、开发、测试和交付的高级 Go 后端工程师。

**目标：一次性完成端到端消息可靠性强化、OpenTelemetry + Jaeger 分布式 Trace、MySQL 性能观测、跨机器部署与压测，以及 RocketMQ 事务消息对照实验，使项目具备可复现、可解释、有量化指标的工程亮点。**

## 1. 执行原则

- 先完整阅读源码、`docs/PROJECT_GUIDE.md`、`docs/ARCHITECTURE.md`、`docs/VALIDATION.md`、`docs/QUEUE_EXPERIMENT.md`、现有测试和部署文件。
- 自主完成设计、实现、调试、实验、文档和测试；无需逐阶段向用户确认。
- 保留默认 `Lua → Stream Outbox → Relay → RocketMQ → Worker → MySQL` 链路，以及已有库存、限领、幂等、核销、补偿和分片逻辑。
- 保持已有接口、测试和实验兼容，不覆盖历史实验工件。
- 优先解决真实工程问题，不增加无意义的技术组件，不伪造数据。

## 2. OpenTelemetry + Jaeger 全链路 Trace

使用官方 OpenTelemetry Go SDK、W3C Trace Context 和 OTLP Exporter。新增独立可选 Compose 文件部署 Jaeger，保留现有 Prometheus 指标。

覆盖核心 Span：

1. Gin HTTP 请求、鉴权、限流。
2. Redis Lua 库存预扣与 Stream 入队。
3. Stream 排队等待、Relay 读取、MQ 发送及确认。
4. RocketMQ 等待、Worker 消费与重试。
5. MySQL 事务：BeginTx、锁模板、库存更新、发券事实插入、Commit。
6. 用户券投影、缓存更新、Redis Finalize。
7. 补偿、对账与故障恢复。

要求：

- 正确实现跨 HTTP、Redis Stream、RocketMQ 的 Trace Context 传播。
- 兼容原有 `Event`、稳定事件 ID、预留账本与幂等校验。不能为了添加 Trace 字段破坏事件一致性。
- 重试与对账补投保留可追溯的上下文；适当使用 Span Links 区分重试。
- 独立区分各队列等待时间和实际处理时间。
- 为 API、Relay、Worker 配置独立 `service.name`。
- 支持采样率、Exporter 地址和开关配置；Jaeger 不可用时不影响业务处理。
- 不在 Trace、日志或 Prometheus 标签中暴露用户凭证、密钥和敏感信息。
- 提供至少一个能在 Jaeger UI 中看到完整发券链路的集成验证。

## 3. MySQL 性能观测与瓶颈分析

- 细化发券事务各阶段耗时，区分数据库请求、锁等待、事务执行和提交。
- 结合 MySQL Performance Schema、Slow Query Log 等实际能力分析热点模板行锁。
- 记录数据库连接池等待、活跃连接和查询延迟。
- 增加数据库正常、人工锁阻塞、Worker 并发变化的对照实验。
- 将 SQL 观测与 Trace、原有 Prometheus 指标关联，明确受理 QPS 和最终发券 TPS 的区别。

## 4. 端到端可靠性故障矩阵

对关键故障窗口实现可重复的自动化注入与检查：

- Lua 执行前后 Redis 异常、HTTP 响应丢失及客户端同 ID 重试。
- Redis 已受理但 Relay 尚未读取。
- Relay 获取事件后崩溃。
- RocketMQ 收到消息后、Relay ACK 前崩溃。
- Broker 停机及恢复。
- SQL 事务提交前后 Worker 崩溃。
- SQL 已发券但用户券投影尚未完成。
- Worker 完成业务后、MQ 消费确认前崩溃。
- 消息重复、乱序、超时与补偿重放。
- Redis AOF 恢复、不可恢复状态下的安全停止。

所有故障实验检查最终库存、用户限领、唯一发券事实、分片投影、pending 和业务幂等，不以 HTTP 200/202 代替最终成功。

测试故障开关不得暴露为可被普通用户调用的线上接口；不得删除已有数据卷或修改历史实验数据。

## 5. RocketMQ 事务消息对照

在保持原主链路不变的前提下，实现隔离、可配置的事务消息实验模式。

- 使用真正的 RocketMQ Transaction Producer、Half Message、Commit/Rollback/Unknown 和事务回查机制。
- 将 MySQL 追加式领取事件作为本地事务事实；不把 Redis 与 MySQL 的两次调用伪装成单个 ACID 事务。
- Worker 复用现有库存约束、幂等发券与最终一致性机制。
- 独立 Topic、业务模式和实验数据，避免两套方案同时处理相同请求。
- 对照原 Stream Outbox 方案的 HTTP QPS、P95/P99、最终 TPS、SQL 压力、重试、回查和故障恢复。
- 根据当前 RocketMQ Broker/Go SDK 版本核验事务消息兼容性，并通过真实 Broker 集成测试；不可只做接口 Mock。

不以是否使用事务消息判断方案优劣，以实验结果说明设计取舍。

## 6. 本地与远程部署、跨机器实验

新增可复现的部署与压测支持：

**推荐部署：**
- Linux 服务器：API、Relay、Worker、Redis、RocketMQ、MySQL、ShardingSphere-Proxy、Jaeger。
- Windows 本地电脑：Git 仓库、k6 客户端、结果分析和浏览器。
- 通过 SSH Tunnel 完成远程连通性验证；性能对照支持 Tailscale/WireGuard 等私有网络直连。

具体要求：

- 增加 `compose.observability.yaml`，尽量保留原有 Compose。
- 生成不含真实密钥的 `.env.example`。
- 所有数据库、Redis、Broker 和 Jaeger UI 默认不暴露公网。
- 修复当前 `tests/k6/claims.js` 将报告写到 `/reports` 的硬编码，使 Windows 原生 k6 与 Docker k6 均可使用。
- 当前 `scripts/benchmark.py` 依赖本地 Docker Compose 与本机指标端口；新增独立的跨机器测试入口，不得简单复用后假装可以远程执行。
- 本地 k6 客户端能针对远程 API 创建测试模板、执行压测、轮询最终发券、保存报告。
- 服务器独立采集 API/Relay/Worker、Redis、MQ、MySQL 的资源与阶段指标。
- 支持按测试 ID 汇总两端结果、Trace、网络 RTT 和最终一致性审计。
- 提供同机与跨机、Tracing 开启与关闭、正常与故障三类可比实验。
- 使用相同负载、多轮测试，保存原始数据，报告 QPS、TPS、P95/P99、排队等待、SQL 延迟及恢复耗时。

## 7. 最终验收与交付

- 全部现有核心测试及新增测试通过。
- 使用真实 Redis、MySQL、RocketMQ 验证消息可靠性。
- Jaeger 能展示跨三个 Go 进程的完整 Trace。
- 实测 SQL 锁等待与端到端瓶颈。
- Windows 本地 k6 可以向 Linux 服务器发起请求并生成报告。
- 完成 RocketMQ 普通消息与事务消息的独立对照。
- 真实测试结果与原始工件可复现；无法完成的验收明确列出，不虚构性能提升。

新增：

- `docs/TRACE_GUIDE.md`：Trace 原理、Span 及 Jaeger 查看方法。
- `docs/RELIABILITY_MATRIX.md`：所有故障窗口、处理机制与测试结果。
- `docs/DISTRIBUTED_EXPERIMENT.md`：Windows + Linux 部署命令、压测流程与数据分析。
- `docs/ARCHITECTURE_COMPARISON.md`：Stream Outbox 与事务消息方案对比。
- `docs/RESUME_BACKEND.md`：根据真实实现与实验数据生成后端实习简历描述及高频面试追问。

完成后自行进行代码检查、测试、修复和文档更新，提交有意义的 Git commits，并输出简短交付报告。

**这是一次完整重构任务，不要拆成需要用户反复重新下达提示词的多个任务。**