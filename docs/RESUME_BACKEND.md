# 后端实习简历与追问

数字应选择同一份实验的口径。下面保留原始同步/异步对照，并以本轮故障与追踪成果补充，不把小规模不同环境的吞吐混成一组。

**FlashCoupon：高并发优惠券分发与核销系统**

**技术栈：** Go、Gin、MySQL、Redis Lua/Streams、RocketMQ、ShardingSphere-Proxy、Excelize、Docker Compose、k6、OpenTelemetry、Jaeger。

面向多商户营销，实现模板管理、限量抢券、批量发券和幂等核销，重点解决库存竞争、异步交接与跨存储状态收敛。

- **高并发抢券：** Redis Lua 原子预扣、限领与事件登记，异步隔离 SQL 写入；原同负载实验 HTTP 受理 QPS 266→2137、P95 122→32ms，最终 TPS 265→218，明确受理优化与落库瓶颈。
- **可靠发券：** Stream Outbox→Relay→RocketMQ，发送确认后 ACK，消费事务幂等、失败补偿及对账恢复；本轮覆盖 12 个定点故障场景及原有故障验收，最终事实、投影与库存审计一致。
- **可观测性与取舍：** 基于官方 OTel SDK 跨 API/Relay/Worker 传播 W3C 上下文，关联 SQL 阶段、连接池与真实行锁等待；独立实验验证 RocketMQ 事务消息回查和 Producer 崩溃恢复，保留性能负结果。
- **分片与缓存：** 通过 Proxy 在单 MySQL 的两逻辑库、四张用户券表路由投影；结合 Bloom Filter、空值缓存、互斥重建与用户券索引减少无效查询，不宣称物理扩容。
- **批量与核销：** 流式解析 Excel 名单、Redis Set 去重和后台批次恢复；SQL 条件更新与订单唯一约束保护重复核销，通过真实集成、race 与故障回归验证。

## 适合口述的追问

| 问题 | 先说什么，再展开什么 |
|---|---|
| 为什么 Redis 成功还不算发券成功？ | 它只预留库存和记录事件；SQL grants 是事实，再补用户券投影和 Redis 终态。 |
| 为什么不用单层 Stream？ | 单层可行。双队列把交接与数据库处理分离，但增加 Broker、重试与状态成本；历史对照未证明正常 TPS 优势。 |
| Trace 能证明正确性吗？ | 只能辅助定位；采样或导出可能丢。最终以库存等式、唯一事实、投影数和 pending 归零验收。 |
| 为什么事务消息不是分布式 ACID？ | Redis 预留与 admission SQL 分开提交，Unknown/回查/扫描修复窗口；最终发券仍是单控制库事务。 |
| 为什么超时不能退库存？ | 超时无法证明 SQL 未提交，贸然补偿会重新放出已卖库存；先查事实，补齐或重试。 |
| 最终 TPS 为什么没跟受理一起提升？ | 热模板行锁串行化、事务往返与投影仍存在；排队只是把等待从 HTTP 移到后台。 |
| SQL lock Span 是精确锁等待吗？ | 不是，含客户端调用全程；用 Performance Schema 等待关系和事务状态交叉验证。 |
| 跨机实验能说明什么？ | 已验证原生 Windows k6→Linux 全流程和数据配对；SSH、硬件差异与短负载限制不能推出生产容量。 |

数据来源：[基础验收](VALIDATION.md)、[历史双队列实验](QUEUE_EXPERIMENT.md)、[本轮汇总](../evidence/observability-2026-11-19/summary.json)、[故障矩阵](RELIABILITY_MATRIX.md)。完整面试主线仍见 [INTERVIEW_CORE.md](INTERVIEW_CORE.md)。没有实现自动告警、跨机数据库扩容或生产多副本容灾，不写入简历。
