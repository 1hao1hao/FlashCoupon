# 故障窗口、恢复与验收

本表以真实进程故障和中间件验证为依据。HTTP 202 只确认 Redis 受理，不代表 SQL 发券、分片投影或 Redis 终态完成。恢复通常允许重复消息，最终唯一性由控制库事务与投影幂等保证。

## 默认链路矩阵

| 注入位置 / 场景 | 谁负责恢复，为什么不能误补偿 | 本轮证据 |
|---|---|---|
| Redis 不可用，Lua 前失败 | API 返回 503，无预留；Redis 恢复后同 ID 重试 | `reliability/redis_before_lua.json` |
| Lua 成功，HTTP 响应前 API 被 kill | 预留与 Stream 已同时登记；Relay 继续；客户端同 ID 不重复扣库存 | `reliability/after_lua.json` |
| 上述窗口后 Redis AOF 重启 | 持久预留/事件恢复；不能仅根据响应丢失退库存 | `reliability/redis_after_lua_aof.json` |
| Relay 未读、反序及重复 Stream 事件 | 未读消息保留；启动 Relay 后发送；Worker 幂等收敛 | `reliability/unread_duplicate_out_of_order.json` |
| Relay 读取后被 kill | PEL 消息接管后重送，ACK 前不会清理 | `reliability/after_stream_read.json` |
| MQ SendOK 后、Stream ACK 前被 kill | 允许 MQ 重复投递；PEL 接管与 SQL 主键消除重复 | `reliability/after_mq_send.json` |
| Broker 停止再恢复 | 发送未确认不 ACK；Outbox 保留，恢复后排空 | `legacy-verification/faults.json` |
| SQL commit 前 Worker 被 kill | 未提交事务回滚；MQ/对账重新处理 | `reliability/before_sql_commit.json` |
| SQL commit 后 Worker 被 kill | 成功事实不可撤销；重放补投影与缓存 | `reliability/after_sql_commit.json` |
| SQL 成功、投影前 / 投影后被 kill | grants 券快照补齐未完成阶段，不覆盖已经核销的券 | `reliability/before_projection.json`、`after_projection.json` |
| 完成业务、MQ 消费确认前被 kill | MQ 重投；成功事实与投影保持一份 | `reliability/before_consume_ack.json` |
| 模板行锁持有 33 秒，超过回调 30 秒 | 超时是结果不确定，重试而非退库存；释放锁后收敛 | `reliability/callback_timeout.json` |
| SQL 明确永久拒绝，重复执行补偿 | failed 事实和 Lua 终态保护只归还一次 | `legacy-verification/faults.json` 及集成测试 |
| Redis 库存元数据缺失 / 不可安全恢复 | 根据账本修复可恢复情况；缺失可靠依据时 fail closed，不猜测可售库存 | `legacy-verification/tests.json` 的恢复测试 |
| Jaeger 停止 | exporter 失败独立于业务，5 张受理/发券/投影均通过 | `checks.json` |

相对路径均以 [本轮工件根目录](../evidence/observability-2026-11-19/) 为起点；[summary.json](../evidence/observability-2026-11-19/reliability/summary.json) 汇总 12 个定点场景。每个模板检查 SQL/Redis 库存、唯一成功事实、分片数、业务 pending=0；单用户上限为 1。8 个崩溃点额外重投三份事件，确认幂等。旧验收套件重新执行并另存 `legacy-verification`，没有覆盖历史工件。

## 确认和删除的边界

Stream PEL Pending 是消费组已经取出但未 ACK 的消息；业务 pending 是预留尚未终结。Relay 在 RocketMQ 返回 SendOK 后才 XACK，ACK 仅确认交接，不表示 MySQL 已发券。Worker 在 SQL 事实、投影、缓存和终态处理成功后返回 ConsumeSuccess，SDK 再推进消费确认。Stream 物理删除由后台有界清理负责，只删所有消费组已读且不在任一 PEL 的条目；预留账本与独立 trace hash 不随 XDEL 删除，仍能支持审计/补投。

重试重新做一次可幂等的操作；恢复从持久事实继续未完成阶段；补偿只处理已经确定的永久发券拒绝；对账检查跨组件差异并补齐投影、终态和待处理事件。它们不是同义词，任何超时都不足以证明 SQL 未提交。

## 事务消息实验

真实 Broker 5.3.1 + Go SDK 2.1.2 测试通过 Commit、Rollback、Unknown 后 Broker 回查，以及无 half-message 的孤立 Redis 预留后台恢复。后者直接构造持久预留且不调用 HTTP，不把它误称为“等到 Broker 回查次数耗尽的实测”。两处 Producer kill：Redis 已预留但 admission SQL 未写入、admission SQL 已提交但事务确认未完成；均在**客户端不重发**的条件下最终发券一张，随后同 ID 重放仍一张。

证据：[Broker 集成](../evidence/observability-2026-11-19/transaction/broker-integration.json)、[崩溃汇总](../evidence/observability-2026-11-19/transaction/crash-summary.json)。Broker 有有限回查预算，API 的后台扫描保留的事务预留并向独立 TX Topic 补发，不能声称 MQ 本身无限重试。

额外的并发 admission/delivery race 用例以 20 个并发请求提交 100 张券，最终 100 份事实/投影一致。共享实验 Broker 日志还观察到 183 次 half-message 回查预算耗尽记录，脱敏计数见 [check-budget-observation.json](../evidence/observability-2026-11-19/transaction/check-budget-observation.json)；这是整个保留实验栈的累计观察，不是某一轮失败率。最终本地/跨机全局审计均无未收敛模板，不能因此推断 Broker 自身提供无限恢复。

## 复现与边界

### Redis HA 扩展

保留上面的单机 Redis 验收。新增 [HA 文档](REDIS_HA.md) 与 [独立工件](../evidence/redis-ha-2026-11-19/)：覆盖 Master 停机、Replica 停机、Sentinel 单点/多数失效、私网隔离、切主并发、PEL 接管、重复投递、旧主回归、WAIT 超时及复制不足拒写。成功轮次检查 SQL/Redis 库存、唯一事实、用户限领、投影及业务 pending；失败轮次保留原因，不算通过。

恢复扩展还验证消费组丢失后从 0 重建、三个 Go 进程不重启而 Redis 全层 AOF/选主状态重启恢复、认证与私有端口，以及包含 HA guard/WAIT 的三进程 Trace。完整且内部平衡的历史丢失仍可能无法发现，不把这些定点测试推广为任意故障下零丢失保证。

```sh
python scripts/verify.py --faults
python scripts/reliability_matrix.py
# 已启动 compose.transaction.yaml，初始化独立 Topic 后：
python scripts/transaction_faults.py
python scripts/verify_observability.py
```

`verify.py` 的证据目录应通过 `EVIDENCE_DIR` 指向新目录，避免覆盖历史。故障门仅由 `FAULT_INJECTION_ENABLED/FAULT_POINT/FAULT_EVENT_ID/FAULT_GATE_DIR` 进程环境启用；宿主观察 reached 文件后 kill 指定角色，默认关闭，等待有界并支持取消。源码：[faults.go](../internal/app/faults.go)、[矩阵脚本](../scripts/reliability_matrix.py)、[事务测试](../internal/app/transaction_test.go)。测试会暂停/重启实验栈，请勿向生产或其他项目运行；不会删除数据卷。

原单机矩阵验证各组件重启、AOF 恢复及指定故障窗口；新增 Redis HA 扩展另有 10 场景矩阵、5 场景恢复验收和三轮并发切主对照，见 [REDIS_HA.md](REDIS_HA.md)。仍不覆盖磁盘永久丢失、Broker 集群灾难或任意网络分区。没有固定次数后自动告警或生产 HA 的实现；预留与追踪账本保留仍有内存成本，后续归档策略尚未实现。
