# FlashCoupon

多商户优惠券分发与核销后端：Go、Gin、MySQL、Redis Lua/Streams、RocketMQ、Excelize、ShardingSphere-Proxy、Docker Compose、k6。提供模板管理、限量抢券、白名单批量发放、查询、优惠结算、幂等核销、过期流转、补偿与对账。

## 部署

```sh
docker compose -f compose.yaml -f compose.sharding.yaml up -d --build
docker compose -f compose.yaml -f compose.sharding.yaml ps
```

| Compose 服务 | ROLE / 职责 | 本机接口 |
|---|---|---|
| app | api：业务 HTTP、Lua 受理、同步发券基线 | `127.0.0.1:8080` |
| relay | relay：Stream → RocketMQ，确认发送后 ACK | `127.0.0.1:8081` |
| worker | worker：MQ 落库、批次消费、定时对账与过期流转 | `127.0.0.1:8082` |

三个服务独立进程、独立健康检查；均提供 `/health/live`、`/health/ready`、`/metrics`，只有 API 开放业务路由。API 不依赖 Broker 启动，Broker 故障期间仍可持久受理。Relay/Worker 等待 Topic 创建完成；分片模式均等待 Proxy。SIGTERM 时 HTTP 停止接入并排空，后台消费者取消和等待退出，Compose 退出宽限 60 秒；重启恢复依靠 Stream PEL、MQ 重投与数据库幂等。

MySQL：`127.0.0.1:13306`；Redis：`127.0.0.1:16379`；Proxy：`127.0.0.1:13307`。当前只有**单个 MySQL 实例，内含两个优惠券逻辑库、四张用户券分片表**，不是物理多实例扩容。`control` 是同实例中的控制库。商户模板还有独立的四张查询投影分表。

不带 `compose.sharding.yaml` 可运行无 Proxy 开发模式。已有数据时不能直接切换两种存储布局，应先迁移券、订单唯一约束及模板投影。启动自动建表、初始化 MQ 数据卷权限、创建 Topic 和安装 JDBC 驱动；首次需要网络下载依赖。Redis 使用 AOF `appendfsync always`/`noeviction`，Broker 使用 `SYNC_FLUSH`。

## 调用

默认开发密钥为 `ADMIN_KEY=dev-admin-key`、`USER_KEY=dev-user-key`；可通过环境变量或本地 `.env` 覆盖，`.env` 不提交。管理员接口携带 `X-Admin-Key`，用户接口使用 `Authorization: Bearer <token>`。签名密钥仅由可信登录后端持有；请求体不能指定其他用户 ID。

```sh
# 本地签发演示用户凭证；真实部署由登录服务签发。
docker compose run --rm -e INTEGRATION=0 toolbox go run ./cmd/token 1001
```

创建模板 JSON 示例（时间替换成有效 UTC Unix 秒）：

```json
{"merchant_id":1,"name":"new-user","stock":100,"per_user":1,"threshold":1000,"discount":200,"starts_at":1794210000,"ends_at":1794296400,"valid_until":1794382800,"mode":"async"}
```

| 方法和路径 | 请求 / 行为 |
|---|---|
| POST / GET `/admin/templates` | 创建 / 查询模板 |
| POST `/admin/templates/:merchant/:id/status` | `{"status":"paused"}` 或 `active`；只影响新受理 |
| POST `/admin/templates/:merchant/:id/warm` | 幂等预热；已有库存账本丢失时拒绝重置 |
| POST `/v1/claims` | `merchant_id,template_id,request_id`；202 仅表示持久受理 |
| POST `/v1/claims/direct` | 同上，仅限 `mode=direct` 模板；同步落库基线 |
| GET `/v1/claims/:id` | 当前用户的 SQL 处理结果；尚未落库时 404，可重试 |
| GET `/v1/coupons` | 当前用户优惠券 |
| POST `/v1/settlements` | `merchant_id,amount`；按优惠金额返回可选券 |
| POST `/v1/coupons/:id/redeem` | `merchant_id,order_id,amount`；条件更新与订单幂等 |
| POST `/admin/batches/:merchant/:template` | multipart `file`，xlsx 首表首列；可有 `user_id` 表头 |
| GET `/admin/batches/:id` | 唯一人数、受理/拒绝、最终发券/失败数 |
| GET `/admin/audit` | 库存、SQL 事实、分片投影、pending 和一致性；可同时指定 `merchant_id`、`template_id` 查询单活动 |
| POST `/admin/reconcile` | 重试待发事件、修复缺失投影、CAS 修复 Redis 计数 |

金额为整数分，优惠不叠加，最多抵扣订单金额。订单归属和金额须由可信订单服务校验，本项目不实现商品/支付。活动窗口为 `[starts_at,ends_at)`；发布后金额、库存、有效期不可变。业务拒绝 409、限流 429、基础设施错误 503。默认用户 100 次/秒、API 全局 `RATE_LIMIT=5000` 次/秒。

单用户列表/结算候选上限 1000 张；单模板每用户最多 100 张，库存最多 1000 万。xlsx 请求最大 20 MiB，最多 100 万数据行，解压限制 128 MiB。非法行产生 partial；此前已排队的行继续执行。批次 accepted 是受理数，issued 是最终 SQL 发券数。

## 指标

三个进程均可直接抓取 `/metrics`，无需新增 Prometheus 服务。抓取配置示例见 [scrape.yaml](deploy/prometheus/scrape.yaml)，指标口径和 PromQL 见 [架构说明](docs/ARCHITECTURE.md)。只使用路由模板、HTTP 方法/状态、角色、处理阶段等有限标签，不把用户、商户、请求或模板 ID 放进标签。

## 可复现验收

需要 Docker Compose v2；完整编排另需 Python 3.10+，仅使用标准库，支持 Windows/Linux/macOS。

```sh
# 真实中间件、常规/race/vet、SDK 竞态回归；恢复独立服务并检查健康/metrics。
python scripts/verify.py
# 额外执行进程重启、Broker 故障、Redis AOF、补偿与核销故障验收。
python scripts/faults.py
# 三轮配对对比 + 1000/5000 并发 + 重复请求 + 批量发券，含排空和 CPU/内存采样。
python scripts/benchmark.py
python scripts/summarize.py
# 可选：使用独立网络和全新数据卷验证首次启动，结束后仅清理本次临时数据卷。
# 此入口的端口覆盖使用 Compose 2.24+；先执行上述启动/构建命令。
python scripts/fresh-start.py
# 一次执行全部：
python scripts/verify.py --faults --benchmark
```

无需 Python 的真实集成入口：`sh scripts/integration.sh`（Docker + POSIX shell）。PowerShell 兼容入口：`./scripts/verify.ps1 -Benchmark -Faults`。脚本不删除数据卷，会停止/恢复本项目服务，请串行执行故障与压测。GitHub Actions 对每次 push/PR 执行常规测试、race、vet 以及 SDK 竞态回归；真实中间件验收独立运行。

原始冗长日志仅保留在忽略的 `reports/`。必要脱敏 k6 JSON、CPU/内存样本、测试配置、故障记录及摘要提交到 [evidence](evidence/engineering-2026-11-09)。[VALIDATION.md](docs/VALIDATION.md) 由汇总脚本生成，不把受理吞吐当成最终发券吞吐。

RocketMQ Go v2.1.2 存在本次真实 race 验证捕获的订阅版本并发写问题；项目固定使用附带回归测试的最小本地源码补丁，来源和改动见 [ROCKETMQ_PATCH.md](third_party/ROCKETMQ_PATCH.md)。未更换客户端、消息链路或中间件。
