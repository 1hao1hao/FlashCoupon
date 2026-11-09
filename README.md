# FlashCoupon

多商户优惠券分发与核销后端：Go / Gin / MySQL / Redis Lua / Redis Streams / RocketMQ / Excelize / ShardingSphere-Proxy / Docker Compose / k6。

## 启动

需要 Docker Engine 与 Compose。推荐直接启用分片配置：

```powershell
docker compose -f compose.yaml -f compose.sharding.yaml up -d --build
Invoke-RestMethod http://localhost:8080/health/ready
```

API 地址 `http://localhost:8080`；MySQL `127.0.0.1:13306`；Redis `127.0.0.1:16379`；Proxy `127.0.0.1:13307`。所有宿主端口仅绑定本机。启动过程自动建表、准备 MQ 数据卷权限、创建 Topic、安装 MySQL JDBC 驱动。首次启动需下载镜像和依赖。

`docker compose up -d --build` 是不使用 Proxy 的轻量开发配置。两种配置使用不同的用户券物理表，已有数据时不要直接切换；需要迁移用户券、订单唯一约束及模板投影后再切流。验收脚本默认使用分片配置。

默认开发密钥 `ADMIN_KEY=dev-admin-key`、`USER_KEY=dev-user-key`，可通过环境变量或 `.env` 设置。管理员接口供营销后台调用，用户接口校验 HMAC 签名的用户凭证，不能通过请求体指定其他用户 ID。`USER_KEY` 仅由可信登录后端持有。

```powershell
# 在真实业务中由登录服务签发；此命令用于本地演示。
$token = docker compose run --rm -e INTEGRATION=0 toolbox go run ./cmd/token 1001 | Select-Object -Last 1
$userHeaders = @{Authorization="Bearer $token"}
$adminHeaders = @{'X-Admin-Key'='dev-admin-key'}
$now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$body = @{merchant_id=1;name='新客立减';stock=100;per_user=1;threshold=1000;discount=200;starts_at=$now-1;ends_at=$now+3600;valid_until=$now+86400;mode='async'} | ConvertTo-Json
$t = Invoke-RestMethod http://localhost:8080/admin/templates -Method Post -Headers $adminHeaders -ContentType application/json -Body $body
$claim = @{merchant_id=1;template_id=$t.id;request_id='checkout-1001-001'} | ConvertTo-Json
Invoke-RestMethod http://localhost:8080/v1/claims -Method Post -Headers $userHeaders -ContentType application/json -Body $claim
Invoke-RestMethod http://localhost:8080/v1/coupons -Headers $userHeaders
```

金额全部使用整数分，时间使用 UTC Unix 秒。优惠不叠加；结算返回按优惠金额排序的可选券，最多抵扣订单金额。订单金额应由可信订单服务校验后传入，本项目没有实现商品和支付系统。

## API

| 方法与路径 | 内容 |
|---|---|
| GET `/health/live`, `/health/ready` | 进程、数据库/Proxy/Redis 连通性；不代表 MQ 无积压 |
| POST / GET `/admin/templates` | 创建模板 / 查询全部模板 |
| POST `/admin/templates/:merchant/:id/status` | `{"status":"paused"}` 或 `active`；只影响新受理 |
| POST `/admin/templates/:merchant/:id/warm` | 幂等预热；库存账本丢失时拒绝重新初始化 |
| POST `/v1/claims` | `merchant_id,template_id,request_id`；202 表示持久受理 |
| POST `/v1/claims/direct` | 同上；仅允许 `mode=direct` 模板，同步落库基线 |
| GET `/v1/claims/:id` | 当前用户的数据库处理结果；尚未落库时 404，稍后重试 |
| GET `/v1/coupons` | 当前用户优惠券，包含已使用和已过期券 |
| POST `/v1/settlements` | `merchant_id,amount`；返回 `offers` |
| POST `/v1/coupons/:id/redeem` | `merchant_id,order_id,amount`；相同订单重复调用返回成功 |
| POST `/admin/batches/:merchant/:template` | multipart `file`，xlsx 首表首列，首行可为 `user_id` |
| GET `/admin/batches/:id` | 导入状态、唯一人数、已处理、受理/拒绝、最终已发/失败数量 |
| GET `/admin/audit` | 数据库库存、发券事实、用户券投影、Redis 库存、pending 和一致性 |
| POST `/admin/reconcile` | 重放待发事件、修复未完成投影和 Redis 计数；返回当前对账结果 |

管理员接口携带 `X-Admin-Key`，用户接口携带 `Authorization: Bearer <token>`。业务冲突返回 409，限流 429，基础设施故障 503。默认用户限流 100 次/秒，全局 `RATE_LIMIT=5000` 次/秒。活动时间为 `[starts_at, ends_at)`，券在 `valid_until` 时失效。模板发布后金额、库存和有效期不可变，修改营销规则时创建新模板。

单用户券列表和结算候选上限为 1000 张；单模板用户限领最大 100；模板库存最大 1000 万。导入请求最大 20 MiB、最多 100 万数据行，解压总量限制 128 MiB。非法行导致 `partial`，此前已持久排队的行仍继续执行；不会把整份名单一次加载进内存。批次 `accepted` 是 Redis 受理人数，`issued` 是数据库最终发券人数。

## 验证与压测

```powershell
# 完整验收：真实中间件、race、vet、k6 和故障注入。
./scripts/verify.ps1 -Benchmark -Faults

# 独立单元测试（可在服务运行时执行）
docker compose run --rm -e INTEGRATION=0 toolbox go test -race ./...

# 真实中间件集成测试。先停 app，避免测试消费者和部署消费者交错。
docker compose -f compose.yaml -f compose.sharding.yaml stop app
docker compose -f compose.yaml -f compose.sharding.yaml run --rm toolbox go test -race -count=1 -v ./internal/app
docker compose -f compose.yaml -f compose.sharding.yaml start app

# 100 并发、2000 请求竞争 1000 库存
docker compose -f compose.yaml -f compose.sharding.yaml run --rm k6 run -e MODE=async -e VUS=100 -e ITERATIONS=2000 -e STOCK=1000 -e REPORT_NAME=async-contention /scripts/claims.js
```

Linux/macOS 可使用同样的 Compose 命令；故障编排脚本需要 PowerShell 5.1+ 或 pwsh。k6 脚本位于 `tests/k6`，原始 JSON 与日志输出到 `reports`。`claim_latency` 统计抢券请求的 P95/P99；`measurements.accepted_http_qps` 使用抢券请求窗口；`final_issue_tps` 使用从开抢到所有已受理请求完成落库的窗口。重复请求的受理 QPS 包含幂等重放，最终 TPS 按唯一发券数计算。测试会等待排空再核对，超时或不一致导致失败。

实测记录见 [验证报告](docs/VALIDATION.md)，系统边界和故障处理见 [设计说明](docs/ARCHITECTURE.md)。
