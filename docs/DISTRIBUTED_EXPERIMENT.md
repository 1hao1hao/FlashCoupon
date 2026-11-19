# Windows 客户端与 Linux 服务端实验

本轮真实使用 Windows 原生 k6 0.57.0 向 Linux 独立 Compose 栈发请求，通过 SSH 转发访问 loopback API；服务端自行采集资源/指标，客户端不把本地 Docker 当作远程 Docker。基础栈在本地 Docker Desktop，跨机栈在授权 Linux 目录，两处硬件不同，所以跨机差值不能仅归因网络。

## 安全部署与工具

准备 Docker Compose v2（支持 `!override`）、Python 3.10+、`pip install paramiko` 和 Windows k6 0.57.0。使用当前已有可信 SSH host key；脚本首次遇到新主机会将 key 记录在被忽略的 `reports/ssh_known_hosts`，仍应通过可信渠道确认指纹。SSH 密码仅放进当前进程的 `FLASHCOUPON_SSH_PASSWORD`，也支持本机 agent/key；不要写入命令脚本、文档或提交 .env。

```powershell
python scripts/remote_deploy.py --host LAB_HOST --user LAB_USER --directory /home/LAB_USER/project/Flashcoupon
```

脚本只部署明确给定的 `/home/...` 目录与项目 `flashcoupon-observe`，不删除卷。首次生成随机 API admin/user 签名密钥并以 0600 保存 `.env`，重新部署保留。生成的 `compose.remote.yaml` 使用 ports override：API/Relay/Worker 为服务端 `127.0.0.1:18080/18081/18082`、Jaeger UI 为 `127.0.0.1:18486`；MySQL、Redis、Proxy 不发布宿主端口，Broker/OTLP 也只在 Docker 网络中。

实验服务器的镜像仓库访问曾超时，本轮实际通过本地保存镜像、SFTP 上传并 docker load 启动。可复现离线入口：先本地 build runtime/Proxy，再保存所需镜像（文件在 ignored reports），通过 `--image-archive` 导入：

```powershell
docker compose -f compose.yaml -f compose.sharding.yaml build app proxy
docker save -o reports/images.tar flashcoupon-runtime:local flashcoupon-proxy:latest mysql:8.0.41 redis:7-alpine apache/rocketmq:5.3.1 jaegertracing/jaeger:2.21.0
python scripts/remote_deploy.py --host LAB_HOST --user LAB_USER --directory /home/LAB_USER/project/Flashcoupon --image-archive reports/images.tar
```

若本地 Proxy 实际镜像名不同，用 `docker tag` 映射到 `flashcoupon-proxy:latest`，对应远程 override。源码上传也支持 `--upload-only`。基础部署默认不开事务实验；实验入口叠加 `compose.transaction.yaml`，仅用于独立 TX Topic 对照。

## 分别运行与汇合

```powershell
# 本地服务器同机基线；不要同时运行两份编排脚本
python scripts/distributed_experiment.py --k6 reports/tools/k6.exe
# Windows -> SSH -> Linux，密码通过环境变量提供
python scripts/distributed_experiment.py --host LAB_HOST --user LAB_USER --directory /home/LAB_USER/project/Flashcoupon --k6 reports/tools/k6.exe --run-tag remote-new
python scripts/summarize_observability.py
```

每组 500 个唯一用户请求、30 VU、500 张库存、每人限领 1；三轮交替用例顺序。normal 比较 Trace 0/1、sample ratio=1；locked 真实锁住当前模板 12 秒；本地额外测 Worker 1/20/40（20 取 normal Trace-on）与 transaction。等待业务收敛后校验 SQL issued=projected=500、pending=0、Redis/SQL 库存一致。两种模式最终共用同一发券事务与投影规则；这里的“排空”指业务 pending 归零，不等于 Broker 的 half-message 系统队列完全清空。

`remote_client.py` 是纯 HTTP 客户端，可独立指定任意受信网络的 `--base-url`，无需 Docker 或指标端口，admin/user key 由环境提供。服务端 `server_collect.py --test-id ID --project PROJECT` 可独立运行；编排入口远程启动它，按 test ID 把结果拉回。健康请求的五次 HTTP 往返用于解释通道成本，包含服务处理和 SSH；不是 ICMP RTT。`--run-tag` 必须换新值以保留失败记录，不能重复 Trace ID 后当作独立样本。

当前编排提供的通道为 SSH。若实验机已有 Tailscale/WireGuard，可用纯 HTTP 客户端连接私网地址，手动按同 ID 配对服务端采集；**本轮未部署或测量这两种私网**，也不开放公网数据库。

保留的 27 轮健康 RTT 使用 Windows Python 3.12 的 monotonic，存在约 15.6ms 计时粒度，个别本机读数为 0，不能理解为零网络延迟。后续入口已改为高精度 perf_counter 并记录时钟分辨率；不回填或伪造原轮次的 RTT。k6 延迟与 QPS/TPS 不使用该 Python 时钟。

## 结果与指标口径

完整汇总：[results.md](../evidence/observability-2026-11-19/results.md)、[summary.json](../evidence/observability-2026-11-19/summary.json)。每个测试包含 k6 原始 JSON、`*-result.json` 最终审计、`*-server.json` 服务器采样，以及开启追踪时的 `*-trace.json`；[manifest.json](../evidence/observability-2026-11-19/manifest.json) 给出 SHA256，文本先将 CRLF 规范化为 LF，与 Git 提交内容一致，不受 Windows 换行影响。

27 轮有效实验共最终发券 13,500 张，均最终库存一致、投影完整、pending=0、k6 错误率为 0，服务器采集无错误。下表各项为三轮中位数，排空从 HTTP 受理完成后计时：

| 环境 / 场景 | Trace / Worker | HTTP 受理 QPS | 最终 TPS | P95 / P99 ms | 后台排空秒 |
|---|---|---:|---:|---:|---:|
| 本机 normal | 0 / 20 | 1724.14 | 211.86 | 40.12 / 62.23 | 2.07 |
| 本机 normal | 1 / 20 | 1742.16 | 213.13 | 41.00 / 64.27 | 2.07 |
| 本机 locked | 1 / 20 | 2272.73 | 34.75 | 35.12 / 37.63 | 14.17 |
| 本机 concurrency | 1 / 1 | 2202.64 | 94.45 | 20.35 / 48.36 | 5.07 |
| 本机 concurrency | 1 / 40 | 1724.14 | 211.69 | 25.43 / 59.85 | 2.07 |
| 本机 transaction | 1 / 20 | 1057.08 | 7.43 | 32.49 / 34.28 | 66.79 |
| 跨机 SSH normal | 0 / 20 | 860.59 | 104.41 | 54.41 / 99.95 | 4.21 |
| 跨机 SSH normal | 1 / 20 | 847.46 | 104.12 | 57.93 / 94.79 | 4.21 |
| 跨机 SSH locked | 1 / 20 | 1070.66 | 29.35 | 96.22 / 106.03 | 16.53 |

代表性的本机 normal Trace-on r1-final：客户端 `sql.lock_template` 均值约 56.38ms、commit 2.45ms、投影 2.86ms；locked r1 锁调用升至 537.10ms，Performance Schema 实际采到最多 210 条等待关系、20 个等待事务。结果支持模板锁竞争是重要瓶颈，但并非排除 MQ、投影和事务往返的其他成本。

资源只报告采样到的高点：上述 normal r1-final 的 Worker 为 54.44% CPU / 18.37 MiB，MySQL 54.38% / 799.40 MiB；同机 transaction r1 为 Worker 43.68% / 20.47 MiB、MySQL 259.34% / 782.10 MiB。Docker CPU 可超过 100%（使用多个核），内存是容器采样值；这些不是准确的瞬时峰值，也不能直接作公平的长期平均比较。逐进程完整采样与计算值保留在原始文件和汇总中。

transaction r1 的采样窗口观测到 1301 次回查、140 次后台补投计数增长。它们属于当前 API 进程，包括历史 half-message 的处理，不能说成“本轮 500 条消息各重试了多少次”。SQL digest/慢日志同样属于采样窗口内的整个控制库实例，包含审计和定时维护，且短实验可能漏掉开始阶段；保留这些数据用于定位，不据此计算每次发券的精确 SQL 成本。

- HTTP 受理 QPS = accepted 数 / 从 setup 到受理完成的窗口；不是含轮询的 `http_reqs rate`。
- 最终 TPS = SQL 成功张数 / 从 setup 到最终审计完成的时间，含后台排空及最多约 1 秒轮询粒度；不是数据库瞬时 commit 峰值。
- P95/P99 使用 claim 请求的 `claim_latency`；错误率取 k6 `http_req_failed`，额外要求所有业务 checks 通过。
- SQL 客户端阶段 histogram/Trace、池等待和服务端锁等待关系各有边界，见[追踪指南](TRACE_GUIDE.md)。`sql.lock_template` 不能直接说成引擎纯锁时间。
- CPU/内存保留 Docker 每进程瞬时采样，Redis INFO 内存另存；采样可能错过亚秒级受理峰值。不能拿没有峰值覆盖的样本宣称准确峰值资源开销。

已观察到：锁阻塞时 Redis 仍能快速受理，但后台 TPS 随等待显著下降；MySQL 热模板行串行化、多个 SQL 语句和分片投影仍限制最终吞吐。提高 Worker 并发只能增加并行机会/锁竞争，不能提升单行处理能力。Trace 开关的小样本差异存在噪声，不将其包装为优化收益。事务消息增加 admission SQL 和同步 Broker 交互，其实际代价以三轮结果为准。

失败/中断样本也保留，未写出 `*-result.json` 的不进入成功中位数。最初 SSH 转发默认 backlog=5，30 VU 出现本机连接拒绝；已改为 256，保留失败 JSON 与最终审计，不归因服务端超发。原生 k6 报告目录已从硬编码 `/reports` 改为 `REPORT_DIR`，Docker k6 显式保留 `/reports`，两种方式兼容。

## 局限

这是短时功能/性能对照，没有持续稳态容量测量或置信区间；Trace 全采样、Jaeger 内存存储、实验恢复任务和采集器均有开销。没有清空历史数据卷或强制清理 Broker half-message，因此事务实验不是无历史负载的纯协议开销测量。Linux 为共享实验机，没有独占 CPU，Docker Desktop 与 Linux 资源不同；不能把网络/硬件/存储差异拆成单一因素。MySQL 仍为单实例逻辑分片，不是物理扩容。本轮未宣称生产 HA、无限持久恢复或双队列天然更快。
