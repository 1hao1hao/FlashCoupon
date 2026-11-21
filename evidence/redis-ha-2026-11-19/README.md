# Redis HA 验收工件

工作始于 2026-11-19，最终验收日期为 2026-11-21。部署、命令、指标口径与可靠性边界见 [REDIS_HA.md](../../docs/REDIS_HA.md)。本目录全部为合成业务及脱敏结果，不含环境密钥或冗长容器日志。

| 入口 | 用途 |
|---|---|
| [summary.json](summary.json)、[results.md](results.md) | 18 轮有效对照，分组中位数及逐轮审计 |
| [environment.json](environment.json) | 环境、实际镜像 ID、Redis 配置、源码哈希及最终全局审计 |
| [benchmark/verified-20261121](benchmark/verified-20261121/) | 正常负载：单机与 Sentinel WAIT=0/1/2，各三轮；只选择 scenario=normal |
| [benchmark/resilience-final-20261121](benchmark/resilience-final-20261121/) | 定速控制与切主，各三轮；每轮独立 k6、服务端采样和结果 JSON |
| [faults/run3/summary.json](faults/run3/summary.json) | 完成的十场景故障矩阵 |
| [recovery/acceptance-20261121/summary.json](recovery/acceptance-20261121/summary.json) | 最终五场景恢复及跨三角色 Trace |
| [recovery/final-20261121/same-connection-integration.json](recovery/final-20261121/same-connection-integration.json) | 真实 Sentinel 下固定连接 EVAL/WAIT 验证 |
| [final-regression-20261121](final-regression-20261121/) | 真实中间件原业务、race、vet、SDK 竞态回归及旧故障注入 |
| [manifest.json](manifest.json) | 本目录其余文件的 SHA-256；计算前统一 CRLF→LF，与 Git 中的文本一致 |

`faults/run1` 的 hostname/DNS/TILT、`run2` 的恢复编排、`benchmark/final-20261121` 的对照夹具认证、`verified-20261121/fault-window-failure.json` 的故障未重叠，以及 `benchmark/resilience-20261121` 的 SSH/采样来源失败均保留供复查，不进入最终统计。`regression` 和 `recovery/final-20261121` 中较早的成功检查也保留；最终证据优先使用表中入口。旧工件未覆盖或改写为成功。

重新汇总：`python scripts/summarize_redis_ha.py`。新实验必须选择全新标签；不要复用旧远端采样文件。每轮最终发券 TPS 含排空、重试和审计等待，不等于 HTTP 受理 QPS，也不代表生产容量。
