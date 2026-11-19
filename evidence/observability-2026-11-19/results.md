| 环境 / 场景 | Trace / Worker | HTTP受理QPS | 最终TPS | P95 / P99 ms | 排空秒 |
|---|---|---:|---:|---:|---:|
| cross-machine-ssh / locked | 1 / 20 | 1070.66 | 29.35 | 96.22 / 106.03 | 16.53 |
| cross-machine-ssh / normal | 0 / 20 | 860.59 | 104.41 | 54.41 / 99.95 | 4.21 |
| cross-machine-ssh / normal | 1 / 20 | 847.46 | 104.12 | 57.93 / 94.79 | 4.21 |
| same-machine / concurrency | 1 / 1 | 2202.64 | 94.45 | 20.35 / 48.36 | 5.07 |
| same-machine / concurrency | 1 / 40 | 1724.14 | 211.69 | 25.43 / 59.85 | 2.07 |
| same-machine / locked | 1 / 20 | 2272.73 | 34.75 | 35.12 / 37.63 | 14.17 |
| same-machine / normal | 0 / 20 | 1724.14 | 211.86 | 40.12 / 62.23 | 2.07 |
| same-machine / normal | 1 / 20 | 1742.16 | 213.13 | 41.00 / 64.27 | 2.07 |
| same-machine / transaction | 1 / 20 | 1057.08 | 7.43 | 32.49 / 34.28 | 66.79 |
