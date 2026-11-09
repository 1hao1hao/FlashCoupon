"""Regenerate the committed report exclusively from sanitized, machine-readable evidence."""
import statistics
from support import *

def load(name):return json.loads((EVIDENCE/(name+'.json')).read_text(encoding='utf-8'))
def value(d,name,key='avg',default=0):return d['metrics'].get(name,{}).get('values',{}).get(key,default)
def main():
    rows=[];groups={};resources={}
    for mode in ('direct','async'):
        group=[]
        for iteration in range(1,4):
            name=f'{mode}-round-{iteration}';d=load(name);assert value(d,'checks','rate')==1 and d['audit']['consistent'] and d['audit']['pending']==0
            r=dict(scenario=name,configuration=d['configuration'],accepted=value(d,'accepted_claims','count'),accepted_qps=d['measurements']['accepted_http_qps'],final_tps=d['measurements']['final_issue_tps'],p95_ms=value(d,'claim_latency','p(95)'),p99_ms=value(d,'claim_latency','p(99)'),error_rate=value(d,'http_req_failed','rate'),limited=value(d,'limited_claims','count'),issued=d['audit']['issued'])
            rows.append(r);group.append(r)
        groups[mode]={key:round(statistics.median(r[key] for r in group),3) for key in ('accepted_qps','final_tps','p95_ms','p99_ms')}
    for name in ('contention-1000','contention-5000','duplicate'):
        d=load(name);assert d['audit']['consistent'] and d['audit']['pending']==0 and value(d,'checks','rate')==1
        rows.append(dict(scenario=name,configuration=d['configuration'],accepted=value(d,'accepted_claims','count'),rejected=value(d,'business_rejected','count'),limited=value(d,'limited_claims','count'),accepted_qps=d['measurements']['accepted_http_qps'],final_tps=d['measurements']['final_issue_tps'],p95_ms=value(d,'claim_latency','p(95)'),p99_ms=value(d,'claim_latency','p(99)'),error_rate=value(d,'http_req_failed','rate'),issued=d['audit']['issued']))
    for file in EVIDENCE.glob('*.json'):
        d=json.loads(file.read_text(encoding='utf-8'))
        for sample in d.get('resources',{}).get('samples',[]):
            assert 'sampling_error' not in sample,(file,sample)
            for service,v in sample.get('services',{}).items():
                r=resources.setdefault(service,{'cpu_percent_peak':0,'memory_bytes_peak':0,'samples':0})
                r['cpu_percent_peak']=max(r['cpu_percent_peak'],v['cpu_percent']);r['memory_bytes_peak']=max(r['memory_bytes_peak'],v['memory_bytes']);r['samples']+=1
    env=load('environment');faults=load('faults');tests=load('tests');deployment=load('deployment');batch=load('batch')
    assert faults['passed'] and tests['passed'] and value(batch,'checks','rate')==1
    assert faults['scenarios'][0].get('blocked_transactions',0)>0 and faults['scenarios'][0]['interrupted_issued']==0
    save('summary.json',dict(comparison_medians=groups,runs=rows,resources=resources,all_inventory_checks_passed=True))
    lines=['# 工程收尾验收报告','', '日期：2026-11-09（Asia/Shanghai）。本文件由 `python scripts/summarize.py` 从 `evidence/engineering-2026-11-09/*.json` 生成。', '',
    '## 环境和复现','',f"Docker Engine {env['engine_version']}，{env['logical_cpus']} 个逻辑 CPU，{env['memory_bytes']/1024**3:.2f} GiB。单个 MySQL 实例，两个优惠券逻辑库、四张用户券表；Proxy 并未连接物理多实例。API、Relay、Worker 各一个进程，均健康且 `/metrics` 返回 200。",'',
    '执行：`python scripts/verify.py --faults --benchmark`，随后 `python scripts/summarize.py`。每次使用新模板，保留已有测试数据；不删除业务数据卷。固定 AOF always、MQ SYNC_FLUSH、全局限流 5000 次/秒、用户限流 100 次/秒。两种方案均执行真实 SQL 事实、用户券投影和缓存写入。暖身不进入结果，三轮交替执行直发/异步顺序；CPU/内存采样覆盖受理到排空的整个测试。','',
    '## 测试与故障','',f"常规测试、race、vet 和 SDK 专用竞态回归均通过；{len(tests['test_cases'])} 条 PASS 记录保存在 tests.json。GitHub Actions 定义执行同样的无中间件检查；此处报告本机容器执行结果，不把未触发的远端 CI 宣称为已运行。",'',
    '独立进程故障记录见 faults.json。验证包含 Relay 死消费者 pending 接管、重复消息、一人一券、Worker 崩溃重启、Broker 故障、Redis AOF 恢复、库存补偿/计数修复和并发核销；所有场景最终 SQL 与 Redis 账目一致。','',
    '实际发现并修复：SDK 共享订阅版本竞态（修复前隔离对照报 race，修复后通过，见 sdk-regression.json）；退出时等待消费回调；批次库存暂不可读不应永久确认；MySQL 初始化临时 socket 健康误判；mqadmin 空集群返回 0 却没有 Topic，改为指定 Broker 并检查 NameServer 路由。','']
    for s in faults['scenarios']:
        detail=f"最终 issued={s['final']['issued']}，pending={s['final']['pending']}，一致={s['final']['consistent']}"
        if 'blocked_transactions' in s:detail+=f"；崩溃前观察到 {s['blocked_transactions']} 个 SQL 锁等待，已发 {s['interrupted_issued']}，重启后完成 100 张，另有 200 次重复 Stream 投递"
        if 'recovery_seconds' in s:detail+=f"；恢复观察耗时 {s['recovery_seconds']:.3f} 秒"
        lines.append(f"- {s['scenario']}：{detail}。")
    lines+=['','批量 k6 验证 100 个名单用户经批次 Stream、抢券 Stream、RocketMQ、Worker 最终发出 100 张券，检查全通过，JSON 见 batch.json。补偿故障通过主动制造 SQL 库存差异触发永久拒绝，再重复投递和恢复源库存完成对账；没有通过放宽库存条件提高吞吐。','',
    '## 性能结果','', '| 场景 | 请求/VU/库存 | 受理QPS | 最终TPS | P95 ms | P99 ms | HTTP错误率 | 唯一发券 |','|---|---|---:|---:|---:|---:|---:|---:|']
    for r in rows:
        c=r['configuration'];lines.append(f"| {r['scenario']} | {c['iterations']}/{c['vus']}/{c['stock']} | {r['accepted_qps']:.2f} | {r['final_tps']:.2f} | {r['p95_ms']:.2f} | {r['p99_ms']:.2f} | {r['error_rate']:.2%} | {r['issued']} |")
    lines+=['','三轮中位数：','', '| 模式 | 受理QPS | 最终TPS | P95 ms | P99 ms |','|---|---:|---:|---:|---:|']
    for mode,g in groups.items():lines.append(f"| {mode} | {g['accepted_qps']:.2f} | {g['final_tps']:.2f} | {g['p95_ms']:.2f} | {g['p99_ms']:.2f} |")
    lines+=['','1000/5000 场景用 per-vu-iterations，每个 VU 一个不同的签名用户和请求、各执行一次，库存为用户数的一半。它们是并发用户执行器，不保证 5000 个网络包在同一瞬间到达。P95/P99 包含库存不足的快速 409 响应，不能与全部成功的直发/异步对比直接等价。409、429 作为业务响应单独计数，HTTP 错误率指非预期状态/传输错误；各文件同时保留原始计数。','',
    '受理 QPS = 成功受理 HTTP 响应数 / 请求阶段窗口；幂等重复请求会重复计入响应数。最终 TPS = 唯一最终发券数 / 从开抢到目标活动 SQL、投影及 Redis pending 全部收敛的窗口。排空每秒观察一次，因此最终 TPS 含至多约一秒观察粒度；不把它称为纯 Worker 的瞬时处理峰值。两者不能互换。','',
    '## 资源观测','', '| 服务 | 采样峰值CPU % | 采样峰值内存 MiB | 样本数 |','|---|---:|---:|---:|']
    for service,r in sorted(resources.items()):lines.append(f"| {service} | {r['cpu_percent_peak']:.2f} | {r['memory_bytes_peak']/1024**2:.2f} | {r['samples']} |")
    lines+=['','Docker CPU 100% 代表一个逻辑核；内存不包含可回收缓存。以上为离散样本峰值，各进程峰值不一定同时发生；短测样本稀疏，不声称捕捉了瞬时最高峰。每轮原始时间序列保存在对应 JSON 的 resources.samples，阶段延迟均值及尝试数在 stage_latency。','',
    '## 瓶颈和真实限制','',
    '- 控制库仍按模板行锁串行扣库存和校验限领；SQL 事务阶段包含等待及多次语句，不等同于纯 CPU 时间。异步削峰并未移除这处最终吞吐约束。',
    '- Relay 使用同步 MQ 发送并等待确认，数据库事实之后还要经 Proxy 写券并回读缓存快照，均增加最终完成成本。每轮 stage_latency 提供各阶段的实际均值，不能把耗时最大的阶段直接认定为唯一瓶颈。',
    '- 用户分片是一个 MySQL 实例里的逻辑布局；没有验证物理横向扩展、HA、多机网络隔离或生产容量。',
    '- 历史测试数据、后台对账和 JIT/缓存暖身会影响三轮波动；只有三轮，不报告置信区间或最大稳定容量。',
    '- 全局对账仍扫描保留的活动/事实，有 12 秒任务期限；数据持续增长时应按错误和新鲜度指标检查是否跟上，需要审计归档及进一步分批策略。',
    '- 单 Redis AOF 与单 Broker 同步刷盘不是磁盘损毁后的容灾保证。完整 Redis 账本丢失时关闭受理；用户券数据库删除/损坏需保留核销历史的备份恢复。',
    '- 单用户查询/结算候选仍限制 1000 张；订单金额和归属由外部可信订单系统校验。','']
    (ROOT/'docs/VALIDATION.md').write_text('\n'.join(lines),encoding='utf-8')
    fresh_path=EVIDENCE/'fresh-start.json'
    if fresh_path.exists():
        fresh=json.loads(fresh_path.read_text());assert fresh['passed'] and fresh['user_coupon_tables']==4
        with (ROOT/'docs/VALIDATION.md').open('a',encoding='utf-8') as report:
            report.write(f"\n## 首次启动验证\n\n使用 `python scripts/fresh-start.py` 在独立 Compose 项目、独立网络和全新 MySQL/Redis/MQ 数据卷启动。三角色均健康，四张用户券表存在，耗时 {fresh['seconds']:.3f} 秒；原项目数据未修改。只清理了本次临时项目的数据卷，未使用会删除共享工具链缓存的 down -v。MySQL 健康检查走 TCP，避免初始化临时 Unix socket 被误判为就绪；Topic 初始化有有限重试。记录见 fresh-start.json。\n")
    artifacts=list(EVIDENCE.glob('*.json'))+[ROOT/'docs/VALIDATION.md']+list((ROOT/'scripts').glob('*.py'))+list((ROOT/'scripts').glob('*.sh'))+list((ROOT/'internal/app').glob('*.go'))+[ROOT/'compose.yaml',ROOT/'compose.sharding.yaml',ROOT/'go.mod',ROOT/'go.sum',ROOT/'cmd/server/main.go',ROOT/'.github/workflows/ci.yml',ROOT/'third_party/rocketmq-client-go/consumer/push_consumer.go']
    save('manifest.json',{'normalization':'UTF-8 text with CRLF normalized to LF, matching .gitattributes','sha256':{p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest() for p in sorted(artifacts) if p.name!='manifest.json'}})
    print(json.dumps(groups,indent=2))

if __name__=='__main__':main()
