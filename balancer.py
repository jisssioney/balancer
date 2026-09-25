#!/usr/bin/env python3
"""平滑加权轮询（smooth weighted round-robin）后端池，仅用标准库。

入口：python balancer.py run
stdin 为 UTF-8 JSON：{"ops": [...]}，成功时 stdout 输出
{"results":[...],"backends":[...]}（无多余空白，末尾恰好一个换行）。
任何错误都不产生 stdout，只向 stderr 写一行 {"error":"..."} 并以约定码退出。

连接管理：open/close/get 按 cid 跟踪活动连接；open 选活动连接数最少的
后端（并列取最早加入者），remove 只允许删除无连接的后端。open/close/probe
的 now 共用同一时钟，依序不得递减。

健康探测：后端新增即 healthy，默认阈值 fail=3、success=2，连续计数从 0 起。
hset 只替换阈值并清连续计数，不改健康状态；probe 按 ok 递增对应连续计数
（封顶阈值）并清零另一项，healthy 连续失败达 fail 转 unhealthy，unhealthy
连续成功达 success 转 healthy，迁移时计数与平滑当前权重一并清零。同一
(id, now, ok) 的 probe 重报幂等；同 (id, now) 而 ok 不同属冲突重报，报
INPUT。pick/open 只作用于 healthy 后端；无 healthy 后端时报 STATE。

权重预热与调权：三键 add（op,id,weight）视为 d=0，目标权重立即生效，旧
行为不变；五键 add 键集 op,id,weight,d,now，d=0 立即取目标，d>0 自 now
起从 1.00 线性预热到目标权重，end=start+d，到达后取目标，插值向下取整到
0.01。ws 同键集，调权自当前有效权重线性过渡到新目标并清零平滑 current；
unhealthy 时调权报 STATE。add/ws 同参重报幂等，同 (id,now) 异参报 INPUT，
重复 add、未知 ws/wg 报 BACKEND。后端 unhealthy 期间有效权重固定 0.00；
probe 恢复 healthy 时以该 probe 的 now 从 1.00 按原 d 重启预热。wg 键集
op,id,now，返回键序 op,id,target,effective,stage,start,end：target 为整数
目标权重，effective 为两位小数字符串，stage ∈ warm/steady/unhealthy，仅
warm 带整数 start/end，其余为 null。pick 以当前有效权重执行旧平滑算法；
open 按连接数升序、有效权重降序、加入顺序选取；chash/route 忽略权重；
backends.weight 始终为目标整数权重。

熔断器：cs 键集 op,id,n,m,r,w,q（n,q,r ∈ [1,100]，m ∈ [1,n]，w ∈ [1,10^9]，
均非 bool 整数），首配或异参重配置 C 并清空窗，同参重报幂等；配置可选，
未配后端的 cr/cg 报 STATE，remove 后重加即回到未配。cr 键集 op,id,ok,now
（ok 仅 bool）：C 保留最近 n 次报告，样本数 ≥ m 且失败数×100 ≥ r×样本数
转 O 并记 next=now+w；O 且 now<next 的 cr 报 STATE，否则先转 H；H 中失败
立即重开并重算 next，连续 q 次成功转 C 并清窗。同 (id, now, ok) 的 cr 重报
幂等，同 (id, now) 异 ok 报 INPUT。cg 键集 op,id,now，触发 O 到期转 H，返
回键序 op,id,state,count,fail,reason,next,used：state ∈ C/O/H，count/fail
为窗口样本与失败数，reason 非 C 为 rate 否则 null，next 仅 O 为整数否则
null，used 仅 H 为已报告数否则 0。cs/cr 返回 op,ok。cr/cg 的 now 纳入同一
非递减时钟。pick/open/route 只选 healthy 且熔断状态为 C（含未配）的后端；
粘性目标非 C 时沿原环迁移且不迁回。

一致性哈希：chash 配置每个 healthy 后端的虚拟节点数 vnodes（1..1024），
同值幂等、异值生效。每个 healthy 后端为 i=0..vnodes-1 生成令牌
SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，摘要按 256 位大端无符号数
排序（并列按加入顺序、i 升序）构成哈希环。route 哈希 UTF8(key) 取首个
不小于它的令牌，越界回绕；首见 key 建立映射，目标 healthy 时命中
（sticky），增删后端或改 vnodes 不迁移，目标不可用时依环迁移且不迁回
（remapped）。route 不改连接数；未 chash 或无 healthy 后端时报 STATE。

优雅摘除：ds 键集 op,id,t（t ∈ [1,10^9] 非 bool 整数）登记排空时限，
同值幂等，D 时改值报 STATE，否则覆盖。dr/du/dg 键集 op,id,now，now
为非负非 bool 整数，纳入共用非递减时钟。已 ds 的 A 执行 dr：start=now、
deadline=now+t、forced=0，有连接转 D，无连接转 X 且 end=now；D/X 再
dr 幂等。D 不参与 pick、open 与新 route 映射，连接仍可 close，原粘性
route 仍命中；最后连接关闭即转 X，end=close.now。dg 在 D 且
now>=deadline 时强关其全部连接，forced=关闭数，转 X，end=deadline；
X 的旧粘性按原环迁移。du 转 A，取消 D 时 end=now，不恢复已强关连接，
调度仍要求健康且熔断为 C，慢启动不重置。ds/dr/du 返回 op,ok；dg 返回
op,id,state,connections,start,end,deadline,forced，state ∈ A/D/X
（可用/排空/已摘除），未开始时三时间为 null。未知 id 报 BACKEND，
未 ds 的 dr/du/dg 报 STATE。

令牌桶：ls 键集 op,scope,id,r,b,now，scope ∈ B/C/S（后端/客户端/
服务类），id/c/s/key 均为非空 UTF-8 串，r,b ∈ [1,10^9] 非 bool 整数，
now ≥ 0 纳入共用非递减时钟；B 桶的 id 须为现存后端，否则 BACKEND。
桶以 (scope,id) 唯一，新桶满令牌起步，同 (r,b,now) 重报幂等（不补充
不推进时钟），其余 ls 一律重配置并置 t=b、at=now；remove 同步删除其
B 桶。la 键集 op,c,s,key,now：先按原 route 语义选后端（含粘性建立
与迁移，未配环或无可选后端报 STATE），再检查该后端 B 桶、客户端 C 桶
（以 c 标识）、服务类 S 桶（以 s 标识），未配置即不限制。各在配桶先
作 t=min(b,t+(now-at)*r)、at=now，均有 t>=1 才各减 1，否则 RATE/6
（无 stdout、整批原子）；成功返回键序 op,backend,ok，ok=true。
lg 键集 op,scope,id,now，按同样规则补充但不消费，查未配置桶报 STATE；
返回键序 op,scope,id,r,b,t,at，值均为整数。ls 返回 op,ok。桶操作
O(1)，la 继承 route 的复杂度上界，空间 O(B+K)。

排队接纳：os 键集 op,cap,q,ttl（均 1..10^6 非 bool 整数），依次为每后端
连接上限、FIFO 容量、等待时限；首配或同参返回 op,ok，异参报 STATE。
oa 键集 op,cid,flow,c,s,key,now：cid/flow 同 open，c/s/key 同 la，now
纳入共用非递减时钟。活动或排队中 cid 重复报 CONNECTION。先按 la 的路由
语义选后端（未 chash 或无可选后端报 STATE），再对在配桶补充检查但不消费，
目标另须排空 A 且连接数 < cap；令牌不足或目标不满足均阻塞入队，返回键序
op,cid,state,backend：接纳为 A 加后端 id（此时才耗令牌并按 open 建连接，
opened_at=now），阻塞为 Q 加 null；队满尾拒绝报 OVERLOAD/7。ot 键集
op,now：先删除全部 now ≥ 入队 now+ttl 的排队项，再自队首逐项按 oa 规则
重试接纳（opened_at=now）至首个阻塞即停，返回 op,expired,admitted，两
数组均按 FIFO 列 cid；ot 至多 q 次 route，空间 O(q)。og 键集 op，返回
op,queue，queue 为 FIFO cid 数组。oa 未 os/chash、ot/og 未 os 报 STATE；
非法键、类型、范围、编码或时钟倒退报 INPUT。

请求度量：mr 键集 op,id,ok,ms,retries,remaps,now，id 须现存否则 BACKEND，
ok 仅 bool，ms/retries/remaps/now 四数均为 [0,10^9] 非 bool 整数，now 纳入
共用非递减时钟。每后端按 window=now//60 只保留当前窗统计，换窗即全部清零；
每报 requests 加 1，ok=false 时 errors 加 1，并累加 retries/remaps；ms 按
上界 [1,10,100,1000] 落入五整数桶（≤1、≤10、≤100、≤1000、>1000），各计数
封顶 10^18。mr 返回 op,ok，ok=true。mg 键集 op,id,now，返回键序
op,id,window,requests,qps,concurrency,errors,error_rate,latency,retries,
remaps,removed：window=now//60，concurrency 为活动连接数，latency 为五整数
桶，qps=requests/60、error_rate=100*errors/requests（零请求为 0）均下截为
两位定点串；removed 依次取 drain（D/X）、health（unhealthy）、circuit（熔断
非 C）、fault（fs 登记且 now 在 [a,z) 窗口内：D 恒为故障，F 仅于
((now-a)//v)%2=0 相位为故障；S、非故障相位及无前述状态为 null），否则 null；
查询时已跨入新窗（含从未 mr）按零计且不改存储。每次 fx 完成只追加一次同构
度量、不新增结果项：归属 id 取 fx 结果 backend，backend 为 null 时取环遍历
首个后端，环内无候选不记；字段为 ok=(state 为 A)、ms=latency、
retries=remaps、remaps=remaps、now=fx.now。mr 与 fx 自动度量按操作顺序
累加；失败批次不留度量。remove 后重加统计归零。mr/mg 均 O(1)，空间 O(B)。

配置导出与热加载：ce 键集仅 op，返回键序 op,config；config 精确键序
{version,backends,vnodes,limits,overload}：version=1；backends 按加入序，
项 {id,weight,d,fail,success,circuit,drain}，circuit=null 或 {n,m,r,w,q}，
drain=null 或登记的 t，均只含登记值不含运行态；vnodes=null 或整数；limits
项 {scope,id,r,b}，按 scope 的 B/C/S 序、id 的 UTF-8 字节升序；overload=
null 或 {cap,q,ttl}。ci 键集 op,config,now，结果 op,ok=true；now 为非负
非 bool 整数并纳入共用非递减时钟，各值沿用 add/hset/ws/chash/cs/ds/ls/os
的类型与范围。非法结构、类型、范围、编码、时钟及重复后端/限流项判 INPUT/2，
B 限流引用未知后端判 BACKEND/3，有活动连接或排队项判 STATE/4，依次判错。
成功时原子替换配置并以 now 重建默认运行态（全部 healthy、d>0 自 now 起算
预热、熔断 C 空窗、排空 A、桶满、队空、粘性清空、度量归零）；失败回滚不变更。
ce/ci 均 O(B+L) 时空（L 为限流项数）。

时钟故障演练：fs 键集 op,id,k,a,z,v（a,z,v ∈ [0,10^9] 非 bool 整数，
a<z；k ∈ D/F/S，D 须 v=0，F/S 须 v>0）为后端登记故障演练，同参重报
幂等、异参覆盖，remove/ci 清除，返回 op,ok；未知 id 报 BACKEND。fx
键集 op,cid,flow,key,timeout,now（cid/flow/key 沿用 open/route 的校验，
timeout/now ∈ [0,10^9] 非 bool 整数，now 纳入共用非递减时钟），未
chash 报 STATE，重复 cid 报 CONNECTION。环同 route（仅健康、熔断 C、
排空 A 后端），自 key 哈希点遍历不同后端，不读写粘性映射；a≤now<z
时 D 不可用、F 于 ((now-a)//v)%2=0 时不可用、S 可用且耗时 v，否则耗
时 0；跳过 D/F 时 remaps 加 1，环外不计。首个可用后端耗时 ≤ timeout
则按 open 建连、state=A；超限不建连，state=R、backend=该 id、
latency=耗时；无可用项则 R、backend=null、latency=0。结果键序
op,cid,state,backend,latency,remaps。fx 完成后按归属 id 追加一次等价
mr 度量（见请求度量段），结果项本身不变。fs O(1)，fx 仍为 O(BV)。

确定性操作记录：record 把原始 stdin 字节作为全新 run 输入执行，无论底层
成功或按既有错误失败，均退出 0、stderr 为空，stdout 输出一行紧凑 JSON
记录，键序 version,stdin,exit,stdout,stderr：version=1（非 bool 整数），
exit 为实际码 0/2/3/4/5/6/7，三个字节字段为带标准填充的 RFC4648 Base64。
replay 只接受该精确键集且拒绝重复键；类型、版本、退出码、解码失败或非
规范 Base64 均报 INPUT/2（无 stdout）。合法记录在全新状态重执解码的
stdin 并逐字节比较退出码、stdout、stderr；不符时 stderr 写
{"error":"REPLAY"} 加换行、退出 8、无 stdout；一致时原样写出记录的
stdout、stderr 并采用记录退出码。两者额外时空 O(I+O)。
"""

import base64
import bisect
import hashlib
import json
import sys
from collections import deque

EXIT_INPUT = 2
EXIT_BACKEND = 3
EXIT_STATE = 4
EXIT_CONNECTION = 5
EXIT_RATE = 6
EXIT_OVERLOAD = 7
EXIT_REPLAY = 8

DEFAULT_FAIL = 3
DEFAULT_SUCCESS = 2

# mr 各计数（requests/errors/retries/remaps/延迟桶）封顶 10^18。
METRIC_CAP = 10 ** 18


class _Failure(Exception):
    """fail() 抛出的内部异常：携带退出码与错误标签，由最外层统一落盘。"""

    def __init__(self, exit_code, label):
        super().__init__(label)
        self.exit_code = exit_code
        self.label = label


def fail(exit_code, label):
    raise _Failure(exit_code, label)


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def parse_cid(value):
    if not isinstance(value, str) or value == "":
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_backend_id(value):
    if not isinstance(value, str) or value == "":
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_now(value):
    # bool 是 int 的子类，必须显式排除。
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_warm_now(value):
    # 新操作 add(d)/ws/wg 的 now ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_threshold(value):
    # bool 是 int 的子类，必须显式排除。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 100
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_rate(value):
    # 令牌桶速率 r ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_burst(value):
    # 令牌桶容量 b ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_duration(value):
    # 预热时长 d ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_circuit_window(value):
    # 熔断恢复窗口 w ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_vnodes(value):
    # bool 是 int 的子类，必须显式排除。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 1024
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_queue_param(value):
    # os 的 cap/q/ttl ∈ [1, 10^6]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 6
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_metric_num(value):
    # mr 的 ms/retries/remaps/now ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_fault_num(value):
    # fs 的 a/z/v 与 fx 的 timeout/now ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_key(value):
    # key 为 UTF-8 可编码的非空字符串；JSON 可能解码出孤立代理项。
    if not isinstance(value, str) or value == "":
        fail(EXIT_INPUT, "INPUT")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        fail(EXIT_INPUT, "INPUT")
    return value


def encode_backend_id(value):
    """chash/route 建环时遇到的 id 也必须 UTF-8 可编码。"""
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError:
        fail(EXIT_INPUT, "INPUT")


def circuit_closed(record):
    """未配置熔断器或熔断状态为 C 的后端才参与调度。"""
    circuit = record["circuit"]
    return circuit is None or circuit["state"] == "C"


def drain_available(record):
    """仅 A（可用）态后端参与 pick/open 与新的 route 映射。"""
    return record["drain"]["state"] == "A"


def parse_drain_timeout(value):
    # 排空时限 t ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def build_ring(backends, vnodes):
    """按 (摘要, 加入顺序, i) 升序返回 healthy、熔断闭合且 A 态后端的令牌环。"""
    tokens = []
    for join_index, (backend_id, record) in enumerate(backends.items()):
        if (
            not record["healthy"]
            or not circuit_closed(record)
            or not drain_available(record)
        ):
            continue
        encoded = encode_backend_id(backend_id)
        for i in range(vnodes):
            digest = hashlib.sha256(
                encoded + b"\x00" + str(i).encode("ascii")
            ).digest()
            tokens.append((int.from_bytes(digest, "big"), join_index, i, backend_id))
    tokens.sort(key=lambda token: (token[0], token[1], token[2]))
    return tokens


def parse_flow(value):
    """校验 [源IP,源端口,目的IP,目的端口,协议]，返回规范化五元组。"""
    if not isinstance(value, list) or len(value) != 5:
        fail(EXIT_INPUT, "INPUT")
    src_ip, src_port, dst_ip, dst_port, protocol = value
    for ip in (src_ip, dst_ip):
        if not isinstance(ip, str) or ip == "":
            fail(EXIT_INPUT, "INPUT")
    for port in (src_port, dst_port):
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            fail(EXIT_INPUT, "INPUT")
    if protocol not in ("tcp", "udp"):
        fail(EXIT_INPUT, "INPUT")
    return [src_ip, src_port, dst_ip, dst_port, protocol]


def parse_config(value):
    """校验 ci 的 config 并返回规范化结构；结构、类型、范围、编码或重复后端/
    限流项一律 INPUT。B 限流对后端的引用在执行期判 BACKEND。"""
    if not isinstance(value, dict) or set(value) != {
        "version", "backends", "vnodes", "limits", "overload",
    }:
        fail(EXIT_INPUT, "INPUT")
    version = value["version"]
    if not isinstance(version, int) or isinstance(version, bool) or version != 1:
        fail(EXIT_INPUT, "INPUT")

    raw_backends = value["backends"]
    if not isinstance(raw_backends, list):
        fail(EXIT_INPUT, "INPUT")
    normalized_backends = []
    seen_backend_ids = set()
    for item in raw_backends:
        if not isinstance(item, dict) or set(item) != {
            "id", "weight", "d", "fail", "success", "circuit", "drain",
        }:
            fail(EXIT_INPUT, "INPUT")
        backend_id = parse_backend_id(item["id"])
        # 后端 id 须 UTF-8 可编码（建环与输出排序都会用到其字节）。
        encode_backend_id(backend_id)
        if backend_id in seen_backend_ids:
            fail(EXIT_INPUT, "INPUT")
        seen_backend_ids.add(backend_id)
        weight = parse_threshold(item["weight"])
        d = parse_duration(item["d"])
        fail_threshold = parse_threshold(item["fail"])
        success_threshold = parse_threshold(item["success"])
        raw_circuit = item["circuit"]
        if raw_circuit is None:
            circuit_params = None
        else:
            if not isinstance(raw_circuit, dict) or set(raw_circuit) != {
                "n", "m", "r", "w", "q",
            }:
                fail(EXIT_INPUT, "INPUT")
            n = parse_threshold(raw_circuit["n"])
            r = parse_threshold(raw_circuit["r"])
            q = parse_threshold(raw_circuit["q"])
            m = raw_circuit["m"]
            # m ∈ [1,n]，与 cs 同范围；bool 显式排除。
            if not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= n:
                fail(EXIT_INPUT, "INPUT")
            w = parse_circuit_window(raw_circuit["w"])
            circuit_params = (n, m, r, w, q)
        raw_drain = item["drain"]
        drain_t = None if raw_drain is None else parse_drain_timeout(raw_drain)
        normalized_backends.append(
            (
                backend_id,
                weight,
                d,
                fail_threshold,
                success_threshold,
                circuit_params,
                drain_t,
            )
        )

    vnodes = None if value["vnodes"] is None else parse_vnodes(value["vnodes"])

    raw_limits = value["limits"]
    if not isinstance(raw_limits, list):
        fail(EXIT_INPUT, "INPUT")
    normalized_limits = []
    seen_buckets = set()
    scope_rank = {"B": 0, "C": 1, "S": 2}
    previous_order_key = None
    for item in raw_limits:
        if not isinstance(item, dict) or set(item) != {"scope", "id", "r", "b"}:
            fail(EXIT_INPUT, "INPUT")
        scope = item["scope"]
        if scope not in scope_rank:
            fail(EXIT_INPUT, "INPUT")
        bucket_id = parse_key(item["id"])
        r = parse_rate(item["r"])
        b = parse_burst(item["b"])
        pair = (scope, bucket_id)
        if pair in seen_buckets:
            fail(EXIT_INPUT, "INPUT")
        seen_buckets.add(pair)
        order_key = (scope_rank[scope], bucket_id.encode("utf-8"))
        if previous_order_key is not None and not previous_order_key < order_key:
            # 必须严格按 scope 的 B/C/S 序、id 的 UTF-8 字节升序排列。
            fail(EXIT_INPUT, "INPUT")
        previous_order_key = order_key
        normalized_limits.append((scope, bucket_id, r, b))

    raw_overload = value["overload"]
    if raw_overload is None:
        overload = None
    else:
        if not isinstance(raw_overload, dict) or set(raw_overload) != {
            "cap", "q", "ttl",
        }:
            fail(EXIT_INPUT, "INPUT")
        overload = (
            parse_queue_param(raw_overload["cap"]),
            parse_queue_param(raw_overload["q"]),
            parse_queue_param(raw_overload["ttl"]),
        )

    return {
        "backends": normalized_backends,
        "vnodes": vnodes,
        "limits": normalized_limits,
        "overload": overload,
    }


def parse_op(raw_op):
    """校验单个操作的形状，返回规范化元组；不合格式直接 INPUT 退出。"""
    if not isinstance(raw_op, dict):
        fail(EXIT_INPUT, "INPUT")

    name = raw_op.get("op")
    if name not in (
        "add", "remove", "pick", "open", "close", "get",
        "hset", "probe", "hget", "chash", "route", "ws", "wg",
        "cs", "cr", "cg", "ds", "dr", "du", "dg",
        "ls", "la", "lg",
        "os", "oa", "ot", "og",
        "mr", "mg",
        "ce", "ci",
        "fs", "fx",
    ):
        fail(EXIT_INPUT, "INPUT")

    keys = set(raw_op)
    if name == "add":
        if keys == {"op", "id", "weight"}:
            # 三键旧形式：d=0，立即取目标权重，now 不参与时钟。
            return (
                "add",
                parse_backend_id(raw_op["id"]),
                parse_threshold(raw_op["weight"]),
                0,
                None,
            )
        if keys == {"op", "id", "weight", "d", "now"}:
            return (
                "add",
                parse_backend_id(raw_op["id"]),
                parse_threshold(raw_op["weight"]),
                parse_duration(raw_op["d"]),
                parse_warm_now(raw_op["now"]),
            )
        fail(EXIT_INPUT, "INPUT")

    if name == "remove":
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        return ("remove", parse_backend_id(raw_op["id"]))

    if name == "pick":
        if keys != {"op"}:
            fail(EXIT_INPUT, "INPUT")
        return ("pick",)

    if name == "open":
        if keys != {"op", "cid", "flow", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "open",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_now(raw_op["now"]),
        )

    if name == "close":
        if keys != {"op", "cid", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("close", parse_cid(raw_op["cid"]), parse_now(raw_op["now"]))

    if name == "hset":
        if keys != {"op", "id", "fail", "success"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "hset",
            parse_backend_id(raw_op["id"]),
            parse_threshold(raw_op["fail"]),
            parse_threshold(raw_op["success"]),
        )

    if name == "probe":
        if keys != {"op", "id", "ok", "now"}:
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        # ok 只接受真正的 bool。
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return (
            "probe",
            parse_backend_id(raw_op["id"]),
            ok,
            parse_now(raw_op["now"]),
        )

    if name == "hget":
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        return ("hget", parse_backend_id(raw_op["id"]))

    if name == "ws":
        if keys != {"op", "id", "weight", "d", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "ws",
            parse_backend_id(raw_op["id"]),
            parse_threshold(raw_op["weight"]),
            parse_duration(raw_op["d"]),
            parse_warm_now(raw_op["now"]),
        )

    if name == "wg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("wg", parse_backend_id(raw_op["id"]), parse_warm_now(raw_op["now"]))

    if name == "chash":
        if keys != {"op", "vnodes"}:
            fail(EXIT_INPUT, "INPUT")
        return ("chash", parse_vnodes(raw_op["vnodes"]))

    if name == "route":
        if keys != {"op", "key"}:
            fail(EXIT_INPUT, "INPUT")
        return ("route", parse_key(raw_op["key"]))

    if name == "cs":
        if keys != {"op", "id", "n", "m", "r", "w", "q"}:
            fail(EXIT_INPUT, "INPUT")
        n = parse_threshold(raw_op["n"])
        r = parse_threshold(raw_op["r"])
        q = parse_threshold(raw_op["q"])
        m = raw_op["m"]
        # m ∈ [1, n]，须先校验 n；bool 是 int 的子类，必须显式排除。
        if not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= n:
            fail(EXIT_INPUT, "INPUT")
        return (
            "cs",
            parse_backend_id(raw_op["id"]),
            n,
            m,
            r,
            parse_circuit_window(raw_op["w"]),
            q,
        )

    if name == "cr":
        if keys != {"op", "id", "ok", "now"}:
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        # ok 只接受真正的 bool。
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return (
            "cr",
            parse_backend_id(raw_op["id"]),
            ok,
            parse_now(raw_op["now"]),
        )

    if name == "cg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("cg", parse_backend_id(raw_op["id"]), parse_now(raw_op["now"]))

    if name == "ds":
        if keys != {"op", "id", "t"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "ds",
            parse_backend_id(raw_op["id"]),
            parse_drain_timeout(raw_op["t"]),
        )

    if name in ("dr", "du", "dg"):
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (name, parse_backend_id(raw_op["id"]), parse_now(raw_op["now"]))

    if name == "ls":
        if keys != {"op", "scope", "id", "r", "b", "now"}:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "ls",
            scope,
            parse_key(raw_op["id"]),
            parse_rate(raw_op["r"]),
            parse_burst(raw_op["b"]),
            parse_now(raw_op["now"]),
        )

    if name == "la":
        if keys != {"op", "c", "s", "key", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "la",
            parse_key(raw_op["c"]),
            parse_key(raw_op["s"]),
            parse_key(raw_op["key"]),
            parse_now(raw_op["now"]),
        )

    if name == "lg":
        if keys != {"op", "scope", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "lg",
            scope,
            parse_key(raw_op["id"]),
            parse_now(raw_op["now"]),
        )

    if name == "os":
        if keys != {"op", "cap", "q", "ttl"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "os",
            parse_queue_param(raw_op["cap"]),
            parse_queue_param(raw_op["q"]),
            parse_queue_param(raw_op["ttl"]),
        )

    if name == "oa":
        if keys != {"op", "cid", "flow", "c", "s", "key", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "oa",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_key(raw_op["c"]),
            parse_key(raw_op["s"]),
            parse_key(raw_op["key"]),
            parse_now(raw_op["now"]),
        )

    if name == "ot":
        if keys != {"op", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("ot", parse_now(raw_op["now"]))

    if name == "og":
        if keys != {"op"}:
            fail(EXIT_INPUT, "INPUT")
        return ("og",)

    if name == "mr":
        if keys != {"op", "id", "ok", "ms", "retries", "remaps", "now"}:
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        # ok 只接受真正的 bool。
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return (
            "mr",
            parse_backend_id(raw_op["id"]),
            ok,
            parse_metric_num(raw_op["ms"]),
            parse_metric_num(raw_op["retries"]),
            parse_metric_num(raw_op["remaps"]),
            parse_metric_num(raw_op["now"]),
        )

    if name == "mg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "mg",
            parse_backend_id(raw_op["id"]),
            parse_metric_num(raw_op["now"]),
        )

    if name == "fs":
        if keys != {"op", "id", "k", "a", "z", "v"}:
            fail(EXIT_INPUT, "INPUT")
        k = raw_op["k"]
        if k not in ("D", "F", "S"):
            fail(EXIT_INPUT, "INPUT")
        a = parse_fault_num(raw_op["a"])
        z = parse_fault_num(raw_op["z"])
        v = parse_fault_num(raw_op["v"])
        if not a < z:
            fail(EXIT_INPUT, "INPUT")
        # D（不可用）须 v=0；F（抖动）/S（慢）须 v>0。
        if k == "D":
            if v != 0:
                fail(EXIT_INPUT, "INPUT")
        elif v == 0:
            fail(EXIT_INPUT, "INPUT")
        return ("fs", parse_backend_id(raw_op["id"]), k, a, z, v)

    if name == "fx":
        if keys != {"op", "cid", "flow", "key", "timeout", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "fx",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_key(raw_op["key"]),
            parse_fault_num(raw_op["timeout"]),
            parse_fault_num(raw_op["now"]),
        )

    if name in ("ce", "ci"):
        if name == "ce":
            if keys != {"op"}:
                fail(EXIT_INPUT, "INPUT")
            return ("ce",)
        if keys != {"op", "config", "now"}:
            fail(EXIT_INPUT, "INPUT")
        # now 为非负非 bool 整数，时钟倒退在执行期与其余操作同序判定。
        parse_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("ci", config, raw_op["now"])

    # get
    if keys != {"op", "cid"}:
        fail(EXIT_INPUT, "INPUT")
    return ("get", parse_cid(raw_op["cid"]))


def effective_weight(record, now):
    """返回 now 时刻的有效权重（百分制整数）；unhealthy 固定 0。

    warm 段 [start, end) 内自 warm_from 线性过渡到 target*100，插值向下
    取整到 0.01；end 起取目标。新增与恢复的 warm_from 为 100（1.00），
    调权时 warm_from 为调权当时的有效权重（百分制）。
    """
    if not record["healthy"]:
        return 0
    # pick 不携带 now，取最近时钟值；此时只可能存在 steady 后端
    # （任何 warm 后端都来自携带 now 的 add/ws/恢复 probe）。
    if now is None or record["stage"] != "warm" or now >= record["warm_end"]:
        return record["weight"] * 100
    target = record["weight"] * 100
    span = record["warm_end"] - record["warm_start"]
    elapsed = now - record["warm_start"]
    return record["warm_from"] + (target - record["warm_from"]) * elapsed // span


def run(raw):
    """在全新状态执行一批操作，返回 stdout 字节；错误经 fail 抛出。"""
    try:
        text = raw.decode("utf-8")
        data = json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, ValueError):
        fail(EXIT_INPUT, "INPUT")

    if not isinstance(data, dict) or set(data) != {"ops"}:
        fail(EXIT_INPUT, "INPUT")
    ops = data["ops"]
    if not isinstance(ops, list):
        fail(EXIT_INPUT, "INPUT")

    # dict 保序即加入顺序；每个后端记录：
    # weight 目标权重（整数），current 平滑加权累加值（百分制），conns
    # 活动连接数，healthy 健康状态，fail/success 迁移阈值，failures/
    # successes 当前连续计数，probe_now/probe_ok 最近一次生效的 probe
    # （用于幂等重报判定）。预热：stage ∈ warm/steady，warm_from 为段内
    # 起始有效权重（百分制，新增/恢复为 100，调权为当时有效值），
    # warm_start/warm_end 为预热区间，warm_d 为登记的 d（恢复时复用）。
    # last_op 记录最近一次写操作的形状用于同参/冲突重报判定。circuit 为
    # 熔断器状态（未配为 None，remove 后重加即回到未配）：params 为
    # (n, m, r, w, q)，state ∈ C/O/H，window 为 C 态最近 n 次报告的
    # deque，next 为 O 态恢复时刻，used 为 H 态已报告数，cr_now/cr_ok
    # 为最近一次生效的 cr（用于幂等重报判定）。drain 为优雅摘除状态：
    # t 为 ds 登记的排空时限（未 ds 为 None，remove 后重加即回到未配），
    # state ∈ A/D/X（可用/排空/已摘除），start/end/deadline 为本次摘除
    # 的三个时刻（未开始为 None），forced 为 dg 强关的连接数。
    backends = {}
    # 活动连接：cid -> [backend_id, flow, opened_at]；关闭即删除，cid 可复用。
    connections = {}
    # 一致性哈希：ring_vnodes 未 chash 时为 None；sticky 映射 key -> backend_id，
    # 只在目标不可用时依环改写（不迁回），增删后端或改 vnodes 均不动它。
    ring_vnodes = None
    sticky_map = {}
    # 令牌桶以 (scope, id) 唯一：scope ∈ B/C/S（后端/客户端/服务类）。
    # B 桶 id 必须是现存后端；remove 即删。每桶 r/b 为速率与容量，t/at
    # 为当前令牌与最近补充时刻，last 为最近一次 ls 的 (r,b,now) 用于重报。
    buckets = {}
    # 排队接纳：queue_cfg 未 os 时为 None，否则为 (cap, q, ttl)；wait_queue
    # 为 FIFO deque，元素 (cid, flow, c, s, key, enqueue_now)，容量上限 q。
    queue_cfg = None
    wait_queue = deque()
    last_now = None
    results = []

    def select_route(key, fatal=True):
        """按原 route 语义选后端：首见建立粘性映射，目标不可用依环迁移且不
        迁回；返回 (backend_id, sticky, remapped)，未配环或无可选后端报 STATE。
        fatal=False 时不退出而返回 None（供排队重试把该情形视为阻塞）。"""
        if ring_vnodes is None:
            if fatal:
                fail(EXIT_STATE, "STATE")
            return None
        mapped = sticky_map.get(key)
        if mapped is not None:
            record = backends.get(mapped)
            if (
                record is not None
                and record["healthy"]
                and circuit_closed(record)
                and record["drain"]["state"] != "X"
            ):
                return mapped, True, False
        tokens = build_ring(backends, ring_vnodes)
        if not tokens:
            if fatal:
                fail(EXIT_STATE, "STATE")
            return None
        digests = [token[0] for token in tokens]
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0  # 越界回绕到环首
        chosen_id = tokens[index][3]
        remapped = mapped is not None
        sticky_map[key] = chosen_id
        return chosen_id, False, remapped

    def refill(bucket, now):
        # 先按时间差补充至容量上限，再推进时钟。
        bucket["t"] = min(
            bucket["b"], bucket["t"] + (now - bucket["at"]) * bucket["r"]
        )
        bucket["at"] = now

    def evaluate_admit(backend_id, cid, flow, c, s, now):
        """对已路由的后端按 la 规则补充检查但不消费；令牌不足、目标非 A 或
        连接数达 cap 时返回 ("block", id)，全部满足才耗令牌、建连接
        （opened_at=now），返回 ("admit", id)。"""
        chosen = []
        for scope, bucket_id in (("B", backend_id), ("C", c), ("S", s)):
            bucket = buckets.get((scope, bucket_id))
            if bucket is not None:
                chosen.append(bucket)
        for bucket in chosen:
            refill(bucket, now)
        record = backends[backend_id]
        if (
            not all(bucket["t"] >= 1 for bucket in chosen)
            or record["drain"]["state"] != "A"
            or record["conns"] >= queue_cfg[0]
        ):
            return "block", backend_id
        # 接纳才耗令牌并按 open 建连接。
        for bucket in chosen:
            bucket["t"] -= 1
        record["conns"] += 1
        connections[cid] = [backend_id, flow, now]
        return "admit", backend_id

    def try_admit(cid, flow, c, s, key, now):
        """按 oa/ot 规则尝试一次接纳：先路由再评估。路由不可用（未配环或无
        可选后端）返回 ("route", None)——oa 据此报 STATE，ot 视为队首阻塞即
        停；其余返回 evaluate_admit 的结果。"""
        routed = select_route(key, fatal=False)
        if routed is None:
            return "route", None
        backend_id, _, _ = routed
        return evaluate_admit(backend_id, cid, flow, c, s, now)

    def record_metric(backend_id, ok, ms, retries, remaps, now):
        """按 mr 语义累加一条度量：window=now//60，换窗清零，五延迟桶
        [≤1,≤10,≤100,≤1000,>1000]，各计数封顶 10^18。mr 与 fx 完成后
        的自动度量按操作顺序共用此入口。"""
        record = backends[backend_id]
        window = now // 60
        metrics = record["metrics"]
        if metrics is None or metrics[0] != window:
            # 首次报告或换窗：统计全部清零，只保留当前窗。
            metrics = [window, 0, 0, 0, 0, [0, 0, 0, 0, 0]]
            record["metrics"] = metrics
        metrics[1] = min(METRIC_CAP, metrics[1] + 1)
        if not ok:
            metrics[2] = min(METRIC_CAP, metrics[2] + 1)
        metrics[3] = min(METRIC_CAP, metrics[3] + retries)
        metrics[4] = min(METRIC_CAP, metrics[4] + remaps)
        # 上界 [1,10,100,1000]：桶依次为 ≤1、≤10、≤100、≤1000、>1000。
        bucket = bisect.bisect_left((1, 10, 100, 1000), ms)
        metrics[5][bucket] = min(METRIC_CAP, metrics[5][bucket] + 1)

    def fault_active(record, now):
        """mg 的 removed=fault 判定：fs 登记且 now ∈ [a,z) 窗口内时，D 恒为
        故障，F 仅 ((now-a)//v)%2=0 相位为故障；S（仅变慢）与非故障相位
        均不算故障。"""
        fault = record["fault"]
        if fault is None or not fault[1] <= now < fault[2]:
            return False
        k, a, _, v = fault
        if k == "D":
            return True
        if k == "F":
            return ((now - a) // v) % 2 == 0
        return False

    for raw_op in ops:
        op = parse_op(raw_op)

        if op[0] in (
            "open", "close", "probe", "add", "ws", "wg", "cr", "cg",
            "dr", "du", "dg", "ls", "la", "lg", "oa", "ot", "mr", "mg",
            "ci", "fx",
        ):
            now = op[-1]
            # 三键 add 的 now 占位为 None，不参与时钟。
            if now is not None:
                if last_now is not None and now < last_now:
                    fail(EXIT_INPUT, "INPUT")
                last_now = now

        if op[0] == "add":
            _, backend_id, weight, d, now = op
            if backend_id in backends:
                if now is None:
                    # 三键旧形式：旧行为不变，重复 add 一律 BACKEND。
                    fail(EXIT_BACKEND, "BACKEND")
                # 五键：同参重报幂等；任何 add5/ws 占用同一 now 而异参，
                # 报 INPUT；其余重复 add 报 BACKEND。
                prev = backends[backend_id]["last_op"]
                if prev == ("add5", weight, d, now):
                    results.append({"op": "add", "ok": True})
                    continue
                if (
                    prev is not None
                    and prev[0] in ("add5", "ws")
                    and prev[3] == now
                ):
                    fail(EXIT_INPUT, "INPUT")
                fail(EXIT_BACKEND, "BACKEND")
            if d == 0:
                stage = "steady"
                warm_start = warm_end = warm_from = None
            else:
                stage = "warm"
                warm_from = 100
                warm_start = now
                warm_end = now + d
            backends[backend_id] = {
                "weight": weight,
                "current": 0,
                "conns": 0,
                "healthy": True,
                "fail": DEFAULT_FAIL,
                "success": DEFAULT_SUCCESS,
                "failures": 0,
                "successes": 0,
                "probe_now": None,
                "probe_ok": None,
                "stage": stage,
                "warm_d": d,
                "warm_from": warm_from,
                "warm_start": warm_start,
                "warm_end": warm_end,
                "circuit": None,
                "drain": {
                    "t": None,
                    "state": "A",
                    "start": None,
                    "end": None,
                    "deadline": None,
                    "forced": 0,
                },
                "last_op": ("add3", weight) if now is None else ("add5", weight, d, now),
                # 故障演练：fs 登记的 (k, a, z, v)，未登记为 None；remove/ci 清除。
                "fault": None,
                # 度量：None 表示从未 mr；否则 (window, requests, errors,
                # retries, remaps, [五个延迟桶])，仅保留当前 60 秒窗。
                "metrics": None,
            }
            results.append({"op": "add", "ok": True})

        elif op[0] == "remove":
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if record["conns"] > 0:
                fail(EXIT_STATE, "STATE")
            del backends[backend_id]
            buckets.pop(("B", backend_id), None)
            results.append({"op": "remove", "ok": True})

        elif op[0] == "pick":
            # 只在 healthy 且熔断闭合的可用（A）池内平滑加权：以最近时钟时刻
            # 的当前有效权重（百分制整数）累加，累加与总权重扣减都忽略不健
            # 康、熔断非 C 或排空中/已摘除的后端。
            chosen_id = None
            chosen_current = None
            healthy_total = 0
            for backend_id, record in backends.items():
                if (
                    not record["healthy"]
                    or not circuit_closed(record)
                    or not drain_available(record)
                ):
                    continue
                weight = effective_weight(record, last_now)
                record["current"] += weight
                healthy_total += weight
                # 严格大于：并列时保留遍历到的最早者。
                if chosen_current is None or record["current"] > chosen_current:
                    chosen_current = record["current"]
                    chosen_id = backend_id
            if chosen_id is None:
                fail(EXIT_STATE, "STATE")
            backends[chosen_id]["current"] -= healthy_total
            results.append({"op": "pick", "id": chosen_id})

        elif op[0] == "open":
            _, cid, flow, now = op
            chosen_id = None
            chosen_key = None
            for backend_id, record in backends.items():
                if (
                    not record["healthy"]
                    or not circuit_closed(record)
                    or not drain_available(record)
                ):
                    continue
                # 连接数升序、有效权重降序、加入顺序（dict 遍历序）。
                key = (record["conns"], -effective_weight(record, now))
                if chosen_key is None or key < chosen_key:
                    chosen_key = key
                    chosen_id = backend_id
            if chosen_id is None:
                fail(EXIT_STATE, "STATE")
            if cid in connections:
                fail(EXIT_CONNECTION, "CONNECTION")
            backends[chosen_id]["conns"] += 1
            connections[cid] = [chosen_id, flow, now]
            results.append({"op": "open", "cid": cid, "backend": chosen_id})

        elif op[0] == "close":
            _, cid, now = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            record = backends[connection[0]]
            record["conns"] -= 1
            del connections[cid]
            drain = record["drain"]
            if drain["state"] == "D" and record["conns"] == 0:
                # 排空中最后连接关闭即转 X，end 取本次 close 的 now。
                drain["state"] = "X"
                drain["end"] = now
            results.append({"op": "close", "ok": True})

        elif op[0] == "hset":
            _, backend_id, fail_threshold, success_threshold = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            # 只换阈值并清连续计数，不改健康状态。
            record["fail"] = fail_threshold
            record["success"] = success_threshold
            record["failures"] = 0
            record["successes"] = 0
            results.append({"op": "hset", "ok": True})

        elif op[0] == "probe":
            _, backend_id, ok, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if record["probe_now"] == now:
                # 同一 (id, now) 已生效过：ok 相同则幂等，不同即冲突重报。
                if record["probe_ok"] != ok:
                    fail(EXIT_INPUT, "INPUT")
            else:
                if ok:
                    record["successes"] = min(
                        record["successes"] + 1, record["success"]
                    )
                    record["failures"] = 0
                    if not record["healthy"] and record["successes"] >= record["success"]:
                        record["healthy"] = True
                        record["successes"] = 0
                        record["failures"] = 0
                        record["current"] = 0
                        # 恢复：以该 probe 的 now 从 1.00 按登记的 d 重启预热；
                        # d=0 时立即回到目标权重。
                        if record["warm_d"] > 0:
                            record["stage"] = "warm"
                            record["warm_from"] = 100
                            record["warm_start"] = now
                            record["warm_end"] = now + record["warm_d"]
                        else:
                            record["stage"] = "steady"
                else:
                    record["failures"] = min(
                        record["failures"] + 1, record["fail"]
                    )
                    record["successes"] = 0
                    if record["healthy"] and record["failures"] >= record["fail"]:
                        record["healthy"] = False
                        record["successes"] = 0
                        record["failures"] = 0
                        record["current"] = 0
                record["probe_now"] = now
                record["probe_ok"] = ok
            results.append({"op": "probe", "ok": True})

        elif op[0] == "hget":
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            results.append(
                {
                    "op": "hget",
                    "id": backend_id,
                    "state": "healthy" if record["healthy"] else "unhealthy",
                    "successes": record["successes"],
                    "failures": record["failures"],
                    "fail": record["fail"],
                    "success": record["success"],
                }
            )

        elif op[0] == "ws":
            _, backend_id, weight, d, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            prev = record["last_op"]
            if prev == ("ws", weight, d, now):
                # 同参重报幂等，不重复过渡、不清 current。
                results.append({"op": "ws", "ok": True})
                continue
            if (
                prev is not None
                and prev[0] in ("add5", "ws")
                and prev[3] == now
            ):
                # 同 (id, now) 异参（或异类操作）冲突，优先于 unhealthy 判定。
                fail(EXIT_INPUT, "INPUT")
            if not record["healthy"]:
                fail(EXIT_STATE, "STATE")
            # 自当前有效权重线性过渡；必须在改 weight 前按旧调度取有效值。
            start_weight = effective_weight(record, now)
            record["weight"] = weight
            record["current"] = 0  # 调权清零平滑 current。
            record["warm_d"] = d
            if d == 0:
                record["stage"] = "steady"
                record["warm_from"] = None
                record["warm_start"] = None
                record["warm_end"] = None
            else:
                record["stage"] = "warm"
                record["warm_from"] = start_weight
                record["warm_start"] = now
                record["warm_end"] = now + d
            record["last_op"] = ("ws", weight, d, now)
            results.append({"op": "ws", "ok": True})

        elif op[0] == "wg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if not record["healthy"]:
                stage = "unhealthy"
                start = end = None
            elif record["stage"] == "warm" and now < record["warm_end"]:
                stage = "warm"
                start = record["warm_start"]
                end = record["warm_end"]
            else:
                stage = "steady"
                start = end = None
            cents = effective_weight(record, now) if stage != "unhealthy" else 0
            effective = "%d.%02d" % (cents // 100, cents % 100)
            results.append(
                {
                    "op": "wg",
                    "id": backend_id,
                    "target": record["weight"],
                    "effective": effective,
                    "stage": stage,
                    "start": start,
                    "end": end,
                }
            )

        elif op[0] == "chash":
            _, vnodes = op
            # 同值幂等、异值生效；建环会遇到的 healthy id 必须可编码。
            for record_id, record in backends.items():
                if record["healthy"]:
                    encode_backend_id(record_id)
            ring_vnodes = vnodes
            results.append({"op": "chash", "ok": True})

        elif op[0] == "route":
            _, key = op
            chosen_id, sticky, remapped = select_route(key)
            results.append(
                {
                    "op": "route",
                    "key": key,
                    "backend": chosen_id,
                    "sticky": sticky,
                    "remapped": remapped,
                }
            )

        elif op[0] == "cs":
            _, backend_id, n, m, r, w, q = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            params = (n, m, r, w, q)
            circuit = record["circuit"]
            if circuit is not None and circuit["params"] == params:
                # 同参重报幂等，不重置状态与窗口。
                results.append({"op": "cs", "ok": True})
                continue
            # 首配或异参重配：置 C 并清空窗。
            record["circuit"] = {
                "params": params,
                "state": "C",
                "window": deque(maxlen=n),
                "next": None,
                "used": 0,
                "cr_now": None,
                "cr_ok": None,
            }
            results.append({"op": "cs", "ok": True})

        elif op[0] == "cr":
            _, backend_id, ok, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            circuit = record["circuit"]
            if circuit is None:
                fail(EXIT_STATE, "STATE")
            if circuit["cr_now"] == now:
                # 同一 (id, now) 已生效过：ok 相同则幂等，不同即冲突重报。
                if circuit["cr_ok"] != ok:
                    fail(EXIT_INPUT, "INPUT")
                results.append({"op": "cr", "ok": True})
                continue
            n, m, r, w, q = circuit["params"]
            if circuit["state"] == "O":
                if now < circuit["next"]:
                    # 熔断恢复窗口未到期，拒绝报告。
                    fail(EXIT_STATE, "STATE")
                # 到期先转 H，本条按 H 处理。
                circuit["state"] = "H"
                circuit["next"] = None
                circuit["used"] = 0
            if circuit["state"] == "C":
                window = circuit["window"]
                window.append(ok)
                samples = len(window)
                failures = sum(1 for value in window if not value)
                if samples >= m and failures * 100 >= r * samples:
                    circuit["state"] = "O"
                    circuit["next"] = now + w
                    circuit["used"] = 0
            else:  # H
                if ok:
                    circuit["used"] += 1
                    if circuit["used"] >= q:
                        # 连续 q 次成功转 C 并清窗。
                        circuit["state"] = "C"
                        circuit["window"].clear()
                        circuit["used"] = 0
                else:
                    # H 中失败立即重开并重算恢复时刻。
                    circuit["state"] = "O"
                    circuit["next"] = now + w
                    circuit["used"] = 0
            circuit["cr_now"] = now
            circuit["cr_ok"] = ok
            results.append({"op": "cr", "ok": True})

        elif op[0] == "cg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            circuit = record["circuit"]
            if circuit is None:
                fail(EXIT_STATE, "STATE")
            if circuit["state"] == "O" and now >= circuit["next"]:
                # 恢复窗口到期，触发转 H。
                circuit["state"] = "H"
                circuit["next"] = None
                circuit["used"] = 0
            state = circuit["state"]
            window = circuit["window"]
            results.append(
                {
                    "op": "cg",
                    "id": backend_id,
                    "state": state,
                    "count": len(window),
                    "fail": sum(1 for value in window if not value),
                    "reason": None if state == "C" else "rate",
                    "next": circuit["next"] if state == "O" else None,
                    "used": circuit["used"] if state == "H" else 0,
                }
            )

        elif op[0] == "ds":
            _, backend_id, t = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] == t:
                # 同值幂等，任意状态下都不改配置。
                results.append({"op": "ds", "ok": True})
                continue
            if drain["state"] == "D":
                # 排空中改值报 STATE。
                fail(EXIT_STATE, "STATE")
            drain["t"] = t
            results.append({"op": "ds", "ok": True})

        elif op[0] == "dr":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] is None:
                fail(EXIT_STATE, "STATE")
            if drain["state"] == "A":
                # 开启新一轮摘除：start/deadline/forced 复位，end 待迁移时填。
                drain["start"] = now
                drain["deadline"] = now + drain["t"]
                drain["forced"] = 0
                drain["end"] = None
                if record["conns"] > 0:
                    drain["state"] = "D"
                else:
                    drain["state"] = "X"
                    drain["end"] = now
            # D/X 再 dr 幂等，不改状态。
            results.append({"op": "dr", "ok": True})

        elif op[0] == "du":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] is None:
                fail(EXIT_STATE, "STATE")
            if drain["state"] == "D":
                # 取消排空记 end=now；已强关的连接不恢复，慢启动不重置。
                drain["end"] = now
            drain["state"] = "A"
            results.append({"op": "du", "ok": True})

        elif op[0] == "dg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] is None:
                fail(EXIT_STATE, "STATE")
            if drain["state"] == "D" and now >= drain["deadline"]:
                # 到期强关该后端全部连接（O(C)），转 X，end=deadline。
                forced = 0
                for cid, connection in list(connections.items()):
                    if connection[0] == backend_id:
                        del connections[cid]
                        forced += 1
                record["conns"] = 0
                drain["forced"] = forced
                drain["state"] = "X"
                drain["end"] = drain["deadline"]
            results.append(
                {
                    "op": "dg",
                    "id": backend_id,
                    "state": drain["state"],
                    "connections": record["conns"],
                    "start": drain["start"],
                    "end": drain["end"],
                    "deadline": drain["deadline"],
                    "forced": drain["forced"],
                }
            )

        elif op[0] == "ls":
            _, scope, bucket_id, r, b, now = op
            # B 桶挂在现存后端上；未知后端优先于其它检查报 BACKEND。
            if scope == "B" and bucket_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            key_pair = (scope, bucket_id)
            bucket = buckets.get(key_pair)
            if bucket is None:
                # 新桶以满令牌起步。
                buckets[key_pair] = {
                    "r": r,
                    "b": b,
                    "t": b,
                    "at": now,
                    "last": (r, b, now),
                }
            else:
                if bucket["last"] == (r, b, now):
                    # 同 (r, b, now) 重报幂等，不补令牌、不推进时钟。
                    results.append({"op": "ls", "ok": True})
                    continue
                # 其余一律按重配置处理：t=b、at=now。
                bucket["r"] = r
                bucket["b"] = b
                bucket["t"] = b
                bucket["at"] = now
                bucket["last"] = (r, b, now)
            results.append({"op": "ls", "ok": True})

        elif op[0] == "la":
            _, c, s, key, now = op
            # 先按原 route 选后端（未配环或无可选后端报 STATE），再检查
            # 该后端/客户端/服务类三个桶；未配置即不限制。
            backend_id, _, _ = select_route(key)
            chosen = []
            for scope, bucket_id in (("B", backend_id), ("C", c), ("S", s)):
                bucket = buckets.get((scope, bucket_id))
                if bucket is not None:
                    chosen.append(bucket)
            for bucket in chosen:
                refill(bucket, now)
            # 均有 t>=1 才各减 1；任一不足则 RATE/6：无 stdout、整批原子。
            if not all(bucket["t"] >= 1 for bucket in chosen):
                fail(EXIT_RATE, "RATE")
            for bucket in chosen:
                bucket["t"] -= 1
            results.append({"op": "la", "backend": backend_id, "ok": True})

        elif op[0] == "lg":
            _, scope, bucket_id, now = op
            bucket = buckets.get((scope, bucket_id))
            if bucket is None:
                # 查询未配置桶报 STATE。
                fail(EXIT_STATE, "STATE")
            refill(bucket, now)
            results.append(
                {
                    "op": "lg",
                    "scope": scope,
                    "id": bucket_id,
                    "r": bucket["r"],
                    "b": bucket["b"],
                    "t": bucket["t"],
                    "at": bucket["at"],
                }
            )

        elif op[0] == "os":
            _, cap, q, ttl = op
            params = (cap, q, ttl)
            if queue_cfg is not None and queue_cfg != params:
                # 异参重配报 STATE（已排队项保留不动）。
                fail(EXIT_STATE, "STATE")
            # 首配或同参重报：同参幂等。
            queue_cfg = params
            results.append({"op": "os", "ok": True})

        elif op[0] == "oa":
            _, cid, flow, c, s, key, now = op
            if queue_cfg is None:
                # 未 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            # 路由检查先于 cid 重复判定，与 open 的 STATE 先于 CONNECTION 一致。
            routed = select_route(key, fatal=False)
            if routed is None:
                # 未 chash 或无可选后端。
                fail(EXIT_STATE, "STATE")
            if cid in connections or any(item[0] == cid for item in wait_queue):
                # 活动或排队中 cid 重复。
                fail(EXIT_CONNECTION, "CONNECTION")
            status, backend_id = evaluate_admit(
                routed[0], cid, flow, c, s, now
            )
            if status == "admit":
                results.append(
                    {"op": "oa", "cid": cid, "state": "A", "backend": backend_id}
                )
            else:
                if len(wait_queue) >= queue_cfg[1]:
                    # FIFO 已满，尾拒绝。
                    fail(EXIT_OVERLOAD, "OVERLOAD")
                wait_queue.append((cid, flow, c, s, key, now))
                results.append(
                    {"op": "oa", "cid": cid, "state": "Q", "backend": None}
                )

        elif op[0] == "ot":
            _, now = op
            if queue_cfg is None:
                fail(EXIT_STATE, "STATE")
            ttl = queue_cfg[2]
            # 先删除全部 now >= 入队 now + ttl 的项（保留 FIFO 相对顺序）。
            survivors = deque()
            expired = []
            for item in wait_queue:
                if now >= item[5] + ttl:
                    expired.append(item[0])
                else:
                    survivors.append(item)
            wait_queue.clear()
            wait_queue.extend(survivors)
            # 再自队首重试接纳，至首个阻塞即停（每个键至多一次 route）。
            admitted = []
            while wait_queue:
                item = wait_queue.popleft()
                # 接纳时刻为本次 ot 的 now（opened_at=now），入队时刻仅用于过期。
                status, _ = try_admit(
                    item[0], item[1], item[2], item[3], item[4], now
                )
                if status == "admit":
                    admitted.append(item[0])
                else:
                    # 阻塞（含路由不可用）：连同该项整体放回队首后停止。
                    wait_queue.appendleft(item)
                    break
            results.append(
                {"op": "ot", "expired": expired, "admitted": admitted}
            )

        elif op[0] == "og":
            if queue_cfg is None:
                fail(EXIT_STATE, "STATE")
            results.append(
                {"op": "og", "queue": [item[0] for item in wait_queue]}
            )

        elif op[0] == "mr":
            _, backend_id, ok, ms, retries, remaps, now = op
            if backend_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            record_metric(backend_id, ok, ms, retries, remaps, now)
            results.append({"op": "mr", "ok": True})

        elif op[0] == "mg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            window = now // 60
            metrics = record["metrics"]
            if metrics is None or metrics[0] != window:
                # 查询新窗（含从未 mr）按零计，不写回存储。
                requests = errors = retries = remaps = 0
                latency = [0, 0, 0, 0, 0]
            else:
                requests = metrics[1]
                errors = metrics[2]
                retries = metrics[3]
                remaps = metrics[4]
                latency = metrics[5]
            # qps=requests/60、error_rate=100*errors/requests 均下截两位。
            qps = "%d.%02d" % divmod(requests * 100 // 60, 100)
            if requests == 0:
                error_rate = "0.00"
            else:
                rate = errors * 10000 // requests
                error_rate = "%d.%02d" % divmod(rate, 100)
            # removed 优先级：drain（D/X）> health（unhealthy）> circuit
            # （非 C）> fault（D 窗口内或 F 故障相位），否则 null。
            drain_state = record["drain"]["state"]
            if drain_state in ("D", "X"):
                removed = "drain"
            elif not record["healthy"]:
                removed = "health"
            elif not circuit_closed(record):
                removed = "circuit"
            elif fault_active(record, now):
                removed = "fault"
            else:
                removed = None
            results.append(
                {
                    "op": "mg",
                    "id": backend_id,
                    "window": window,
                    "requests": requests,
                    "qps": qps,
                    "concurrency": record["conns"],
                    "errors": errors,
                    "error_rate": error_rate,
                    "latency": list(latency),
                    "retries": retries,
                    "remaps": remaps,
                    "removed": removed,
                }
            )

        elif op[0] == "ce":
            # 导出纯配置（登记值），不含任何运行态。
            exported_backends = []
            for backend_id, record in backends.items():
                circuit = record["circuit"]
                exported_backends.append(
                    {
                        "id": backend_id,
                        "weight": record["weight"],
                        "d": record["warm_d"],
                        "fail": record["fail"],
                        "success": record["success"],
                        "circuit": (
                            None
                            if circuit is None
                            else {
                                "n": circuit["params"][0],
                                "m": circuit["params"][1],
                                "r": circuit["params"][2],
                                "w": circuit["params"][3],
                                "q": circuit["params"][4],
                            }
                        ),
                        "drain": record["drain"]["t"],
                    }
                )
            exported_limits = [
                {"scope": scope, "id": bucket_id, "r": bucket["r"], "b": bucket["b"]}
                # 仅遍历不消费：按 (scope 秩, id UTF-8 字节) 升序输出。
                for (scope, bucket_id), bucket in sorted(
                    buckets.items(),
                    key=lambda item: (
                        {"B": 0, "C": 1, "S": 2}[item[0][0]],
                        item[0][1].encode("utf-8"),
                    ),
                )
            ]
            exported_overload = (
                None
                if queue_cfg is None
                else {"cap": queue_cfg[0], "q": queue_cfg[1], "ttl": queue_cfg[2]}
            )
            results.append(
                {
                    "op": "ce",
                    "config": {
                        "version": 1,
                        "backends": exported_backends,
                        "vnodes": ring_vnodes,
                        "limits": exported_limits,
                        "overload": exported_overload,
                    },
                }
            )

        elif op[0] == "ci":
            _, config, now = op
            config_backends = config["backends"]
            config_limits = config["limits"]
            # B 限流引用未知后端：BACKEND，先于活动状态判定。
            config_backend_ids = {entry[0] for entry in config_backends}
            for scope, bucket_id, _, _ in config_limits:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 有活动连接或排队项时拒绝热加载：STATE。
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")

            # 校验全部通过，原子替换配置并以 now 重建默认运行态。
            def make_record(weight, d, fail_threshold, success_threshold,
                            circuit_params, drain_t):
                if d == 0:
                    stage = "steady"
                    warm_from = warm_start = warm_end = None
                else:
                    # 预热自本次 now 起算。
                    stage = "warm"
                    warm_from = 100
                    warm_start = now
                    warm_end = now + d
                return {
                    "weight": weight,
                    "current": 0,
                    "conns": 0,
                    "healthy": True,
                    "fail": fail_threshold,
                    "success": success_threshold,
                    "failures": 0,
                    "successes": 0,
                    "probe_now": None,
                    "probe_ok": None,
                    "stage": stage,
                    "warm_d": d,
                    "warm_from": warm_from,
                    "warm_start": warm_start,
                    "warm_end": warm_end,
                    "circuit": (
                        None
                        if circuit_params is None
                        else {
                            "params": circuit_params,
                            "state": "C",
                            "window": deque(maxlen=circuit_params[0]),
                            "next": None,
                            "used": 0,
                            "cr_now": None,
                            "cr_ok": None,
                        }
                    ),
                    "drain": {
                        "t": drain_t,
                        "state": "A",
                        "start": None,
                        "end": None,
                        "deadline": None,
                        "forced": 0,
                    },
                    # 热加载不携带历史写操作形状。
                    "last_op": None,
                    "metrics": None,
                    # 热加载以默认运行态重建，不携带故障演练。
                    "fault": None,
                }

            new_backends = {}
            for (backend_id, weight, d, fail_threshold,
                 success_threshold, circuit_params, drain_t) in config_backends:
                new_backends[backend_id] = make_record(
                    weight, d, fail_threshold, success_threshold,
                    circuit_params, drain_t,
                )
            # 新桶满令牌起步，at=now。
            new_buckets = {}
            for scope, bucket_id, r, b in config_limits:
                new_buckets[(scope, bucket_id)] = {
                    "r": r,
                    "b": b,
                    "t": b,
                    "at": now,
                    "last": None,
                }
            backends = new_backends
            buckets = new_buckets
            ring_vnodes = config["vnodes"]
            queue_cfg = config["overload"]
            wait_queue = deque()
            sticky_map = {}
            results.append({"op": "ci", "ok": True})

        elif op[0] == "fs":
            _, backend_id, k, a, z, v = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            # 同参重报幂等（不改登记），异参覆盖；均返回 ok。
            record["fault"] = (k, a, z, v)
            results.append({"op": "fs", "ok": True})

        elif op[0] == "fx":
            _, cid, flow, key, timeout, now = op
            if ring_vnodes is None:
                # 未 chash 报 STATE，先于 cid 重复判定（与 oa 一致）。
                fail(EXIT_STATE, "STATE")
            if cid in connections:
                fail(EXIT_CONNECTION, "CONNECTION")
            # 环同 route：仅健康、熔断 C、排空 A 后端；不读写粘性映射。
            tokens = build_ring(backends, ring_vnodes)
            remaps = 0
            state = "R"
            chosen_id = None
            latency = 0
            first_id = None  # 环遍历的首个候选后端，backend=null 时归属于它
            if tokens:
                digests = [token[0] for token in tokens]
                key_hash = int.from_bytes(
                    hashlib.sha256(key.encode("utf-8")).digest(), "big"
                )
                index = bisect.bisect_left(digests, key_hash)
                if index == len(tokens):
                    index = 0  # 越界回绕到环首
                seen = set()
                for offset in range(len(tokens)):
                    backend_id = tokens[(index + offset) % len(tokens)][3]
                    if backend_id in seen:
                        continue
                    seen.add(backend_id)
                    if first_id is None:
                        first_id = backend_id
                    fault = backends[backend_id]["fault"]
                    cost = 0
                    unavailable = False
                    if fault is not None and fault[1] <= now < fault[2]:
                        k, a, _, v = fault
                        if k == "D":
                            unavailable = True
                        elif k == "F":
                            # 抖动：((now-a)//v)%2=0 的相位不可用。
                            unavailable = ((now - a) // v) % 2 == 0
                        else:  # S：可用但耗时 v。
                            cost = v
                    if unavailable:
                        # 跳过 D/F 计一次 remap；环外后端不计。
                        remaps += 1
                        continue
                    # 首个可用后端即终止遍历：耗时超限则不建连。
                    chosen_id = backend_id
                    latency = cost
                    if cost <= timeout:
                        state = "A"
                    break
            if state == "A":
                # 按 open 建连（opened_at=now）。
                backends[chosen_id]["conns"] += 1
                connections[cid] = [chosen_id, flow, now]
            results.append(
                {
                    "op": "fx",
                    "cid": cid,
                    "state": state,
                    "backend": chosen_id,
                    "latency": latency,
                    "remaps": remaps,
                }
            )
            # fx 完成后只追加一次等价 mr 度量，不新增结果项：归属取结果
            # backend，为 null 时取环遍历首个后端，环内无候选不记。
            metric_id = chosen_id if chosen_id is not None else first_id
            if metric_id is not None:
                record_metric(
                    metric_id, state == "A", latency, remaps, remaps, now
                )

        else:  # get
            _, cid = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            results.append(
                {
                    "op": "get",
                    "cid": cid,
                    "flow": connection[1],
                    "backend": connection[0],
                    "opened_at": connection[2],
                }
            )

    output = {
        "results": results,
        "backends": [
            {"id": backend_id, "weight": record["weight"]}
            for backend_id, record in backends.items()
        ],
    }
    encoded = json.dumps(output, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return encoded + b"\n"


def execute(raw):
    """在全新状态执行 raw，返回 (退出码, stdout 字节, stderr 字节)。"""
    try:
        return 0, run(raw), b""
    except _Failure as error:
        return (
            error.exit_code,
            b"",
            ('{"error":"%s"}\n' % error.label).encode("utf-8"),
        )


def build_record(raw):
    """把原始 stdin 字节作为全新 run 输入执行，返回记录行（紧凑 JSON + 换行）。

    键序 version,stdin,exit,stdout,stderr；三个字节字段为带标准填充的
    RFC4648 Base64。底层成功或按既有错误失败均产出记录。
    """
    exit_code, out, err = execute(raw)
    record = {
        "version": 1,
        "stdin": base64.b64encode(raw).decode("ascii"),
        "exit": exit_code,
        "stdout": base64.b64encode(out).decode("ascii"),
        "stderr": base64.b64encode(err).decode("ascii"),
    }
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    return line.encode("utf-8") + b"\n"


def decode_base64_field(value):
    """校验并解码带标准填充的规范 Base64；类型、解码或规范性不符判 INPUT。"""
    if not isinstance(value, str):
        fail(EXIT_INPUT, "INPUT")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        fail(EXIT_INPUT, "INPUT")
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except ValueError:
        fail(EXIT_INPUT, "INPUT")
    # 重编码必须逐字节一致：拒绝非零填充位、缺/多填充等非规范形式。
    if base64.b64encode(decoded) != encoded:
        fail(EXIT_INPUT, "INPUT")
    return decoded


def replay(raw):
    """校验记录并在全新状态重执解码的 stdin，逐字节比对退出码与两路输出。

    一致时返回 (记录退出码, 记录 stdout, 记录 stderr)；记录非法判 INPUT，
    比对不符判 REPLAY/8。
    """
    try:
        text = raw.decode("utf-8")
        data = json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, ValueError):
        fail(EXIT_INPUT, "INPUT")
    if not isinstance(data, dict) or set(data) != {
        "version", "stdin", "exit", "stdout", "stderr",
    }:
        fail(EXIT_INPUT, "INPUT")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) or version != 1:
        fail(EXIT_INPUT, "INPUT")
    exit_code = data["exit"]
    if (
        not isinstance(exit_code, int)
        or isinstance(exit_code, bool)
        or exit_code not in (0, 2, 3, 4, 5, 6, 7)
    ):
        fail(EXIT_INPUT, "INPUT")
    stdin_bytes = decode_base64_field(data["stdin"])
    stdout_bytes = decode_base64_field(data["stdout"])
    stderr_bytes = decode_base64_field(data["stderr"])
    actual_code, actual_out, actual_err = execute(stdin_bytes)
    if (
        actual_code != exit_code
        or actual_out != stdout_bytes
        or actual_err != stderr_bytes
    ):
        fail(EXIT_REPLAY, "REPLAY")
    return exit_code, stdout_bytes, stderr_bytes


def main(argv):
    try:
        if len(argv) != 2 or argv[1] not in ("run", "record", "replay"):
            fail(EXIT_INPUT, "INPUT")
        raw = sys.stdin.buffer.read()
        if argv[1] == "run":
            exit_code, out, err = execute(raw)
        elif argv[1] == "record":
            # 底层成败均退出 0、stderr 为空，stdout 输出记录。
            exit_code, out, err = 0, build_record(raw), b""
        else:
            exit_code, out, err = replay(raw)
    except _Failure as error:
        exit_code = error.exit_code
        out = b""
        err = ('{"error":"%s"}\n' % error.label).encode("utf-8")
    sys.stdout.buffer.write(out)
    sys.stderr.buffer.write(err)
    sys.exit(exit_code)


if __name__ == "__main__":
    main(sys.argv)
