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

一致性哈希：chash 配置每个 healthy 后端的虚拟节点数 vnodes（1..1024），
同值幂等、异值生效。每个 healthy 后端为 i=0..vnodes-1 生成令牌
SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，摘要按 256 位大端无符号数
排序（并列按加入顺序、i 升序）构成哈希环。route 哈希 UTF8(key) 取首个
不小于它的令牌，越界回绕；首见 key 建立映射，目标 healthy 时命中
（sticky），增删后端或改 vnodes 不迁移，目标不可用时依环迁移且不迁回
（remapped）。route 不改连接数；未 chash 或无 healthy 后端时报 STATE。

断路器（circuit breaker）：cs 键集 op,id,n,m,r,w,q 配置参数（n,q,r ∈
[1,100]、m ∈ [1,n]、w ∈ [1,10^9]，均为非 bool 整数）；首次配置与异参
重配以空窗进入 C，同参重报幂等，未配置时 cr/cg 报 STATE，删除后重新
加入的后端为未配置。cr 键集 op,id,ok,now（ok 仅 bool）上报样本：C 保留
最近 n 次，样本数 ≥ m 且 失败数×100 ≥ r×样本数 时转 O，next=now+w；
O 中 now<next 的上报报 STATE，到期先转 H 再处理该上报；H 中失败即重开
并重算 next，连续 q 次成功则转 C 并清空窗口。同一 (id,now,ok) 重报幂等，
同 (id,now) 异 ok 报 INPUT。cg 键集 op,id,now，到期转 H，返回键序
op,id,state,count,fail,reason,next,used：state ∈ {C,O,H}，count/fail/used
为非负整数，reason 非 C 为 rate、C 为 null，next 仅 O 为整数否则 null，
used 仅 H 为报告数否则 0。cs/cr 成功返回 {op,ok:true}。cr/cg 的 now 纳入
非递减时钟；pick/open/route 仅选 healthy 且未配置或处于 C 的后端，粘性
目标处于 O/H 时沿原环迁移且不迁回。
"""

import bisect
import hashlib
import json
import sys

EXIT_INPUT = 2
EXIT_BACKEND = 3
EXIT_STATE = 4
EXIT_CONNECTION = 5

DEFAULT_FAIL = 3
DEFAULT_SUCCESS = 2


def fail(exit_code, label):
    sys.stderr.buffer.write(
        ('{"error":"%s"}\n' % label).encode("utf-8")
    )
    sys.exit(exit_code)


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


def parse_duration(value):
    # 预热时长 d ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_cb_window(value):
    # 断路器等待时长 w ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_cb_m(value, n):
    # m ∈ [1, n]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= n
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


def build_ring(backends, vnodes):
    """按 (摘要, 加入顺序, i) 升序返回可选后端的令牌环。"""
    tokens = []
    for join_index, (backend_id, record) in enumerate(backends.items()):
        if not selectable(record):
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


def parse_op(raw_op):
    """校验单个操作的形状，返回规范化元组；不合格式直接 INPUT 退出。"""
    if not isinstance(raw_op, dict):
        fail(EXIT_INPUT, "INPUT")

    name = raw_op.get("op")
    if name not in (
        "add", "remove", "pick", "open", "close", "get",
        "hset", "probe", "hget", "chash", "route", "ws", "wg",
        "cs", "cr", "cg",
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

    if name == "cs":
        if keys != {"op", "id", "n", "m", "r", "w", "q"}:
            fail(EXIT_INPUT, "INPUT")
        n = parse_threshold(raw_op["n"])
        return (
            "cs",
            parse_backend_id(raw_op["id"]),
            n,
            parse_cb_m(raw_op["m"], n),
            parse_threshold(raw_op["r"]),
            parse_cb_window(raw_op["w"]),
            parse_threshold(raw_op["q"]),
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

    if name == "chash":
        if keys != {"op", "vnodes"}:
            fail(EXIT_INPUT, "INPUT")
        return ("chash", parse_vnodes(raw_op["vnodes"]))

    if name == "route":
        if keys != {"op", "key"}:
            fail(EXIT_INPUT, "INPUT")
        return ("route", parse_key(raw_op["key"]))

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


def selectable(record):
    """pick/open/route 的可选条件：healthy 且断路器未配置或处于 C。"""
    return record["healthy"] and (
        record["cb"] is None or record["cb"]["state"] == "C"
    )


def run():
    raw = sys.stdin.buffer.read()
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
    # last_op 记录最近一次写操作的形状用于同参/冲突重报判定。
    backends = {}
    # 活动连接：cid -> [backend_id, flow, opened_at]；关闭即删除，cid 可复用。
    connections = {}
    # 一致性哈希：ring_vnodes 未 chash 时为 None；sticky 映射 key -> backend_id，
    # 只在目标不可用时依环改写（不迁回），增删后端或改 vnodes 均不动它。
    ring_vnodes = None
    sticky_map = {}
    last_now = None
    results = []

    for raw_op in ops:
        op = parse_op(raw_op)

        if op[0] in ("open", "close", "probe", "add", "ws", "wg", "cr", "cg"):
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
                "last_op": ("add3", weight) if now is None else ("add5", weight, d, now),
                # 断路器：cb 为 None 表示未配置；否则记录参数与运行态：
                # state ∈ C/O/H，window 为最近样本 (now, ok) 列表（C 保留
                # 最近 n 次），next 为 O/H 的等待到期时刻，used 为 H 中
                # 已处理的报告数，last 为最近一次生效的 (now, ok) 重报键。
                "cb": None,
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
            results.append({"op": "remove", "ok": True})

        elif op[0] == "pick":
            # 只在可选池（healthy 且断路器未配置或处于 C）内平滑加权：以最近
            # 时钟时刻的当前有效权重（百分制整数）累加，累加与总权重扣减
            # 都忽略不可选后端。
            chosen_id = None
            chosen_current = None
            healthy_total = 0
            for backend_id, record in backends.items():
                if not selectable(record):
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
                if not selectable(record):
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
            _, cid, _now = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            backends[connection[0]]["conns"] -= 1
            del connections[cid]
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

        elif op[0] == "cs":
            _, backend_id, n, m, r, w, q = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            params = {"n": n, "m": m, "r": r, "w": w, "q": q}
            if record["cb"] is not None and record["cb"]["params"] == params:
                # 同参重报幂等：状态、窗口、next 均不变。
                results.append({"op": "cs", "ok": True})
                continue
            # 首次配置或异参重配：以空窗进入 C。
            record["cb"] = {
                "params": params,
                "state": "C",
                "window": [],
                "next": None,
                "used": 0,
                "last": None,
            }
            results.append({"op": "cs", "ok": True})

        elif op[0] == "cr":
            _, backend_id, ok, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            cb = record["cb"]
            if cb is None:
                fail(EXIT_STATE, "STATE")
            if cb["last"] == (now, ok):
                # 同一 (id, now, ok) 重报幂等，不重复处理。
                results.append({"op": "cr", "ok": True})
                continue
            if cb["last"] is not None and cb["last"][0] == now:
                # 同 (id, now) 而异 ok：冲突重报。
                fail(EXIT_INPUT, "INPUT")
            state = cb["state"]
            if state == "O":
                if now < cb["next"]:
                    fail(EXIT_STATE, "STATE")
                # 到期：先转 H，再把本次报告作为首个观察样本（转 H 不计 used）。
                state = "H"
                cb["state"] = "H"
                cb["used"] = 0
            if state == "C":
                # 只保留最近 n 个样本；达标即按失败率转 O。
                cb["window"].append((now, ok))
                if len(cb["window"]) > cb["params"]["n"]:
                    del cb["window"][0]
                samples = len(cb["window"])
                failures = sum(
                    1 for _t, sample_ok in cb["window"] if not sample_ok
                )
                if (
                    samples >= cb["params"]["m"]
                    and failures * 100 >= cb["params"]["r"] * samples
                ):
                    cb["state"] = "O"
                    cb["next"] = now + cb["params"]["w"]
            else:  # H
                cb["used"] += 1
                if not ok:
                    # 观察期失败：立即重开（回 O）并重算 next，重新计时。
                    cb["state"] = "O"
                    cb["next"] = now + cb["params"]["w"]
                    cb["used"] = 0
                elif cb["used"] >= cb["params"]["q"]:
                    # 连续 q 次成功：转 C 并清空窗口。
                    cb["state"] = "C"
                    cb["window"] = []
                    cb["next"] = None
                    cb["used"] = 0
            cb["last"] = (now, ok)
            results.append({"op": "cr", "ok": True})

        elif op[0] == "cg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            cb = record["cb"]
            if cb is None:
                fail(EXIT_STATE, "STATE")
            if cb["state"] == "O" and now >= cb["next"]:
                # 到期触发：O -> H。
                cb["state"] = "H"
                cb["used"] = 0
            state = cb["state"]
            if state == "C":
                count = len(cb["window"])
                fail_count = sum(
                    1 for _t, sample_ok in cb["window"] if not sample_ok
                )
                reason = None
                next_value = None
                used = 0
            else:
                # O/H 期间窗口不再追加样本：count 为观察期已处理报告数，
                # 失败即重开不留失败计数，故 fail 恒为 0。
                count = cb["used"]
                fail_count = 0
                reason = "rate"
                next_value = cb["next"] if state == "O" else None
                used = cb["used"] if state == "H" else 0
            results.append(
                {
                    "op": "cg",
                    "id": backend_id,
                    "state": state,
                    "count": count,
                    "fail": fail_count,
                    "reason": reason,
                    "next": next_value,
                    "used": used,
                }
            )

        elif op[0] == "chash":
            _, vnodes = op
            # 同值幂等、异值生效；建环会遇到的可选 id 必须可编码。
            for record_id, record in backends.items():
                if selectable(record):
                    encode_backend_id(record_id)
            ring_vnodes = vnodes
            results.append({"op": "chash", "ok": True})

        elif op[0] == "route":
            _, key = op
            if ring_vnodes is None:
                fail(EXIT_STATE, "STATE")
            mapped = sticky_map.get(key)
            if mapped is not None:
                record = backends.get(mapped)
                if record is not None and selectable(record):
                    # 可选旧映射命中（healthy 且断路器处于 C），无需动环。
                    results.append(
                        {
                            "op": "route",
                            "key": key,
                            "backend": mapped,
                            "sticky": True,
                            "remapped": False,
                        }
                    )
                    continue
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                fail(EXIT_STATE, "STATE")
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
            results.append(
                {
                    "op": "route",
                    "key": key,
                    "backend": chosen_id,
                    "sticky": False,
                    "remapped": remapped,
                }
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
    sys.stdout.buffer.write(encoded + b"\n")


def main(argv):
    if len(argv) != 2 or argv[1] != "run":
        fail(EXIT_INPUT, "INPUT")
    run()


if __name__ == "__main__":
    main(sys.argv)
