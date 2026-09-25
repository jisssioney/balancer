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

权重预热与调权：add 可带 d,now（三键形式视为 d=0，旧行为不变）；weight 为
目标值，d=0 立即生效，否则从 1.00 起在 [now, now+d] 内线性爬升，插值向下
取整到 0.01，到期后恒取目标。ws（op,id,weight,d,now）自当前有效值线性过渡
到新目标并清零平滑当前值；同 (id,now) 同参重报幂等、异参报 INPUT，unhealthy
时报 STATE。恢复（unhealthy 转 healthy）以该 probe 的 now 从 1.00 按 d 重启
预热。wg（op,id,now）返回 target、effective（两位小数字符串，unhealthy 固定
0.00）、stage（warm/steady/unhealthy）、start/end（仅 warm 为整数，否则
null）。pick 以当前有效权重执行平滑加权；open 按连接数升序、有效权重降序、
加入顺序选取；chash/route 忽略权重。add/ws/wg 的 now 与 open/close/probe 共
用同一非递减时钟。

一致性哈希：chash 配置每个 healthy 后端的虚拟节点数 vnodes（1..1024），
同值幂等、异值生效。每个 healthy 后端为 i=0..vnodes-1 生成令牌
SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，摘要按 256 位大端无符号数
排序（并列按加入顺序、i 升序）构成哈希环。route 哈希 UTF8(key) 取首个
不小于它的令牌，越界回绕；首见 key 建立映射，目标 healthy 时命中
（sticky），增删后端或改 vnodes 不迁移，目标不可用时依环迁移且不迁回
（remapped）。route 不改连接数；未 chash 或无 healthy 后端时报 STATE。
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

MAX_RAMP_PARAM = 10**9


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


def parse_threshold(value):
    # bool 是 int 的子类，必须显式排除。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 100
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


def parse_ramp_param(value):
    # add/ws/wg 的 d 与 now：非 bool 整数，0..10^9。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= MAX_RAMP_PARAM
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
    """按 (摘要, 加入顺序, i) 升序返回 healthy 后端的令牌环。"""
    tokens = []
    for join_index, (backend_id, record) in enumerate(backends.items()):
        if not record["healthy"]:
            continue
        encoded = encode_backend_id(backend_id)
        for i in range(vnodes):
            digest = hashlib.sha256(
                encoded + b"\x00" + str(i).encode("ascii")
            ).digest()
            tokens.append((int.from_bytes(digest, "big"), join_index, i, backend_id))
    tokens.sort(key=lambda token: (token[0], token[1], token[2]))
    return tokens


def effective_weight_h(record, now):
    """now 时刻的有效权重，以 0.01 为单位的非负整数（unhealthy 恒为 0）。

    预热/调权在 [ramp_start, ramp_start+d] 内从 ramp_from_h 线性过渡到
    目标，插值向下取整到 0.01；到期后恒为目标。ramp_start 为 None 表示
    稳态（d=0 立即生效）。
    """
    if not record["healthy"]:
        return 0
    target_h = record["weight"] * 100
    start = record["ramp_start"]
    if start is None:
        return target_h
    span = record["d"]
    if span == 0 or now >= start + span:
        return target_h
    from_h = record["ramp_from_h"]
    if now <= start:
        return from_h
    # 纯整数运算实现向下取整：Python 的 // 对负分子同样向 -inf 取整。
    return (from_h * span + (target_h - from_h) * (now - start)) // span


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
    ):
        fail(EXIT_INPUT, "INPUT")

    keys = set(raw_op)
    if name == "add":
        if keys == {"op", "id", "weight"}:
            # 三键形式视为 d=0：立即生效，无预热。
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
                parse_ramp_param(raw_op["d"]),
                parse_ramp_param(raw_op["now"]),
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

    if name == "chash":
        if keys != {"op", "vnodes"}:
            fail(EXIT_INPUT, "INPUT")
        return ("chash", parse_vnodes(raw_op["vnodes"]))

    if name == "route":
        if keys != {"op", "key"}:
            fail(EXIT_INPUT, "INPUT")
        return ("route", parse_key(raw_op["key"]))

    if name == "ws":
        if keys != {"op", "id", "weight", "d", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "ws",
            parse_backend_id(raw_op["id"]),
            parse_threshold(raw_op["weight"]),
            parse_ramp_param(raw_op["d"]),
            parse_ramp_param(raw_op["now"]),
        )

    if name == "wg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "wg",
            parse_backend_id(raw_op["id"]),
            parse_ramp_param(raw_op["now"]),
        )

    # get
    if keys != {"op", "cid"}:
        fail(EXIT_INPUT, "INPUT")
    return ("get", parse_cid(raw_op["cid"]))


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

    # dict 保序即加入顺序；删除后重加自然落到末尾。每个后端记录：
    # weight 目标权重，d 预热/调权时长，ramp_start/ramp_from_h 线性过渡的
    # 起点时刻与起始有效权重（0.01 单位，ramp_start 为 None 表示稳态），
    # current 平滑加权当前值，conns 活动连接数，healthy 健康状态，
    # fail/success 迁移阈值，failures/successes 当前连续计数，
    # probe_now/probe_ok 最近一次生效的 probe（用于幂等重报判定），
    # last_ws 最近一次生效的 ws（now, weight, d，用于幂等重报判定）。
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

        if op[0] in ("open", "close", "probe", "add", "ws", "wg"):
            now = op[-1]
            # 三键 add 无 now，不推进时钟。
            if now is not None:
                if last_now is not None and now < last_now:
                    fail(EXIT_INPUT, "INPUT")
                last_now = now

        if op[0] == "add":
            _, backend_id, weight, duration, now = op
            if backend_id in backends:
                fail(EXIT_BACKEND, "BACKEND")
            backends[backend_id] = {
                "weight": weight,
                "d": duration,
                # d>0 时从 1.00 起线性预热；d=0 立即生效（稳态）。
                "ramp_start": now if duration > 0 else None,
                "ramp_from_h": 100,
                "current": 0,
                "conns": 0,
                "healthy": True,
                "fail": DEFAULT_FAIL,
                "success": DEFAULT_SUCCESS,
                "failures": 0,
                "successes": 0,
                "probe_now": None,
                "probe_ok": None,
                "last_ws": None,
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
            # 只在 healthy 池内平滑加权：累加与总权重扣减都忽略 unhealthy。
            # 权重取当前有效值（0.01 单位）；d=0 时等价于旧权重整体放大
            # 100 倍，平滑加权对缩放不变，故旧行为逐字节保持。
            clock = last_now if last_now is not None else 0
            chosen_id = None
            chosen_current = None
            healthy_total = 0
            for backend_id, record in backends.items():
                if not record["healthy"]:
                    continue
                effective = effective_weight_h(record, clock)
                record["current"] += effective
                healthy_total += effective
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
            chosen_conns = None
            chosen_effective = None
            for backend_id, record in backends.items():
                if not record["healthy"]:
                    continue
                effective = effective_weight_h(record, now)
                # 连接数升序、有效权重降序；皆并列时保留最早加入者。
                if (
                    chosen_id is None
                    or record["conns"] < chosen_conns
                    or (
                        record["conns"] == chosen_conns
                        and effective > chosen_effective
                    )
                ):
                    chosen_id = backend_id
                    chosen_conns = record["conns"]
                    chosen_effective = effective
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
                        # 恢复即以该 probe 的 now 从 1.00 按 d 重启预热。
                        record["ramp_start"] = now
                        record["ramp_from_h"] = 100
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
            if ring_vnodes is None:
                fail(EXIT_STATE, "STATE")
            mapped = sticky_map.get(key)
            if mapped is not None:
                record = backends.get(mapped)
                if record is not None and record["healthy"]:
                    # 健康旧映射命中，无需动环。
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

        elif op[0] == "ws":
            _, backend_id, weight, duration, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            last_ws = record["last_ws"]
            if last_ws is not None and last_ws[0] == now:
                # 同一 (id, now) 已生效过：同参幂等，异参即冲突重报。
                if last_ws[1] != weight or last_ws[2] != duration:
                    fail(EXIT_INPUT, "INPUT")
            else:
                if not record["healthy"]:
                    fail(EXIT_STATE, "STATE")
                # 自当前有效值线性过渡到新目标；调权清零平滑当前值。
                # 必须先用旧状态求当前有效值，再写入新目标。
                current_h = effective_weight_h(record, now)
                record["weight"] = weight
                record["d"] = duration
                record["ramp_start"] = now if duration > 0 else None
                record["ramp_from_h"] = current_h
                record["current"] = 0
                record["last_ws"] = (now, weight, duration)
            results.append({"op": "ws", "ok": True})

        elif op[0] == "wg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if not record["healthy"]:
                stage = "unhealthy"
                start = None
                end = None
                effective = "0.00"
            else:
                start = record["ramp_start"]
                end = None
                if start is not None and record["d"] > 0 and now < start + record["d"]:
                    stage = "warm"
                    end = start + record["d"]
                else:
                    stage = "steady"
                    start = None
                effective_h = effective_weight_h(record, now)
                effective = "%d.%02d" % (effective_h // 100, effective_h % 100)
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
