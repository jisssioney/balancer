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
"""

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


def parse_threshold(value):
    # bool 是 int 的子类，必须显式排除。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 100
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


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
        "hset", "probe", "hget",
    ):
        fail(EXIT_INPUT, "INPUT")

    keys = set(raw_op)
    if name == "add":
        if keys != {"op", "id", "weight"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "add",
            parse_backend_id(raw_op["id"]),
            parse_threshold(raw_op["weight"]),
        )

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
    # weight/current 平滑加权，conns 活动连接数，healthy 健康状态，
    # fail/success 迁移阈值，failures/successes 当前连续计数，
    # probe_now/probe_ok 最近一次生效的 probe（用于幂等重报判定）。
    backends = {}
    # 活动连接：cid -> [backend_id, flow, opened_at]；关闭即删除，cid 可复用。
    connections = {}
    last_now = None
    results = []

    for raw_op in ops:
        op = parse_op(raw_op)

        if op[0] in ("open", "close", "probe"):
            now = op[-1]
            if last_now is not None and now < last_now:
                fail(EXIT_INPUT, "INPUT")
            last_now = now

        if op[0] == "add":
            _, backend_id, weight = op
            if backend_id in backends:
                fail(EXIT_BACKEND, "BACKEND")
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
            chosen_id = None
            chosen_current = None
            healthy_total = 0
            for backend_id, record in backends.items():
                if not record["healthy"]:
                    continue
                record["current"] += record["weight"]
                healthy_total += record["weight"]
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
            for backend_id, record in backends.items():
                if not record["healthy"]:
                    continue
                # 严格小于：并列时保留遍历到的最早（最早加入）者。
                if chosen_conns is None or record["conns"] < chosen_conns:
                    chosen_conns = record["conns"]
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
