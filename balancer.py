#!/usr/bin/env python3
"""平滑加权轮询（smooth weighted round-robin）后端池，仅用标准库。

入口：python balancer.py run
stdin 为 UTF-8 JSON：{"ops": [...]}，成功时 stdout 输出
{"results":[...],"backends":[...]}（无多余空白，末尾恰好一个换行）。
任何错误都不产生 stdout，只向 stderr 写一行 {"error":"..."} 并以约定码退出。

连接管理：open/close/get 按 cid 跟踪活动连接；open 选活动连接数最少的
后端（并列取最早加入者），remove 只允许删除无连接的后端。open/close 的
now 依序不得递减。

健康探测：后端新增时为 healthy，阈值 fail=3、success=2，连续计数为 0。
hset 更换阈值并清计数（不改状态）；probe 按 ok 累计连续计数（封顶阈值）、
清零另一项，healthy 失败达 fail 转 unhealthy，unhealthy 成功达 success 转
healthy，迁移时计数与后端 current 清零。同 id,now,ok 重报幂等，同 id,now
异 ok 报 INPUT；probe 的 now 与 open/close 共用非递减时钟。pick/open 只
在 healthy 池中进行，累加与扣减总权重均忽略 unhealthy。
"""

import json
import sys

EXIT_INPUT = 2
EXIT_BACKEND = 3
EXIT_STATE = 4
EXIT_CONNECTION = 5


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


def parse_now(value):
    # bool 是 int 的子类，必须显式排除。
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
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
        backend_id = raw_op["id"]
        weight = raw_op["weight"]
        if not isinstance(backend_id, str) or backend_id == "":
            fail(EXIT_INPUT, "INPUT")
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(weight, int)
            or isinstance(weight, bool)
            or not 1 <= weight <= 100
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("add", backend_id, weight)

    if name == "remove":
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        backend_id = raw_op["id"]
        if not isinstance(backend_id, str) or backend_id == "":
            fail(EXIT_INPUT, "INPUT")
        return ("remove", backend_id)

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
        backend_id = raw_op["id"]
        if not isinstance(backend_id, str) or backend_id == "":
            fail(EXIT_INPUT, "INPUT")
        new_fail = raw_op["fail"]
        new_success = raw_op["success"]
        for threshold in (new_fail, new_success):
            # bool 是 int 的子类，必须显式排除。
            if (
                not isinstance(threshold, int)
                or isinstance(threshold, bool)
                or not 1 <= threshold <= 100
            ):
                fail(EXIT_INPUT, "INPUT")
        return ("hset", backend_id, new_fail, new_success)

    if name == "probe":
        if keys != {"op", "id", "ok", "now"}:
            fail(EXIT_INPUT, "INPUT")
        backend_id = raw_op["id"]
        if not isinstance(backend_id, str) or backend_id == "":
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return ("probe", backend_id, ok, parse_now(raw_op["now"]))

    if name == "hget":
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        backend_id = raw_op["id"]
        if not isinstance(backend_id, str) or backend_id == "":
            fail(EXIT_INPUT, "INPUT")
        return ("hget", backend_id)

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

    # dict 保序即加入顺序；删除后重加自然落到末尾。记录为
    # [weight, current, conns, healthy, fail, success, failures, successes, last_probe]，
    # last_probe 为 (now, ok) 或 None；total_weight 与 healthy_count 只统计 healthy 后端。
    backends = {}
    total_weight = 0
    healthy_count = 0
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
            backends[backend_id] = [weight, 0, 0, True, 3, 2, 0, 0, None]
            total_weight += weight
            healthy_count += 1
            results.append({"op": "add", "ok": True})

        elif op[0] == "remove":
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if record[2] > 0:
                fail(EXIT_STATE, "STATE")
            if record[3]:
                total_weight -= record[0]
                healthy_count -= 1
            del backends[backend_id]
            results.append({"op": "remove", "ok": True})

        elif op[0] == "pick":
            if healthy_count == 0:
                fail(EXIT_STATE, "STATE")
            chosen_id = None
            chosen_current = None
            for backend_id, record in backends.items():
                if not record[3]:
                    continue
                record[1] += record[0]
                # 严格大于：并列时保留遍历到的最早者。
                if chosen_current is None or record[1] > chosen_current:
                    chosen_current = record[1]
                    chosen_id = backend_id
            backends[chosen_id][1] -= total_weight
            results.append({"op": "pick", "id": chosen_id})

        elif op[0] == "open":
            _, cid, flow, now = op
            if healthy_count == 0:
                fail(EXIT_STATE, "STATE")
            if cid in connections:
                fail(EXIT_CONNECTION, "CONNECTION")
            chosen_id = None
            chosen_conns = None
            for backend_id, record in backends.items():
                if not record[3]:
                    continue
                # 严格小于：并列时保留遍历到的最早（最早加入）者。
                if chosen_conns is None or record[2] < chosen_conns:
                    chosen_conns = record[2]
                    chosen_id = backend_id
            backends[chosen_id][2] += 1
            connections[cid] = [chosen_id, flow, now]
            results.append({"op": "open", "cid": cid, "backend": chosen_id})

        elif op[0] == "close":
            _, cid, _now = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            backends[connection[0]][2] -= 1
            del connections[cid]
            results.append({"op": "close", "ok": True})

        elif op[0] == "hset":
            _, backend_id, new_fail, new_success = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            # 只换阈值并清连续计数，不改健康状态与 current。
            record[4] = new_fail
            record[5] = new_success
            record[6] = 0
            record[7] = 0
            results.append({"op": "hset", "ok": True})

        elif op[0] == "probe":
            _, backend_id, ok, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            last_probe = record[8]
            if last_probe is not None and last_probe[0] == now:
                if last_probe[1] != ok:
                    fail(EXIT_INPUT, "INPUT")
                # 同 id,now,ok 幂等重报：不改变任何状态。
            else:
                record[8] = (now, ok)
                if ok:
                    record[7] = min(record[7] + 1, record[5])
                    record[6] = 0
                    if not record[3] and record[7] >= record[5]:
                        record[3] = True
                        record[1] = 0
                        record[6] = 0
                        record[7] = 0
                        total_weight += record[0]
                        healthy_count += 1
                else:
                    record[6] = min(record[6] + 1, record[4])
                    record[7] = 0
                    if record[3] and record[6] >= record[4]:
                        record[3] = False
                        record[1] = 0
                        record[6] = 0
                        record[7] = 0
                        total_weight -= record[0]
                        healthy_count -= 1
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
                    "state": "healthy" if record[3] else "unhealthy",
                    "successes": record[7],
                    "failures": record[6],
                    "fail": record[4],
                    "success": record[5],
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
            {"id": backend_id, "weight": record[0]}
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
