#!/usr/bin/env python3
"""四层后端池与平滑加权轮询调度（仅标准库）。

入口：``python balancer.py run``

stdin 为单个 UTF-8 JSON 对象 ``{"ops": [...]}``，stdout 输出一行紧凑
JSON ``{"results": [...], "backends": [...]}``。任何错误都只写 stderr、
退出码非零，且整批操作原子生效。
"""

import json
import sys

_EXIT_INPUT = 2
_EXIT_BACKEND = 3
_EXIT_STATE = 4

_ERROR_LINE = {
    "INPUT": '{"error":"INPUT"}\n',
    "BACKEND": '{"error":"BACKEND"}\n',
    "STATE": '{"error":"STATE"}\n',
}

_OP_KEYS = {
    "add": frozenset(("op", "id", "weight")),
    "remove": frozenset(("op", "id")),
    "pick": frozenset(("op",)),
}


class InputError(ValueError):
    """stdin 不是合格式的批处理请求。"""


def _reject_constant(value):
    # 拒绝 NaN / Infinity（json 默认会接受这些非标准 JSON 常量）。
    raise InputError("constant not allowed: %s" % value)


def _no_duplicate_keys(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise InputError("duplicate key: %s" % key)
        document[key] = value
    return document


def parse_batch(raw):
    """解析并做全部结构校验；任何不合格式之处抛 InputError。"""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InputError("not utf-8") from exc
    try:
        document = json.loads(
            text,
            parse_constant=_reject_constant,
            object_pairs_hook=_no_duplicate_keys,
        )
    except ValueError as exc:
        raise InputError("not json") from exc

    if not isinstance(document, dict) or set(document) != {"ops"}:
        raise InputError("top level must be {ops: [...]}")
    ops = document["ops"]
    if not isinstance(ops, list):
        raise InputError("ops must be a list")

    for op in ops:
        if not isinstance(op, dict):
            raise InputError("op must be an object")
        name = op.get("op")
        if not isinstance(name, str) or name not in _OP_KEYS:
            raise InputError("unknown op")
        if set(op) != _OP_KEYS[name]:
            raise InputError("wrong keys for op %s" % name)
        if name in ("add", "remove"):
            backend_id = op["id"]
            if not isinstance(backend_id, str) or not backend_id:
                raise InputError("id must be a non-empty string")
        if name == "add":
            weight = op["weight"]
            # bool 是 int 的子类，必须先排除。
            if isinstance(weight, bool) or not isinstance(weight, int):
                raise InputError("weight must be an integer")
            if not 1 <= weight <= 100:
                raise InputError("weight out of range")
    return ops


def execute(ops):
    """在临时状态上执行整批操作。

    成功返回 (输出对象, None)；语义错误返回 (None, "BACKEND"|"STATE")。
    只有全部成功时调用方才会写 stdout，保证整批原子。
    """
    # dict 即按加入顺序；remove 后重加自然排到末尾，add/remove 均摊 O(1)。
    weights = {}
    current = {}
    results = []

    for op in ops:
        name = op["op"]
        if name == "add":
            backend_id = op["id"]
            if backend_id in weights:
                return None, "BACKEND"
            weights[backend_id] = op["weight"]
            current[backend_id] = 0
            results.append({"op": "add", "ok": True})
        elif name == "remove":
            backend_id = op["id"]
            if backend_id not in weights:
                return None, "BACKEND"
            del weights[backend_id]
            del current[backend_id]
            results.append({"op": "remove", "ok": True})
        else:  # pick
            if not weights:
                return None, "STATE"
            total_weight = 0
            chosen = None
            best = None
            # 平滑加权轮询：各后端 current += weight，取最大者；
            # 按加入顺序严格比较，并列时保留最早者。
            for backend_id, weight in weights.items():
                value = current[backend_id] + weight
                current[backend_id] = value
                total_weight += weight
                if best is None or value > best:
                    best = value
                    chosen = backend_id
            current[chosen] -= total_weight
            results.append({"op": "pick", "id": chosen})

    backends = [
        {"id": backend_id, "weight": weights[backend_id]}
        for backend_id in weights
    ]
    return {"results": results, "backends": backends}, None


def main(argv):
    if len(argv) != 2 or argv[1] != "run":
        sys.stderr.write(_ERROR_LINE["INPUT"])
        return _EXIT_INPUT

    try:
        ops = parse_batch(sys.stdin.buffer.read())
    except InputError:
        sys.stderr.write(_ERROR_LINE["INPUT"])
        return _EXIT_INPUT

    output, error = execute(ops)
    if error is not None:
        sys.stderr.write(_ERROR_LINE[error])
        if error == "BACKEND":
            return _EXIT_BACKEND
        return _EXIT_STATE

    encoded = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.buffer.write(encoded.encode("utf-8") + b"\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
