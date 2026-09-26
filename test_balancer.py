"""balancer.py 的标准库黑盒回归测试（仅 stdlib，不联网）。

经子进程以 UTF-8 JSON 驱动 ``python balancer.py run``，覆盖 rh/ra 不可用
原因分钟历史与全池汇总的一批核心场景，并校验 record/replay 的逐字节复现。
发现方式：``python -m unittest discover``。
"""

import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BALANCER = ROOT / "balancer.py"

# 所有连接操作共用的 flow：["s",1,"t",2,"tcp"]。
FLOW = ["s", 1, "t", 2, "tcp"]


def encode_ops(ops):
    """把操作批序列化为紧凑 UTF-8 JSON 字节（子进程 stdin 载荷）。"""
    return json.dumps(
        {"ops": ops}, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def run_balancer(subcommand, payload):
    """以 payload 为 stdin 运行子命令，返回完成后的进程结果。"""
    return subprocess.run(
        [sys.executable, str(BALANCER), subcommand],
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(ROOT),
    )


def core_ops():
    """核心单批：h/d/c/o 四后端各触发一类不可用原因，全部 now=0。

    依次：add h/d/c/o（weight=1）；hset h 的 fail=success=1 并 probe false
    （h 转 unhealthy，记 health）；open x 使 d 持有连接，ds d 的 t=10 后
    dr（d 有连转 D，记 drain）；cs c 的 n=m=r=w=q=1 后 cr false（c 熔断
    打开，记 circuit）；chash vnodes=1、os cap=1,q=2,ttl=10，open y 使
    key "k" 的唯一可选后端 o 满载，再 oa z 入队（o 记 overload）。随后
    查询四个 rh 和两次 ra，from=to=0。
    """
    ops = [
        {"op": "add", "id": "h", "weight": 1},
        {"op": "add", "id": "d", "weight": 1},
        {"op": "add", "id": "c", "weight": 1},
        {"op": "add", "id": "o", "weight": 1},
        {"op": "hset", "id": "h", "fail": 1, "success": 1},
        {"op": "probe", "id": "h", "ok": False, "now": 0},
        {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
        {"op": "ds", "id": "d", "t": 10},
        {"op": "dr", "id": "d", "now": 0},
        {"op": "cs", "id": "c", "n": 1, "m": 1, "r": 1, "w": 1, "q": 1},
        {"op": "cr", "id": "c", "ok": False, "now": 0},
        {"op": "chash", "vnodes": 1},
        {"op": "os", "cap": 1, "q": 2, "ttl": 10},
        {"op": "open", "cid": "y", "flow": FLOW, "now": 0},
        {"op": "oa", "cid": "z", "flow": FLOW,
         "c": "k", "s": "k", "key": "k", "now": 0},
    ]
    for backend_id in ("h", "d", "c", "o"):
        ops.append(
            {"op": "rh", "id": backend_id, "from": 0, "to": 0, "now": 0}
        )
    ops.append({"op": "ra", "from": 0, "to": 0, "now": 0})
    ops.append({"op": "ra", "from": 0, "to": 0, "now": 0})
    return ops


class BalancerRunRegressionTests(unittest.TestCase):
    """``python balancer.py run`` 黑盒回归。"""

    def test_core_batch_exit_clean_and_rh_ra_counts(self):
        result = run_balancer("run", encode_ops(core_ops()))

        # 成功批：退出 0，stderr 必须为空。
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stderr, b"")

        data = json.loads(result.stdout.decode("utf-8"))
        results = data["results"]

        # 前置设置锁定：x 落在 d（dr 前 d 已持连），y 落在 o，z 因 o 满载
        # 而排队。
        opens = {r["cid"]: r["backend"] for r in results if r["op"] == "open"}
        self.assertEqual(opens, {"x": "d", "y": "o"})
        oa = [r for r in results if r["op"] == "oa"]
        self.assertEqual(
            oa, [{"op": "oa", "cid": "z", "state": "Q", "backend": None}]
        )

        # h/d/c/o 的 rh 分别仅 health/drain/circuit/overload 为 1，其余为 0。
        rh = {r["id"]: r for r in results if r["op"] == "rh"}
        self.assertEqual(set(rh), {"h", "d", "c", "o"})
        only_reason = {"h": "health", "d": "drain",
                       "c": "circuit", "o": "overload"}
        for backend_id, reason in only_reason.items():
            window = {
                "window": 0,
                "health": 1 if reason == "health" else 0,
                "drain": 1 if reason == "drain" else 0,
                "circuit": 1 if reason == "circuit" else 0,
                "overload": 1 if reason == "overload" else 0,
            }
            self.assertEqual(rh[backend_id]["windows"], [window])

        # 两次 ra：只读结果必须完全相同。
        ra = [r for r in results if r["op"] == "ra"]
        self.assertEqual(len(ra), 2)
        self.assertEqual(ra[0], ra[1])

        windows = ra[0]["windows"]
        self.assertEqual(len(windows), 1)
        window = windows[0]
        self.assertEqual(window["window"], 0)

        # backends 按加入序 h,d,c,o 列出全部现存后端。
        self.assertEqual(
            [entry["id"] for entry in window["backends"]],
            ["h", "d", "c", "o"],
        )
        by_id = {entry["id"]: entry for entry in window["backends"]}
        for backend_id, reason in only_reason.items():
            entry = by_id[backend_id]
            for name in ("health", "drain", "circuit", "overload"):
                self.assertEqual(entry[name], 1 if name == reason else 0)

        # total 四项均为 1。
        self.assertEqual(
            window["total"],
            {"health": 1, "drain": 1, "circuit": 1, "overload": 1},
        )

        # 顶层 backends 同样按加入序且目标权重不变。
        self.assertEqual(
            data["backends"],
            [
                {"id": "h", "weight": 1},
                {"id": "d", "weight": 1},
                {"id": "c", "weight": 1},
                {"id": "o", "weight": 1},
            ],
        )

    def test_ra_wrong_key_order_is_input(self):
        # ra 要求精确键序 op,from,to,now；乱序（此处 to 先于 from）为 INPUT。
        payload = (
            b'{"ops":['
            b'{"op":"add","id":"h","weight":1},'
            b'{"op":"ra","to":0,"from":0,"now":0}'
            b']}'
        )
        result = run_balancer("run", payload)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b'{"error":"INPUT"}\n')

    def test_ra_bool_numeric_is_input(self):
        # bool 是 int 子类：from=true 必须按非整数数值拒绝为 INPUT，不能被
        # 当作 1 接受。
        payload = (
            b'{"ops":['
            b'{"op":"add","id":"h","weight":1},'
            b'{"op":"ra","from":true,"to":0,"now":0}'
            b']}'
        )
        result = run_balancer("run", payload)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b'{"error":"INPUT"}\n')

    def test_clock_going_backwards_is_input(self):
        # now 纳入共用非递减时钟：先 now=1 再 now=0 即时钟倒退，整批 INPUT。
        ops = [
            {"op": "ra", "from": 0, "to": 0, "now": 1},
            {"op": "ra", "from": 0, "to": 0, "now": 0},
        ]
        result = run_balancer("run", encode_ops(ops))
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b'{"error":"INPUT"}\n')

    def test_window_before_retention_lower_bound_is_state(self):
        # now=3600 时当前窗为 60、保留下界为 max(0,60-59)=1，from=0 过早：
        # ra 与 rh 均报 STATE/4（rh 需现存后端，先 add h）。
        ra_result = run_balancer(
            "run", encode_ops([{"op": "ra", "from": 0, "to": 0, "now": 3600}])
        )
        self.assertEqual(ra_result.returncode, 4)
        self.assertEqual(ra_result.stdout, b"")
        self.assertEqual(ra_result.stderr, b'{"error":"STATE"}\n')

        rh_result = run_balancer(
            "run",
            encode_ops(
                [
                    {"op": "add", "id": "h", "weight": 1},
                    {"op": "rh", "id": "h", "from": 0, "to": 0, "now": 3600},
                ]
            ),
        )
        self.assertEqual(rh_result.returncode, 4)
        self.assertEqual(rh_result.stdout, b"")
        self.assertEqual(rh_result.stderr, b'{"error":"STATE"}\n')


class RecordReplayRegressionTests(unittest.TestCase):
    """record/replay 须逐字节复现 run 的退出码与两路输出。"""

    def assert_record_replay_matches(self, payload):
        direct = run_balancer("run", payload)

        record_result = run_balancer("record", payload)
        # record 无论底层成败均退出 0、stderr 为空。
        self.assertEqual(record_result.returncode, 0)
        self.assertEqual(record_result.stderr, b"")
        record = json.loads(record_result.stdout.decode("utf-8"))
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["exit"], direct.returncode)

        replay_result = run_balancer("replay", record_result.stdout)
        self.assertEqual(replay_result.returncode, direct.returncode)
        self.assertEqual(replay_result.stdout, direct.stdout)
        self.assertEqual(replay_result.stderr, direct.stderr)

    def test_core_input_roundtrip_byte_identical(self):
        self.assert_record_replay_matches(encode_ops(core_ops()))

    def test_failing_input_roundtrip_byte_identical(self):
        # 失败批（时钟倒退，exit=2）同样须逐字节复现退出码、stdout、stderr。
        payload = encode_ops(
            [
                {"op": "ra", "from": 0, "to": 0, "now": 1},
                {"op": "ra", "from": 0, "to": 0, "now": 0},
            ]
        )
        self.assert_record_replay_matches(payload)


if __name__ == "__main__":
    unittest.main()
