"""balancer.py 黑盒回归测试（仅用标准库）。

通过子进程运行 `python balancer.py run|record|replay`，stdin 发送 UTF-8
JSON，断言退出码、stdout、stderr。不修改被测程序的任何行为。
"""

import base64
import bisect
import hashlib
import json
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
BALANCER = os.path.join(HERE, "balancer.py")

FLOW = ["s", 1, "t", 2, "tcp"]


def encode_ops(ops):
    """与实现同款紧凑序列化，UTF-8 字节。"""
    return json.dumps(
        {"ops": ops}, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def run_balancer(command, raw):
    """以子进程执行 balancer.py，返回 (退出码, stdout 字节, stderr 字节)。"""
    proc = subprocess.run(
        [sys.executable, BALANCER, command],
        input=raw,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc.returncode, proc.stdout, proc.stderr


def core_ops():
    """核心单批：依次制造 health/drain/circuit/overload 各一次后查询。"""
    ops = []
    for backend_id in ("h", "d", "c", "o"):
        ops.append({"op": "add", "id": backend_id, "weight": 1})
    # h：fail=success=1 后一次失败探测即转 unhealthy，记 health。
    ops.append({"op": "hset", "id": "h", "fail": 1, "success": 1})
    ops.append({"op": "probe", "id": "h", "ok": False, "now": 0})
    # d：open x 持有连接（h 已不健康，d 为最早可选），ds 后 dr 转 D，记 drain。
    ops.append({"op": "open", "cid": "x", "flow": FLOW, "now": 0})
    ops.append({"op": "ds", "id": "d", "t": 10})
    ops.append({"op": "dr", "id": "d", "now": 0})
    # c：n=m=r=w=q=1 后一次失败报告即熔断转 O，记 circuit。
    ops.append({"op": "cs", "id": "c", "n": 1, "m": 1, "r": 1, "w": 1, "q": 1})
    ops.append({"op": "cr", "id": "c", "ok": False, "now": 0})
    # o：环上唯一可选后端，cap=1 被 open y 占满，oa z 满载入队，记 overload。
    ops.append({"op": "chash", "vnodes": 1})
    ops.append({"op": "os", "cap": 1, "q": 2, "ttl": 10})
    ops.append({"op": "open", "cid": "y", "flow": FLOW, "now": 0})
    ops.append(
        {
            "op": "oa",
            "cid": "z",
            "flow": FLOW,
            "c": "k",
            "s": "k",
            "key": "k",
            "now": 0,
        }
    )
    # 全部 now=0，查询 from=to=0。
    for backend_id in ("h", "d", "c", "o"):
        ops.append({"op": "rh", "id": backend_id, "from": 0, "to": 0, "now": 0})
    ops.append({"op": "ra", "from": 0, "to": 0, "now": 0})
    ops.append({"op": "ra", "from": 0, "to": 0, "now": 0})
    return ops


def core_input_bytes():
    return encode_ops(core_ops())


class CoreBatchTest(unittest.TestCase):
    """核心批次：四种不可用原因各记一次，rh/ra 查询结果确定。"""

    @classmethod
    def setUpClass(cls):
        cls.raw = core_input_bytes()
        cls.exit_code, cls.stdout, cls.stderr = run_balancer("run", cls.raw)

    def test_exit_zero_and_stderr_empty(self):
        self.assertEqual(self.exit_code, 0)
        self.assertEqual(self.stderr, b"")

    def test_rh_reasons(self):
        output = json.loads(self.stdout.decode("utf-8"))
        results = output["results"]
        rh_results = [r for r in results if r["op"] == "rh"]
        self.assertEqual([r["id"] for r in rh_results], ["h", "d", "c", "o"])
        expected = {
            "h": {"health": 1, "drain": 0, "circuit": 0, "overload": 0},
            "d": {"health": 0, "drain": 1, "circuit": 0, "overload": 0},
            "c": {"health": 0, "drain": 0, "circuit": 1, "overload": 0},
            "o": {"health": 0, "drain": 0, "circuit": 0, "overload": 1},
        }
        for result in rh_results:
            windows = result["windows"]
            self.assertEqual(len(windows), 1)
            window = windows[0]
            self.assertEqual(window["window"], 0)
            for reason in ("health", "drain", "circuit", "overload"):
                self.assertEqual(window[reason], expected[result["id"]][reason])

    def test_ra_identical_and_totals(self):
        output = json.loads(self.stdout.decode("utf-8"))
        results = output["results"]
        ra_results = [r for r in results if r["op"] == "ra"]
        self.assertEqual(len(ra_results), 2)
        # 两次 ra 完全相同。
        self.assertEqual(ra_results[0], ra_results[1])
        windows = ra_results[0]["windows"]
        self.assertEqual(len(windows), 1)
        window = windows[0]
        self.assertEqual(window["window"], 0)
        # backends 按加入序 h,d,c,o。
        self.assertEqual([b["id"] for b in window["backends"]], ["h", "d", "c", "o"])
        # total 四项均为 1。
        self.assertEqual(
            window["total"],
            {"health": 1, "drain": 1, "circuit": 1, "overload": 1},
        )


class RaRejectionTest(unittest.TestCase):
    """ra 的非法输入与时钟、窗口约束。"""

    def assert_failure(self, raw, exit_code, label):
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def test_ra_key_order_is_input(self):
        # 键须按 op,from,to,now 顺序出现。
        self.assert_failure(
            b'{"ops":[{"op":"ra","to":0,"from":0,"now":0}]}', 2, "INPUT"
        )

    def test_ra_bool_number_is_input(self):
        # bool 不是合法数值。
        self.assert_failure(
            b'{"ops":[{"op":"ra","from":true,"to":0,"now":0}]}', 2, "INPUT"
        )

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "h", "weight": 1},
            {"op": "probe", "id": "h", "ok": True, "now": 5},
            {"op": "ra", "from": 0, "to": 0, "now": 0},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_premature_window_is_state(self):
        # from 早于最近 60 窗下界（now=3600 时当前窗为 60，下界为 1）。
        self.assert_failure(
            b'{"ops":[{"op":"ra","from":0,"to":0,"now":3600}]}', 4, "STATE"
        )


def config_v6(weight, **overrides):
    """最小 version=6 配置：单后端 a，可按键覆盖顶层字段。"""
    config = {
        "version": 6,
        "backends": [
            {
                "id": "a",
                "weight": weight,
                "d": 0,
                "fail": 3,
                "success": 2,
                "circuit": None,
                "drain": None,
                "endpoint": None,
            }
        ],
        "vnodes": None,
        "limits": [],
        "overload": None,
        "sticky": None,
        "idle": None,
        "backpressure": None,
        "scheduler": {"pick": "W"},
    }
    config.update(overrides)
    return config


def config_v7(weight, faults=(), **overrides):
    """最小 version=7 配置：单后端 a，九键同 v6 且末置 faults 数组。"""
    config = config_v6(weight, **overrides)
    config["version"] = 7
    config["faults"] = list(faults)
    return config


class ConfigCommitTest(unittest.TestCase):
    """配置提交与回滚（ci 提交、cl 历史、cb 回滚）。"""

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        return code, out

    def test_cl_initially_empty(self):
        code, out = self.run_ops([{"op": "cl"}])
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][0]
        self.assertEqual(list(result), ["op", "current", "commits"])
        self.assertEqual(result, {"op": "cl", "current": None, "commits": []})

    def test_ci_commits_normalized_v7(self):
        # version=1 旧结构成功加载后，提交为规范化 version=7 配置
        # （faults 空计划）。
        config_v1 = {
            "version": 1,
            "backends": [
                {
                    "id": "a",
                    "weight": 2,
                    "d": 0,
                    "fail": 3,
                    "success": 2,
                    "circuit": None,
                    "drain": None,
                }
            ],
            "vnodes": None,
            "limits": [],
            "overload": None,
        }
        code, out = self.run_ops(
            [{"op": "ci", "config": config_v1, "now": 5}, {"op": "cl"}]
        )
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][1]
        self.assertEqual(result["current"], 1)
        self.assertEqual(len(result["commits"]), 1)
        commit = result["commits"][0]
        self.assertEqual(list(commit), ["rev", "config"])
        self.assertEqual(commit["rev"], 1)
        self.assertEqual(
            commit["config"],
            config_v7(2),
        )

    def test_failed_ci_does_not_commit(self):
        # B 限流引用未知后端：ci 失败（BACKEND），整批无 stdout。
        bad = config_v6(1, limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}])
        code, out, err = run_balancer(
            "run",
            encode_ops(
                [
                    {"op": "ci", "config": config_v6(1), "now": 0},
                    {"op": "ci", "config": bad, "now": 1},
                ]
            ),
        )
        self.assertEqual((code, out, err), (3, b"", b'{"error":"BACKEND"}\n'))

    def test_eviction_keeps_recent_16(self):
        ops = [
            {"op": "ci", "config": config_v6(i), "now": i} for i in range(1, 21)
        ]
        ops.append({"op": "cl"})
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][-1]
        self.assertEqual(result["current"], 20)
        self.assertEqual(
            [commit["rev"] for commit in result["commits"]],
            list(range(5, 21)),
        )
        self.assertEqual(
            result["commits"][0]["config"]["backends"][0]["weight"], 5
        )

    def test_cb_rollback_creates_new_rev(self):
        ops = [
            {"op": "ci", "config": config_v6(1), "now": 0},
            {"op": "ci", "config": config_v6(2), "now": 1},
            {"op": "cb", "rev": 1, "now": 2},
            {"op": "ce"},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)
        results = json.loads(out)["results"]
        self.assertEqual(
            results[2], {"op": "cb", "target": 1, "rev": 3, "ok": True}
        )
        # 回滚后当前配置即 rev=1 的规范化 v7 快照（faults 空计划）。
        self.assertEqual(results[3]["config"], config_v7(1))

    def test_cb_restores_runtime_state(self):
        # 回滚按目标快照重建默认运行态：调度策略、限流桶、预热自 cb.now 起算。
        config = config_v6(
            1,
            vnodes=5,
            limits=[{"scope": "B", "id": "a", "r": 3, "b": 9}],
            scheduler={"pick": "R"},
        )
        config["backends"][0]["d"] = 100
        ops = [
            {"op": "ci", "config": config, "now": 10},
            {"op": "ci", "config": config_v6(1), "now": 20},
            {"op": "cb", "rev": 1, "now": 50},
            {"op": "wg", "id": "a", "now": 50},
            {"op": "lg", "scope": "B", "id": "a", "now": 50},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)
        results = json.loads(out)["results"]
        self.assertEqual(results[3]["stage"], "warm")
        self.assertEqual((results[3]["start"], results[3]["end"]), (50, 150))
        self.assertEqual((results[4]["t"], results[4]["at"]), (9, 50))

    def test_cb_unknown_or_evicted_rev_is_state(self):
        ops = [
            {"op": "ci", "config": config_v6(i), "now": i} for i in range(1, 21)
        ]
        ops.append({"op": "cb", "rev": 4, "now": 20})
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (4, b"", b'{"error":"STATE"}\n'))

    def test_cb_with_active_connection_is_state(self):
        ops = [
            {"op": "ci", "config": config_v6(1), "now": 0},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 1},
            {"op": "cb", "rev": 1, "now": 2},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (4, b"", b'{"error":"STATE"}\n'))

    def test_cb_invalid_rev_is_input(self):
        for rev in (0, -1, 10 ** 18 + 1, True, "1", 1.5):
            code, out, err = run_balancer(
                "run", encode_ops([{"op": "cb", "rev": rev, "now": 0}])
            )
            self.assertEqual(
                (code, out, err), (2, b"", b'{"error":"INPUT"}\n'), rev
            )

    def test_cb_clock_regression_is_input(self):
        ops = [
            {"op": "ci", "config": config_v6(1), "now": 10},
            {"op": "cb", "rev": 1, "now": 5},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))

    def test_record_replay_covers_cl_cb(self):
        ops = [
            {"op": "ci", "config": config_v6(1), "now": 0},
            {"op": "ci", "config": config_v6(2), "now": 1},
            {"op": "cb", "rev": 1, "now": 2},
            {"op": "cl"},
        ]
        raw = encode_ops(ops)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, base64.b64decode(record["stderr"]))


class FaultHotReloadTest(unittest.TestCase):
    """version=7 faults 时间线纳入 ce/ci/cl/cb 热加载。"""

    FLOW = ["s", 1, "t", 2, "tcp"]

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        self.assertEqual(code, 0)
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, raw, exit_code, label):
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def seg(self, backend="a", k="D", a=0, z=10, v=0):
        return {"id": backend, "k": k, "a": a, "z": z, "v": v}

    def test_ce_exports_v7_with_empty_faults_last(self):
        results = self.run_ops([{"op": "add", "id": "a", "weight": 1},
                                {"op": "ce"}])
        config = results[-1]["config"]
        self.assertEqual(
            list(config),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults"],
        )
        self.assertEqual(config["version"], 7)
        self.assertEqual(config["faults"], [])

    def test_ci_loads_faults_observed_by_fq(self):
        # 乱序提交（段与后端），ce/fq 按后端加入序、段 a 升序规范化。
        config = config_v7(
            1,
            vnodes=4,
            faults=[
                self.seg("b", "D", 0, 10),
                self.seg("a", "S", 5, 20, 3),
                self.seg("a", "D", 0, 5),
            ],
        )
        config["backends"].append(
            {"id": "b", "weight": 1, "d": 0, "fail": 3, "success": 2,
             "circuit": None, "drain": None, "endpoint": None}
        )
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "fq", "now": 2},
            {"op": "ce"},
        ])
        fq = results[1]
        self.assertEqual(
            [(f["id"], f["k"], f["a"]) for f in fq["faults"]],
            [("a", "D", 0), ("a", "S", 5), ("b", "D", 0)],
        )
        self.assertEqual(
            [f["effect"] for f in fq["faults"]], ["D", "N", "D"]
        )
        self.assertEqual((fq["down"], fq["slow"]), (2, 0))
        # ce 不含 effect，纯登记段，顺序同 fq。
        self.assertEqual(
            [(f["id"], f["k"], f["a"], f["z"], f["v"])
             for f in results[2]["config"]["faults"]],
            [("a", "D", 0, 5, 0), ("a", "S", 5, 20, 3), ("b", "D", 0, 10, 0)],
        )

    def test_loaded_timeline_drives_fx_fr_fi(self):
        config = config_v7(
            1,
            vnodes=8,
            faults=[self.seg("a", "D", 0, 10)],
        )
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "fx", "cid": "c1", "flow": self.FLOW, "key": "k",
             "timeout": 9, "now": 2},
            {"op": "fr", "cid": "c2", "flow": self.FLOW, "key": "k",
             "timeout": 9, "max": 3, "now": 2},
            {"op": "fi", "keys": ["x", "y"], "timeout": 9, "max": 3,
             "now": 2},
        ])
        self.assertEqual(results[1]["state"], "R")
        self.assertIsNone(results[1]["backend"])
        self.assertEqual(results[2]["state"], "R")
        self.assertIsNone(results[2]["backend"])
        self.assertTrue(all(c["state"] == "R" for c in results[3]["cases"]))

    def test_reload_resets_fault_runtime_but_keeps_timeline(self):
        config = config_v7(1, vnodes=8, faults=[self.seg("a", "D", 0, 10)])
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "fx", "cid": "c1", "flow": self.FLOW, "key": "k",
             "timeout": 9, "now": 1},
            {"op": "fm", "id": "a"},
            {"op": "ci", "config": config, "now": 2},
            {"op": "fm", "id": "a"},
            {"op": "fq", "now": 3},
        ])
        self.assertEqual(results[2]["D"]["affected"], 1)
        self.assertEqual(results[4]["D"]["affected"], 0)
        self.assertEqual(results[5]["down"], 1)

    def test_v6_and_below_clear_faults(self):
        # 既有 fs 时间线在 version=6（无 faults 键）热加载后清空。
        results = self.run_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
            {"op": "fq", "now": 0},
            {"op": "ci", "config": config_v6(1), "now": 1},
            {"op": "fq", "now": 2},
        ])
        self.assertEqual(results[2]["down"], 1)
        self.assertEqual(results[4]["faults"], [])

    def test_v7_requires_exact_faults_key(self):
        # 十键结构但 version=6（v6 结构不含 faults）。
        bad_version = config_v7(1)
        bad_version["version"] = 6
        self.assert_failure(
            encode_ops([{"op": "ci", "config": bad_version, "now": 0}]),
            2, "INPUT",
        )
        # version=7 缺 faults 键（九键）。
        missing = config_v6(1)
        missing["version"] = 7
        self.assert_failure(
            encode_ops([{"op": "ci", "config": missing, "now": 0}]),
            2, "INPUT",
        )

    def test_faults_validation_errors(self):
        cases = [
            b'{"op":"ci","now":0,"config":' + self._raw(faults="{}") + b"}",
            # 项键集缺 v。
            b'{"op":"ci","now":0,"config":'
            + self._raw(faults='[{"id":"a","k":"D","a":0,"z":5}]') + b"}",
            # 项键序错误。
            b'{"op":"ci","now":0,"config":'
            + self._raw(faults='[{"id":"a","k":"D","z":5,"a":0,"v":0}]')
            + b"}",
            # k 越界。
            self._enc([{"op": "ci", "now": 0, "config":
                        config_v7(1, faults=[self.seg(k="X")])}]),
            # D 须 v=0。
            self._enc([{"op": "ci", "now": 0, "config":
                        config_v7(1, faults=[self.seg(k="D", v=1)])}]),
            # F 须 v>0。
            self._enc([{"op": "ci", "now": 0, "config":
                        config_v7(1, faults=[self.seg(k="F", v=0)])}]),
            # a 为 bool。
            self._enc([{"op": "ci", "now": 0, "config":
                        config_v7(1, faults=[self.seg(a=True)])}]),
            # a==z。
            self._enc([{"op": "ci", "now": 0, "config":
                        config_v7(1, faults=[self.seg(a=5, z=5)])}]),
            # 同 id 段重叠。
            self._enc([{"op": "ci", "now": 0, "config": config_v7(
                1, faults=[self.seg(a=0, z=5), self.seg(a=4, z=9)])}]),
            # 同 id 完全重复段。
            self._enc([{"op": "ci", "now": 0, "config": config_v7(
                1, faults=[self.seg(), self.seg()])}]),
            # id 非 UTF-8（孤立代理）。
            b'{"op":"ci","now":0,"config":'
            + self._raw(faults='[{"id":"\\ud800","k":"D","a":0,"z":5,"v":0}]')
            + b"}",
        ]
        for raw in cases:
            self.assert_failure(raw, 2, "INPUT")

    @staticmethod
    def _raw(**fields):
        # 构造一个嵌入 faults 原始 JSON 片段的 v7 config 字节。
        parts = [
            '"version":7',
            '"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            '"circuit":null,"drain":null,"endpoint":null}]',
            '"vnodes":null', '"limits":[]', '"overload":null',
            '"sticky":null', '"idle":null', '"backpressure":null',
            '"scheduler":{"pick":"W"}',
        ]
        for key, value in fields.items():
            parts.append('"%s":%s' % (key, value))
        return ("{" + ",".join(parts) + "}").encode("utf-8")

    @staticmethod
    def _enc(ops):
        return encode_ops(ops)

    def test_unknown_fault_backend_is_backend(self):
        config = config_v7(
            1, faults=[{"id": "ghost", "k": "D", "a": 0, "z": 5, "v": 0}]
        )
        self.assert_failure(
            encode_ops([{"op": "ci", "config": config, "now": 0}]),
            3, "BACKEND",
        )
        # INPUT 先于 BACKEND：未知 id 但 k 非法仍判 INPUT。
        bad = self._raw(faults='[{"id":"ghost","k":"X","a":0,"z":5,"v":0}]')
        self.assert_failure(
            b'{"ops":[{"op":"ci","now":0,"config":' + bad + b"}]}",
            2, "INPUT",
        )

    def test_backend_precedes_state_for_unknown_fault(self):
        config = config_v7(
            1, faults=[{"id": "ghost", "k": "D", "a": 0, "z": 5, "v": 0}]
        )
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v7(1), "now": 0},
                {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
                {"op": "ci", "config": config, "now": 2},
            ]),
            3, "BACKEND",
        )

    def test_active_connection_or_queue_is_state(self):
        with_fault = config_v7(1, faults=[self.seg("a", "D", 0, 5)])
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v7(1), "now": 0},
                {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
                {"op": "ci", "config": with_fault, "now": 2},
            ]),
            4, "STATE",
        )

    def test_cb_restores_target_faults_and_resets_runtime(self):
        seg = self.seg("a", "D", 0, 10)
        with_fault = config_v7(1, faults=[seg])
        results = self.run_ops([
            {"op": "ci", "config": with_fault, "now": 0},
            {"op": "ci", "config": config_v7(1), "now": 1},
            {"op": "fq", "now": 2},
            {"op": "cb", "rev": 1, "now": 3},
            {"op": "fq", "now": 4},
            {"op": "cl"},
        ])
        self.assertEqual(results[2]["faults"], [])
        self.assertEqual(
            results[3], {"op": "cb", "target": 1, "rev": 3, "ok": True}
        )
        self.assertEqual(results[4]["down"], 1)
        commits = results[5]["commits"]
        self.assertEqual(commits[0]["config"]["faults"],
                         [{"id": "a", "k": "D", "a": 0, "z": 10, "v": 0}])
        self.assertEqual(commits[2]["config"]["faults"],
                         [{"id": "a", "k": "D", "a": 0, "z": 10, "v": 0}])

    def test_repeated_load_builds_new_rev(self):
        seg = self.seg("a", "D", 0, 10)
        config = config_v7(1, faults=[seg])
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "ci", "config": config, "now": 1},
            {"op": "cl"},
        ])
        self.assertEqual(
            [c["rev"] for c in results[2]["commits"]], [1, 2]
        )
        self.assertEqual(results[2]["current"], 2)

    def test_record_replay_covers_v7_faults(self):
        config = config_v7(
            1, vnodes=4,
            faults=[self.seg("a", "D", 0, 5), self.seg("a", "S", 5, 20, 3)],
        )
        ops = [
            {"op": "ci", "config": config, "now": 0},
            {"op": "fq", "now": 6},
            {"op": "ce"},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual((run_code, rec_code), (0, 0))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)
        # ce 输出逐字节固定键序、紧凑、单换行。
        self.assertEqual(run_stdout.count(b"\n"), 1)
        self.assertIn(b'"version":7', run_stdout)


class FaultTimelineTest(unittest.TestCase):
    """fp 故障时间线：多段、规范化、fq 快照与 fx/fm 跨段恢复记账。"""

    FLOW = ["s", 1, "t", 2, "tcp"]

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        if err:
            self.assertEqual(code, 0, err)
        self.assertEqual(err, b"")
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, raw, exit_code, label):
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def test_multi_segment_fq_effects(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fp", "items": [
                {"id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
                {"id": "a", "k": "S", "a": 10, "z": 20, "v": 3},
            ]},
            {"op": "fq", "now": 5},
            {"op": "fq", "now": 15},
            {"op": "fq", "now": 20},
        ]
        results = self.run_ops(ops)
        fq = [r for r in results if r["op"] == "fq"]
        self.assertEqual(
            [(f["k"], f["effect"]) for f in fq[0]["faults"]],
            [("D", "D"), ("S", "N")],
        )
        self.assertEqual((fq[0]["down"], fq[0]["slow"]), (1, 0))
        self.assertEqual(
            [(f["k"], f["effect"]) for f in fq[1]["faults"]],
            [("D", "N"), ("S", "S")],
        )
        self.assertEqual((fq[1]["down"], fq[1]["slow"]), (0, 1))
        # 间隙/越界：全部段 effect=N。
        self.assertTrue(
            all(f["effect"] == "N" for f in fq[2]["faults"])
        )
        self.assertEqual((fq[2]["down"], fq[2]["slow"]), (0, 0))

    def test_unordered_segments_normalized_and_idempotent(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fp", "items": [
                {"id": "a", "k": "S", "a": 10, "z": 20, "v": 3},
                {"id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
            ]},
            {"op": "fq", "now": 5},
        ]
        results = self.run_ops(ops)
        fq = results[-1]
        # 输出按 a 升序规范化。
        self.assertEqual([(f["k"], f["a"]) for f in fq["faults"]],
                         [("D", 0), ("S", 10)])

    def test_recovered_attributed_to_previous_segment_kind(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "fp", "items": [
                {"id": "a", "k": "D", "a": 0, "z": 5, "v": 0},
                {"id": "a", "k": "S", "a": 5, "z": 10, "v": 1},
            ]},
            {"op": "fx", "cid": "c1", "flow": self.FLOW, "key": "k",
             "timeout": 9, "now": 0},
            {"op": "fx", "cid": "c2", "flow": self.FLOW, "key": "k",
             "timeout": 9, "now": 5},
            {"op": "fm", "id": "a"},
        ]
        results = self.run_ops(ops)
        fm = results[-1]
        # D 段受影响并被拒；进入相邻 S 段时 D 记 recovered，S 记 affected。
        self.assertEqual(
            fm["D"],
            {"affected": 1, "rejected": 1, "retries": 0,
             "remaps": 1, "recovered": 1},
        )
        self.assertEqual(fm["S"]["affected"], 1)
        self.assertEqual(fm["S"]["recovered"], 0)

    def test_fp_empty_is_noop(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
            {"op": "fp", "items": []},
            {"op": "fq", "now": 0},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["down"], 1)

    def test_fb_clears_unlisted(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fp", "items": [
                {"id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
            ]},
            {"op": "fb", "items": []},
            {"op": "fq", "now": 0},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["faults"], [])

    def test_fp_rejections(self):
        # 顶层键序错误。
        self.assert_failure(
            b'{"ops":[{"items":[],"op":"fp"}]}', 2, "INPUT"
        )
        # 项键序错误。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "a", "weight": 1},
                {"op": "fp", "items": [
                    {"id": "a", "z": 2, "k": "D", "a": 1, "v": 0},
                ]},
            ]),
            2, "INPUT",
        )
        # items 不是数组。
        self.assert_failure(
            b'{"ops":[{"op":"fp","items":{}}]}', 2, "INPUT"
        )
        # 同后端段重叠。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "a", "weight": 1},
                {"op": "fp", "items": [
                    {"id": "a", "k": "D", "a": 0, "z": 5, "v": 0},
                    {"id": "a", "k": "D", "a": 4, "z": 9, "v": 0},
                ]},
            ]),
            2, "INPUT",
        )
        # 项数超 4096。
        too_many = [
            {"id": "a", "k": "D", "a": i, "z": i + 1, "v": 0}
            for i in range(4097)
        ]
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "a", "weight": 1},
                {"op": "fp", "items": too_many},
            ]),
            2, "INPUT",
        )
        # 结构合法但 id 未知 -> BACKEND。
        self.assert_failure(
            encode_ops([
                {"op": "fp", "items": [
                    {"id": "ghost", "k": "D", "a": 0, "z": 5, "v": 0},
                ]},
            ]),
            3, "BACKEND",
        )
        # INPUT 先于 BACKEND 判定。
        self.assert_failure(
            b'{"ops":[{"op":"fp","items":['
            b'{"id":"ghost","k":"X","a":0,"z":5,"v":0}]}]}',
            2, "INPUT",
        )

    def test_fp_4096_adjacent_segments_accepted(self):
        segments = [
            {"id": "a", "k": "D", "a": i, "z": i + 1, "v": 0}
            for i in range(4096)
        ]
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fp", "items": segments},
            {"op": "fq", "now": 7},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["down"], 1)
        self.assertEqual(results[-1]["slow"], 0)
        self.assertEqual(len(results[-1]["faults"]), 4096)

    def test_fp_only_replaces_listed_backends(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "add", "id": "b", "weight": 1},
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
            {"op": "fs", "id": "b", "k": "D", "a": 0, "z": 10, "v": 0},
            {"op": "fp", "items": [
                {"id": "a", "k": "S", "a": 0, "z": 10, "v": 2},
            ]},
            {"op": "fq", "now": 0},
        ]
        results = self.run_ops(ops)
        faults = results[-1]["faults"]
        self.assertEqual(
            [(f["id"], f["k"], f["effect"]) for f in faults],
            [("a", "S", "S"), ("b", "D", "D")],
        )

    def test_record_replay_covers_fp(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fp", "items": [
                {"id": "a", "k": "D", "a": 0, "z": 10, "v": 0},
                {"id": "a", "k": "S", "a": 10, "z": 20, "v": 3},
            ]},
            {"op": "fq", "now": 15},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual(rec_code, 0)
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)


class MoSnapshotTest(unittest.TestCase):
    """全池增量快照 mo：游标、缓存、基线、跨窗与重置契约。"""

    def run_ops(self, ops):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        if code != 0:
            self.fail("run failed: %d %r" % (code, stderr))
        return json.loads(stdout.decode("utf-8"))["results"]

    def assert_fails(self, ops, exit_code):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr,
            ('{"error":"%s"}\n' % ("INPUT" if exit_code == 2 else "STATE")).encode(),
        )

    @staticmethod
    def mo(seq, now):
        return {"op": "mo", "seq": seq, "now": now}

    @staticmethod
    def mr(backend_id, ok, ms, now, retries=0, remaps=0):
        return {
            "op": "mr", "id": backend_id, "ok": ok, "ms": ms,
            "retries": retries, "remaps": remaps, "now": now,
        }

    def test_empty_pool_first_snapshot(self):
        results = self.run_ops([self.mo(1, 0)])
        self.assertEqual(results[-1], {"op": "mo", "seq": 1, "window": 0,
                                       "backends": []})
        self.assertEqual(
            list(results[-1]), ["op", "seq", "window", "backends"]
        )

    def test_first_snapshot_zero_baseline_and_key_order(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "add", "id": "b", "weight": 1},
            self.mr("a", True, 1, 0, retries=2, remaps=1),
            self.mr("a", False, 50, 0),
            self.mo(1, 0),
        ]
        item = self.run_ops(ops)[-1]["backends"][0]
        self.assertEqual(
            list(item),
            ["id", "requests", "qps", "concurrency", "errors", "error_rate",
             "latency", "retries", "remaps", "removed"],
        )
        self.assertEqual(item["requests"], 2)
        self.assertEqual(item["errors"], 1)
        self.assertEqual(item["qps"], "0.03")
        self.assertEqual(item["error_rate"], "50.00")
        # ms=1 落 ≤1 桶，ms=50 落 ≤100 桶。
        self.assertEqual(item["latency"], [1, 0, 1, 0, 0])
        self.assertEqual(item["retries"], 2)
        self.assertEqual(item["remaps"], 1)
        self.assertEqual(item["concurrency"], 0)
        self.assertIsNone(item["removed"])
        # backends 按现存加入序。
        self.assertEqual(
            [b["id"] for b in self.run_ops(ops)[-1]["backends"]], ["a", "b"]
        )

    def test_replay_same_seq_now_returns_cache_without_advance(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            self.mo(1, 0),
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[-2], results[-1])
        # 缓存重报不推进时钟：其后 now=0 的 mr 合法，且下一次 mo 仍是 seq=2。
        ops.append(self.mr("a", True, 1, 0))
        ops.append(self.mo(2, 0))
        results = self.run_ops(ops)
        # 增量仅含基线之后的一次 mr。
        self.assertEqual(results[-1]["backends"][0]["requests"], 1)

    def test_replay_after_clock_advanced_still_cached(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            self.mr("a", True, 1, 200),
            self.mo(1, 0),
        ]
        results = self.run_ops(ops)
        # ops 索引：0 add、1 mr、2 mo(1,0)、3 mr now=200、4 mo(1,0) 重报。
        self.assertEqual(results[-1], results[2])
        # 时钟未被重报回退：now=150 的 mr 倒退报 INPUT。
        self.assert_fails(ops + [self.mr("a", True, 1, 150)], 2)

    def test_increment_same_window(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            self.mr("a", False, 5000, 59, retries=3),
            self.mo(2, 59),
        ]
        item = self.run_ops(ops)[-1]["backends"][0]
        self.assertEqual(item["requests"], 1)
        self.assertEqual(item["errors"], 1)
        self.assertEqual(item["retries"], 3)
        self.assertEqual(item["latency"], [0, 0, 0, 0, 1])
        self.assertEqual(item["qps"], "0.01")
        self.assertEqual(item["error_rate"], "100.00")

    def test_cross_window_baseline_zero(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            self.mr("a", True, 100, 60),
            self.mo(2, 60),
        ]
        result = self.run_ops(ops)[-1]
        self.assertEqual(result["window"], 1)
        self.assertEqual(result["backends"][0]["requests"], 1)
        self.assertEqual(result["backends"][0]["latency"], [0, 0, 1, 0, 0])

    def test_concurrency_and_removed_present_values(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "open", "cid": "c", "flow": FLOW, "now": 0},
            self.mo(1, 0),
            {"op": "probe", "id": "a", "ok": False, "now": 1},
            self.mo(2, 1),
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[-3]["backends"][0]["concurrency"], 1)
        self.assertIsNone(results[-3]["backends"][0]["removed"])
        self.assertEqual(results[-1]["backends"][0]["concurrency"], 1)
        self.assertEqual(results[-1]["backends"][0]["removed"], "health")

    def test_remove_readd_counts_from_zero(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            {"op": "remove", "id": "a"},
            {"op": "add", "id": "a", "weight": 1},
            self.mo(2, 0),
        ]
        self.assertEqual(self.run_ops(ops)[-1]["backends"][0]["requests"], 0)

    def test_ci_resets_cursor_and_baseline(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            {"op": "ce"},
        ]
        results = self.run_ops(ops)
        config = results[-1]["config"]
        ops.append({"op": "ci", "config": config, "now": 1})
        ops.append(self.mo(1, 1))
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["seq"], 1)
        self.assertEqual(results[-1]["backends"][0]["requests"], 0)
        # ci 后 seq 重置：seq=2 跳号报 STATE。
        self.assert_fails(ops[:-1] + [self.mo(2, 1)], 4)

    def test_seq_violations_are_state(self):
        self.assert_fails([self.mo(2, 0)], 4)          # 首次非 1
        self.assert_fails([self.mo(1, 0), self.mo(3, 1)], 4)   # 跳号
        self.assert_fails([self.mo(1, 0), self.mo(1, 1)], 4)   # 同 seq 异 now
        self.assert_fails(                                         # 旧 seq、时钟不倒退
            [self.mo(1, 0), self.mo(2, 1), self.mo(2, 2)], 4
        )

    def test_input_violations(self):
        cases = [
            b'{"ops":[{"op":"mo","now":0,"seq":1}]}',          # 键序
            b'{"ops":[{"op":"mo","seq":1}]}',                  # 缺键
            b'{"ops":[{"op":"mo","seq":1,"now":0,"x":1}]}',    # 多键
            b'{"ops":[{"op":"mo","seq":true,"now":0}]}',       # bool seq
            b'{"ops":[{"op":"mo","seq":1,"now":true}]}',       # bool now
            b'{"ops":[{"op":"mo","seq":0,"now":0}]}',          # seq 越界
            b'{"ops":[{"op":"mo","seq":1,"now":-1}]}',         # now 越界
            b'{"ops":[{"op":"mo","seq":1,"now":1000000001}]}',
        ]
        for raw in cases:
            code, stdout, stderr = run_balancer("run", raw)
            self.assertEqual((code, stdout, stderr),
                             (2, b"", b'{"error":"INPUT"}\n'), raw)

    def test_clock_regression_is_input_before_seq(self):
        # seq 同时失配，但时钟倒退优先报 INPUT。
        self.assert_fails([self.mo(1, 5), self.mo(2, 4)], 2)
        self.assert_fails([self.mo(1, 5), self.mo(1, 4)], 2)

    def test_failed_batch_rolls_back_cursor_and_clock(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", True, 1, 0),
            self.mo(1, 0),
            {"op": "get", "cid": "missing"},
        ]
        code, stdout, _ = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (5, b""))

    def test_record_replay_covers_mo(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            self.mr("a", False, 10, 0),
            self.mo(1, 0),
            self.mo(1, 0),
            self.mr("a", True, 100, 60),
            self.mo(2, 60),
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class QuotaWindowTest(unittest.TestCase):
    """qs/qg 固定窗口配额与 la 的配额判定。"""

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        self.assertEqual(code, 0)
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, ops, exit_code, label):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def base_ops(self):
        # 环上唯一后端 a，供 la 路由。
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 4},
        ]

    def test_qs_qg_basic_flow_and_key_order(self):
        results = self.run_ops([
            {"op": "qs", "scope": "C", "id": "c1",
             "limit": 5, "span": 10, "now": 7},
            {"op": "qg", "scope": "C", "id": "c1", "now": 8},
        ])
        self.assertEqual(results[0], {"op": "qs", "ok": True})
        self.assertEqual(
            list(results[1]),
            ["op", "scope", "id", "limit", "span",
             "window", "used", "remaining"],
        )
        self.assertEqual(
            results[1],
            {"op": "qg", "scope": "C", "id": "c1", "limit": 5, "span": 10,
             "window": 0, "used": 0, "remaining": 5},
        )

    def test_qs_idempotent_reconfigure_resets(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 10, "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
            # 同 (limit,span,now) 重报幂等：used 保持 1。
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 10, "now": 0},
            {"op": "qg", "scope": "B", "id": "a", "now": 0},
            # 异参重配置：window=now//span、used=0。
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 10, "now": 1},
            {"op": "qg", "scope": "B", "id": "a", "now": 1},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[5]["used"], 1)
        self.assertEqual(results[7]["used"], 0)
        self.assertEqual(results[7]["window"], 0)

    def test_la_consumes_quota_then_rate(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 1, "span": 10, "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
            # 配额已尽：RATE/6，整批无 stdout。
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 1},
        ]
        self.assert_failure(ops, 6, "RATE")

    def test_la_unconfigured_quota_unlimited(self):
        ops = self.base_ops() + [
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 1},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[2], {"op": "la", "backend": "a", "ok": True})
        self.assertEqual(results[3], {"op": "la", "backend": "a", "ok": True})

    def test_la_cost_based_quota_deduction(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 10, "span": 100, "now": 0},
            {"op": "qs", "scope": "C", "id": "c1",
             "limit": 3, "span": 100, "now": 0},
            {"op": "qs", "scope": "S", "id": "s1",
             "limit": 100, "span": 100, "now": 0},
            {"op": "la", "c": "c1", "s": "s1", "key": "k",
             "bc": 4, "cc": 2, "sc": 7, "now": 0},
            {"op": "qg", "scope": "B", "id": "a", "now": 0},
            {"op": "qg", "scope": "C", "id": "c1", "now": 0},
            {"op": "qg", "scope": "S", "id": "s1", "now": 0},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[6]["used"], 4)
        self.assertEqual(results[7]["used"], 2)
        self.assertEqual(results[7]["remaining"], 1)
        self.assertEqual(results[8]["used"], 7)

    def test_la_token_and_quota_both_required(self):
        # 令牌足、配额不足：RATE。
        ops = self.base_ops() + [
            {"op": "ls", "scope": "B", "id": "a", "r": 1, "b": 100, "now": 0},
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 1, "span": 10, "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
        ]
        self.assert_failure(ops, 6, "RATE")
        # 配额足、令牌不足：RATE（既有行为不变）。
        ops = self.base_ops() + [
            {"op": "ls", "scope": "B", "id": "a", "r": 1, "b": 1, "now": 0},
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 100, "span": 10, "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
        ]
        self.assert_failure(ops, 6, "RATE")

    def test_window_crossing_resets_used(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 1, "span": 10, "now": 0},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 0},
            # 跨窗（now//span 0→1）：used 清零，本窗可再扣一次。
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 10},
            {"op": "qg", "scope": "B", "id": "a", "now": 19},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[4], {"op": "la", "backend": "a", "ok": True})
        self.assertEqual(
            results[5],
            {"op": "qg", "scope": "B", "id": "a", "limit": 1, "span": 10,
             "window": 1, "used": 1, "remaining": 0},
        )

    def test_qg_unconfigured_is_state(self):
        self.assert_failure(
            [{"op": "qg", "scope": "C", "id": "c1", "now": 0}], 4, "STATE"
        )

    def test_qs_unknown_backend_is_backend(self):
        self.assert_failure(
            [{"op": "qs", "scope": "B", "id": "ghost",
              "limit": 1, "span": 1, "now": 0}],
            3, "BACKEND",
        )
        # C/S 作用域不引用后端，任意 id 均可配置。
        results = self.run_ops([
            {"op": "qs", "scope": "S", "id": "ghost",
             "limit": 1, "span": 1, "now": 0},
        ])
        self.assertEqual(results[0], {"op": "qs", "ok": True})

    def test_input_violations(self):
        bad_ops = [
            # 键集不符。
            {"op": "qs", "scope": "C", "id": "c", "limit": 1, "span": 1},
            {"op": "qs", "scope": "C", "id": "c", "limit": 1, "span": 1,
             "now": 0, "x": 1},
            {"op": "qg", "scope": "C", "id": "c"},
            {"op": "qg", "scope": "C", "id": "c", "now": 0, "limit": 1},
            # scope 非法。
            {"op": "qs", "scope": "X", "id": "c", "limit": 1, "span": 1,
             "now": 0},
            {"op": "qg", "scope": "b", "id": "c", "now": 0},
            # id 非法（空串、非字符串）。
            {"op": "qs", "scope": "C", "id": "", "limit": 1, "span": 1,
             "now": 0},
            {"op": "qg", "scope": "C", "id": 1, "now": 0},
            # limit/span/now 类型与范围。
            {"op": "qs", "scope": "C", "id": "c", "limit": 0, "span": 1,
             "now": 0},
            {"op": "qs", "scope": "C", "id": "c", "limit": 10 ** 18 + 1,
             "span": 1, "now": 0},
            {"op": "qs", "scope": "C", "id": "c", "limit": True, "span": 1,
             "now": 0},
            {"op": "qs", "scope": "C", "id": "c", "limit": 1, "span": 0,
             "now": 0},
            {"op": "qs", "scope": "C", "id": "c", "limit": 1,
             "span": 10 ** 9 + 1, "now": 0},
            {"op": "qs", "scope": "C", "id": "c", "limit": 1, "span": 1,
             "now": -1},
            {"op": "qs", "scope": "C", "id": "c", "limit": 1, "span": 1,
             "now": 10 ** 9 + 1},
            {"op": "qg", "scope": "C", "id": "c", "now": False},
        ]
        for bad in bad_ops:
            self.assert_failure([bad], 2, "INPUT")

    def test_clock_regression_is_input(self):
        # qs/qg 的 now 纳入共用非递减时钟。
        self.assert_failure(
            [
                {"op": "qs", "scope": "C", "id": "c",
                 "limit": 1, "span": 1, "now": 5},
                {"op": "qg", "scope": "C", "id": "c", "now": 4},
            ],
            2, "INPUT",
        )
        self.assert_failure(
            self.base_ops() + [
                {"op": "la", "c": "c", "s": "s", "key": "k", "now": 5},
                {"op": "qs", "scope": "C", "id": "c",
                 "limit": 1, "span": 1, "now": 4},
            ],
            2, "INPUT",
        )

    def test_qs_exact_rereport_after_clock_advanced(self):
        # qs 精确重报（limit/span/now 同上次配置）豁免时钟倒退：时钟已前进
        # 仍返回 ok，不回拨时钟并保留 window/used。
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 10, "now": 3},
            {"op": "la", "c": "c", "s": "s", "key": "k", "now": 3},
            # 时钟前进到 8（与配额同窗 0）。
            {"op": "probe", "id": "a", "ok": True, "now": 8},
            # 同 (limit,span,now) 精确重报：幂等返回 ok，不清 used。
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 10, "now": 3},
            {"op": "qg", "scope": "B", "id": "a", "now": 9},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[5], {"op": "qs", "ok": True})
        # used 保留为 1（重报未重配置、未清已用），窗口未被重报拨动。
        self.assertEqual(results[6]["window"], 0)
        self.assertEqual(results[6]["used"], 1)
        # 时钟未回拨：重报后 last_now 仍为 8，now=4 的 qg 仍报 INPUT。
        self.assert_failure(
            self.base_ops() + [
                {"op": "qs", "scope": "B", "id": "a",
                 "limit": 5, "span": 10, "now": 3},
                {"op": "probe", "id": "a", "ok": True, "now": 8},
                {"op": "qs", "scope": "B", "id": "a",
                 "limit": 5, "span": 10, "now": 3},
                {"op": "qg", "scope": "B", "id": "a", "now": 4},
            ],
            2, "INPUT",
        )

    def test_qs_old_now_other_shapes_still_input(self):
        # 其他旧时刻 qs/qg 仍报 INPUT：异参 qs（异 limit/span/now）与 qg。
        for stale in (
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 6, "span": 10, "now": 3},
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 11, "now": 3},
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 5, "span": 10, "now": 2},
            {"op": "qg", "scope": "B", "id": "a", "now": 3},
        ):
            self.assert_failure(
                self.base_ops() + [
                    {"op": "qs", "scope": "B", "id": "a",
                     "limit": 5, "span": 10, "now": 3},
                    {"op": "probe", "id": "a", "ok": True, "now": 8},
                    stale,
                ],
                2, "INPUT",
            )

    def test_remove_deletes_b_quota(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 1, "span": 1, "now": 0},
            {"op": "remove", "id": "a"},
            {"op": "qg", "scope": "B", "id": "a", "now": 0},
        ]
        self.assert_failure(ops, 4, "STATE")

    def test_ci_clears_quotas_and_ce_unchanged(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "C", "id": "c",
             "limit": 1, "span": 1, "now": 0},
            {"op": "ci", "config": config_v7(1), "now": 1},
            {"op": "ce"},
        ]
        results = self.run_ops(ops)
        # ce 导出不变：精确十键、无配额内容。
        self.assertEqual(
            list(results[4]["config"]),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults"],
        )
        # ci 成功后配额已清空。
        self.assert_failure(
            self.base_ops() + [
                {"op": "qs", "scope": "C", "id": "c",
                 "limit": 1, "span": 1, "now": 0},
                {"op": "ci", "config": config_v7(1), "now": 1},
                {"op": "qg", "scope": "C", "id": "c", "now": 1},
            ],
            4, "STATE",
        )

    def test_cb_clears_quotas(self):
        ops = [
            {"op": "ci", "config": config_v7(1), "now": 0},
            {"op": "qs", "scope": "C", "id": "c",
             "limit": 1, "span": 1, "now": 1},
            {"op": "cb", "rev": 1, "now": 2},
            {"op": "qg", "scope": "C", "id": "c", "now": 2},
        ]
        self.assert_failure(ops, 4, "STATE")

    def test_record_replay_covers_quotas(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "B", "id": "a",
             "limit": 2, "span": 10, "now": 0},
            {"op": "qs", "scope": "C", "id": "c1",
             "limit": 5, "span": 3, "now": 0},
            {"op": "la", "c": "c1", "s": "s", "key": "k", "now": 0},
            {"op": "qg", "scope": "B", "id": "a", "now": 1},
            {"op": "qg", "scope": "C", "id": "c1", "now": 4},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual((run_code, rec_code), (0, 0))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class V7FaultNormalizationTest(unittest.TestCase):
    """v7 faults 规范化：乱序提交按 a 升序输出，重叠判定不变。"""

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        self.assertEqual(code, 0)
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, ops, exit_code, label):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def seg(self, backend="a", k="D", a=0, z=10, v=0):
        return {"id": backend, "k": k, "a": a, "z": z, "v": v}

    def test_shuffled_segments_normalized_by_start(self):
        # 多后端、多段乱序提交：ce 按后端加入序、段 a 升序输出。
        faults = [
            self.seg("b", "S", 50, 60, 2),
            self.seg("a", "D", 20, 30),
            self.seg("b", "D", 0, 10),
            self.seg("a", "F", 0, 20, 5),
            self.seg("a", "D", 30, 40),
        ]
        config = config_v7(1, faults=faults)
        config["backends"].append(
            {"id": "b", "weight": 1, "d": 0, "fail": 3, "success": 2,
             "circuit": None, "drain": None, "endpoint": None}
        )
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "ce"},
        ])
        self.assertEqual(
            [(f["id"], f["k"], f["a"], f["z"], f["v"])
             for f in results[1]["config"]["faults"]],
            [("a", "F", 0, 20, 5), ("a", "D", 20, 30, 0),
             ("a", "D", 30, 40, 0), ("b", "D", 0, 10, 0),
             ("b", "S", 50, 60, 2)],
        )

    def test_adjacent_endpoints_accepted(self):
        # 相邻端点可接（z_i == a_{i+1}）。
        config = config_v7(1, faults=[
            self.seg("a", "D", 10, 20),
            self.seg("a", "D", 0, 10),
        ])
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "ce"},
        ])
        self.assertEqual(
            [f["a"] for f in results[1]["config"]["faults"]], [0, 10]
        )

    def test_overlap_and_same_start_rejected(self):
        for faults in (
            # 相交。
            [self.seg("a", "D", 0, 10), self.seg("a", "D", 5, 15)],
            # 同起点。
            [self.seg("a", "D", 0, 10), self.seg("a", "D", 0, 5)],
            # 乱序提交的重叠同样拒绝。
            [self.seg("a", "D", 5, 15), self.seg("a", "D", 0, 10)],
        ):
            self.assert_failure(
                [{"op": "ci", "config": config_v7(1, faults=faults),
                  "now": 0}],
                2, "INPUT",
            )


class OverloadHistoryTest(unittest.TestCase):
    """oh 过载分钟历史：oa/ot 记账、只读查询、保留窗与清空规则。"""

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        self.assertEqual(code, 0)
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, ops, exit_code, label):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def base_ops(self, ttl=10):
        # 环上唯一后端 a，cap=1 便于制造入队。
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 4, "ttl": ttl},
        ]

    def oa(self, cid, now):
        return {"op": "oa", "cid": cid, "flow": FLOW,
                "c": "k", "s": "k", "key": "k", "now": now}

    def test_oh_counts_peak_and_key_order(self):
        ops = self.base_ops() + [
            self.oa("c1", 0),   # A：immediate+1
            self.oa("c2", 0),   # Q：queued+1，peak=1
            self.oa("c3", 1),   # Q：queued+1，peak=2
            {"op": "oh", "from": 0, "to": 0, "now": 1},
        ]
        results = self.run_ops(ops)
        oh = results[6]
        self.assertEqual(list(oh), ["op", "windows"])
        self.assertEqual(len(oh["windows"]), 1)
        window = oh["windows"][0]
        self.assertEqual(
            list(window),
            ["window", "immediate", "queued", "dequeued", "expired", "peak"],
        )
        self.assertEqual(
            window,
            {"window": 0, "immediate": 1, "queued": 2,
             "dequeued": 0, "expired": 0, "peak": 2},
        )

    def test_oh_ot_dequeued_and_expired_in_ot_window(self):
        ops = self.base_ops() + [
            self.oa("c1", 0),               # A
            self.oa("c2", 0),               # Q
            self.oa("c3", 1),               # Q
            {"op": "close", "cid": "c1", "now": 2},
            {"op": "ot", "now": 5},         # c2 接纳（dequeued），c3 阻塞
            {"op": "ot", "now": 61},        # c3 过期（expired，归窗 1）
            {"op": "oh", "from": 0, "to": 1, "now": 61},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[7]["admitted"], ["c2"])
        self.assertEqual(results[8]["expired"], ["c3"])
        windows = results[9]["windows"]
        self.assertEqual(
            windows[0],
            {"window": 0, "immediate": 1, "queued": 2,
             "dequeued": 1, "expired": 0, "peak": 2},
        )
        self.assertEqual(
            windows[1],
            {"window": 1, "immediate": 0, "queued": 0,
             "dequeued": 0, "expired": 1, "peak": 0},
        )

    def test_oh_empty_windows_zero_and_readonly(self):
        ops = self.base_ops() + [
            self.oa("c1", 0),
            {"op": "oh", "from": 0, "to": 3, "now": 200},
            {"op": "oh", "from": 0, "to": 3, "now": 200},
        ]
        results = self.run_ops(ops)
        # 只读：两次查询逐字节一致。
        self.assertEqual(results[4], results[5])
        windows = results[4]["windows"]
        self.assertEqual([w["window"] for w in windows], [0, 1, 2, 3])
        self.assertEqual(
            windows[0],
            {"window": 0, "immediate": 1, "queued": 0,
             "dequeued": 0, "expired": 0, "peak": 0},
        )
        for window in windows[1:]:
            self.assertEqual(
                window,
                {"window": window["window"], "immediate": 0, "queued": 0,
                 "dequeued": 0, "expired": 0, "peak": 0},
            )

    def test_oh_unconfigured_os_is_state(self):
        self.assert_failure(
            [{"op": "oh", "from": 0, "to": 0, "now": 0}], 4, "STATE"
        )

    def test_oh_premature_from_is_state(self):
        # now=3600 时当前窗为 60，from 早于下界 1 报 STATE。
        self.assert_failure(
            self.base_ops() + [{"op": "oh", "from": 0, "to": 0, "now": 3600}],
            4, "STATE",
        )

    def test_oh_input_violations(self):
        bad_ops = [
            # 键序不符（须 op,from,to,now）。
            b'{"ops":[{"op":"oh","to":0,"from":0,"now":0}]}',
            # 缺键、多键。
            b'{"ops":[{"op":"oh","from":0,"to":0}]}',
            b'{"ops":[{"op":"oh","from":0,"to":0,"now":0,"x":1}]}',
            # bool 不是合法数值。
            b'{"ops":[{"op":"oh","from":true,"to":0,"now":0}]}',
            # 范围：now 超 10^9。
            b'{"ops":[{"op":"oh","from":0,"to":0,"now":1000000001}]}',
            # 关系：from>to、to-from>=60、to>now//60。
            b'{"ops":[{"op":"oh","from":2,"to":1,"now":180}]}',
            b'{"ops":[{"op":"oh","from":0,"to":60,"now":3600}]}',
            b'{"ops":[{"op":"oh","from":0,"to":2,"now":60}]}',
        ]
        for raw in bad_ops:
            code, stdout, stderr = run_balancer("run", raw)
            self.assertEqual(code, 2)
            self.assertEqual(stdout, b"")
            self.assertEqual(stderr, b'{"error":"INPUT"}\n')
        # 时钟倒退报 INPUT。
        self.assert_failure(
            self.base_ops() + [
                {"op": "oh", "from": 0, "to": 1, "now": 120},
                {"op": "oh", "from": 0, "to": 0, "now": 60},
            ],
            2, "INPUT",
        )

    def test_ci_cb_clear_history(self):
        config = config_v7(
            1, vnodes=1, overload={"cap": 1, "q": 4, "ttl": 100}
        )
        ops = self.base_ops(ttl=100) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "close", "cid": "c1", "now": 1},
            {"op": "oc", "cid": "c2"},
            {"op": "ci", "config": config, "now": 2},
            {"op": "oh", "from": 0, "to": 0, "now": 2},
        ]
        results = self.run_ops(ops)
        # ci 成功清空历史：窗 0 全 0。
        self.assertEqual(
            results[8]["windows"][0],
            {"window": 0, "immediate": 0, "queued": 0,
             "dequeued": 0, "expired": 0, "peak": 0},
        )
        # cb 成功同样清空历史。
        ops = [
            {"op": "ci", "config": config, "now": 0},
            self.oa("c1", 1),
            self.oa("c2", 1),
            {"op": "close", "cid": "c1", "now": 2},
            {"op": "oc", "cid": "c2"},
            {"op": "cb", "rev": 1, "now": 3},
            {"op": "oh", "from": 0, "to": 0, "now": 3},
        ]
        results = self.run_ops(ops)
        self.assertEqual(
            results[6]["windows"][0],
            {"window": 0, "immediate": 0, "queued": 0,
             "dequeued": 0, "expired": 0, "peak": 0},
        )

    def test_record_replay_covers_oh(self):
        ops = self.base_ops() + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "ot", "now": 200},
            {"op": "oh", "from": 0, "to": 3, "now": 200},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual((run_code, rec_code), (0, 0))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class RecordReplayTest(unittest.TestCase):
    """核心输入经 record、replay 逐字节复现退出码、stdout、stderr。"""

    def test_record_replay_round_trip(self):
        raw = core_input_bytes()
        run_code, run_stdout, run_stderr = run_balancer("run", raw)

        # record：无论底层成败均退出 0、stderr 为空，stdout 为一行记录。
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual(rec_code, 0)
        self.assertEqual(rec_stderr, b"")
        self.assertTrue(rec_stdout.endswith(b"\n"))
        self.assertEqual(rec_stdout.count(b"\n"), 1)

        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual(
            list(record), ["version", "stdin", "exit", "stdout", "stderr"]
        )
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["exit"], run_code)
        # 三个字节字段与直接 run 的结果逐字节一致。
        self.assertEqual(base64.b64decode(record["stdin"]), raw)
        self.assertEqual(base64.b64decode(record["stdout"]), run_stdout)
        self.assertEqual(base64.b64decode(record["stderr"]), run_stderr)

        # replay：逐字节复现记录的退出码、stdout、stderr。
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)


class OiDryRunTest(unittest.TestCase):
    """oi 只读接纳预演：fr 环序遍历，fault/slow/capacity/quota 判失败。"""

    FLOW = ["s", 1, "t", 2, "tcp"]

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        if err:
            self.assertEqual(code, 0, err)
        self.assertEqual(err, b"")
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, raw, exit_code, label):
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def setup(self, ids=("a", "b", "c"), vnodes=8):
        ops = [{"op": "add", "id": backend_id, "weight": 1}
               for backend_id in ids]
        ops.append({"op": "chash", "vnodes": vnodes})
        ops.append({"op": "os", "cap": 1, "q": 2, "ttl": 10})
        return ops

    def oi(self, **overrides):
        op = {
            "op": "oi", "key": "k1", "c": "c1", "s": "s1",
            "bc": 1, "cc": 1, "sc": 1,
            "timeout": 5, "max": 3, "now": 10,
        }
        op.update(overrides)
        return op

    def ring_order(self, ids, vnodes, key):
        """复刻 build_ring 自 key 哈希点的去重后端遍历序。"""
        tokens = []
        for join_index, backend_id in enumerate(ids):
            encoded = backend_id.encode("utf-8")
            for i in range(vnodes):
                digest = hashlib.sha256(
                    encoded + b"\x00" + str(i).encode("ascii")
                ).digest()
                tokens.append(
                    (int.from_bytes(digest, "big"), join_index, i, backend_id)
                )
        tokens.sort(key=lambda token: (token[0], token[1], token[2]))
        digests = [token[0] for token in tokens]
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash) % len(tokens)
        order = []
        for offset in range(len(tokens)):
            backend_id = tokens[(index + offset) % len(tokens)][3]
            if backend_id not in order:
                order.append(backend_id)
        return order

    def result(self, ops):
        return self.run_ops(ops)[-1]

    def test_success_on_first_candidate(self):
        result = self.result(self.setup() + [self.oi(now=0)])
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], "c")
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["latency"], 0)
        self.assertEqual(result["retries"], 0)
        self.assertEqual(result["remaps"], 0)
        self.assertEqual(
            [result[k] for k in
             ("fault", "slow", "capacity", "quota")],
            [0, 0, 0, 0],
        )

    def test_fault_then_accept(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "fs", "id": order[0], "k": "D",
                    "a": 0, "z": 100, "v": 0})
        ops.append(self.oi(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], order[1])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["retries"], 1)
        self.assertEqual(result["remaps"], 1)
        self.assertEqual(result["fault"], 1)
        self.assertEqual(result["latency"], 0)

    def test_all_faults_exhaust(self):
        ops = self.setup()
        for backend_id in ("a", "b", "c"):
            ops.append({"op": "fs", "id": backend_id, "k": "D",
                        "a": 0, "z": 100, "v": 0})
        ops.append(self.oi(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertIsNone(result["backend"])
        self.assertEqual(result["attempts"], 3)
        self.assertEqual(result["retries"], 2)
        self.assertEqual(result["remaps"], 2)
        self.assertEqual(result["fault"], 3)

    def test_slow_latency_is_timeout(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "fs", "id": order[0], "k": "S",
                    "a": 0, "z": 100, "v": 9})
        ops.append(self.oi(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["slow"], 1)
        # S 且 v>timeout：尝试耗时按 timeout 计。
        self.assertEqual(result["latency"], 5)

    def test_s_within_timeout_passes_with_v(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "fs", "id": order[0], "k": "S",
                    "a": 0, "z": 100, "v": 3})
        ops.append(self.oi(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], order[0])
        self.assertEqual(result["latency"], 3)

    def test_capacity_failure_latency_follows_s_rule(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        # fr max=1 成功必在 order[0] 建连，占满 os.cap=1。
        ops.append({"op": "fr", "cid": "x", "flow": self.FLOW,
                    "key": "k1", "timeout": 9, "max": 1, "now": 0})
        # 同候选处 S v=3≤timeout：capacity 失败但尝试耗时仍为 v。
        ops.append({"op": "fs", "id": order[0], "k": "S",
                    "a": 0, "z": 100, "v": 3})
        ops.append(self.oi(now=1))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], order[1])
        self.assertEqual(result["capacity"], 1)
        self.assertEqual(result["latency"], 3)

    def test_bucket_shortage_is_quota(self):
        ops = self.setup()
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 1, "now": 0})
        ops.append(self.oi(cc=2, now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertEqual(result["quota"], 3)
        self.assertEqual(result["attempts"], 3)

    def test_fixed_window_quota_shortage_is_quota(self):
        ops = self.setup()
        ops.append({"op": "qs", "scope": "C", "id": "c1",
                    "limit": 1, "span": 10, "now": 0})
        # la 在路由后端真实耗掉窗内唯一配额。
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k1",
                    "now": 0})
        ops.append(self.oi(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertEqual(result["quota"], 3)

    def test_priority_fault_over_quota(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 1, "now": 0})
        ops.append({"op": "fs", "id": order[0], "k": "D",
                    "a": 0, "z": 100, "v": 0})
        ops.append(self.oi(cc=2, now=0))
        result = self.result(ops)
        # 首候选 D 计 fault（不查桶），后两候选桶不足计 quota。
        self.assertEqual(result["fault"], 1)
        self.assertEqual(result["quota"], 2)

    def test_read_only_does_not_consume_or_connect_or_account(self):
        ops = self.setup()
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 1, "now": 0})
        ops.append(self.oi(cc=1, now=0))
        # 预演成功未耗令牌：随后真实 la 同成本仍成功。
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k1",
                    "now": 0})
        results = self.run_ops(ops)
        self.assertEqual(results[-2]["state"], "A")
        self.assertEqual(results[-1], {"op": "la", "backend": "c",
                                       "ok": True})
        # 预演不建连：cap=1 下随后 oa 仍直接 A。
        ops.append({"op": "oa", "cid": "x", "flow": self.FLOW,
                    "c": "c1", "s": "s1", "key": "k1", "now": 1})
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["state"], "A")
        # 预演访问故障段不记 fm。
        for backend_id in ("a", "b", "c"):
            ops.append({"op": "fs", "id": backend_id, "k": "D",
                        "a": 100, "z": 200, "v": 0})
        ops.append(self.oi(now=100))
        ops.append({"op": "fm", "id": "a"})
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["D"]["affected"], 0)

    def test_projected_refill_without_state_change(self):
        ops = self.setup()
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 2, "now": 0})
        # 真实耗空到 t=0（la 成本 2）。
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k1",
                    "bc": 1, "cc": 2, "sc": 1, "now": 0})
        ops.append(self.oi(cc=2, now=0))
        ops.append(self.oi(cc=2, now=1))
        results = self.run_ops(ops)
        # now=0 投影仍为 0：全部 quota 失败；now=1 投影补到 1 仍不足；
        # now=2 投影补到 2 通过。
        ops.append(self.oi(cc=2, now=2))
        results = self.run_ops(ops)
        self.assertEqual(results[-3]["state"], "R")
        self.assertEqual(results[-2]["state"], "R")
        self.assertEqual(results[-1]["state"], "A")

    def test_max_bounds_distinct_attempts(self):
        ops = self.setup()
        for backend_id in ("a", "b", "c"):
            ops.append({"op": "fs", "id": backend_id, "k": "D",
                        "a": 0, "z": 100, "v": 0})
        ops.append(self.oi(max=2, now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["fault"], 2)

    def test_clock_advances_and_regression_is_input(self):
        ops = self.setup()
        ops.append(self.oi(now=10))
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k1",
                    "now": 9})
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_state_errors_precede_clock(self):
        # 未 chash：STATE，即便 now 相对前序倒退（无前序，仍先于时钟）。
        ops = [{"op": "add", "id": "a", "weight": 1},
               {"op": "os", "cap": 1, "q": 2, "ttl": 10},
               self.oi()]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        # 已 chash 未 os：STATE。
        ops = [{"op": "add", "id": "a", "weight": 1},
               {"op": "chash", "vnodes": 1}, self.oi()]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        # 环内无合格候选（唯一后端不健康）：STATE。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 2, "ttl": 10},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 0},
            self.oi(now=0),
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_input_errors(self):
        setup = self.setup()

        def raw_of(op_obj):
            return encode_ops(setup + [op_obj])

        # 键序错误（c 先于 key）。
        self.assert_failure(
            b'{"ops":[{"op":"oi","c":"c1","key":"k1","s":"s1",'
            b'"bc":1,"cc":1,"sc":1,"timeout":5,"max":3,"now":0}]}',
            2, "INPUT",
        )
        # 三项成本全零。
        self.assert_failure(raw_of(self.oi(bc=0, cc=0, sc=0, now=0)),
                            2, "INPUT")
        # bool 混入 max。
        self.assert_failure(raw_of(self.oi(max=True, now=0)), 2, "INPUT")
        # timeout 越界。
        self.assert_failure(raw_of(self.oi(timeout=-1, now=0)),
                            2, "INPUT")
        # 多键。
        extra = self.oi(now=0)
        extra["x"] = 1
        self.assert_failure(raw_of(extra), 2, "INPUT")
        # 缺键。
        missing = self.oi(now=0)
        del missing["sc"]
        self.assert_failure(raw_of(missing), 2, "INPUT")
        # 空 key。
        self.assert_failure(raw_of(self.oi(key="", now=0)), 2, "INPUT")

    def test_exact_result_key_order_bytes(self):
        raw = encode_ops(self.setup() + [self.oi(now=0)])
        code, out, err = run_balancer("run", raw)
        self.assertEqual((code, err), (0, b""))
        line = next(line for line in out.split(b"\n")
                    if b'"op":"oi"' in line)
        expected = (
            b'{"op":"oi","state":"A","backend":"c","attempts":1,'
            b'"latency":0,"retries":0,"remaps":0,"fault":0,"slow":0,'
            b'"capacity":0,"quota":0}'
        )
        self.assertIn(expected, line)

    def test_record_replay_round_trip(self):
        raw = encode_ops(self.setup() + [self.oi(now=0)])
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual((run_code, rec_code), (0, 0))
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )

    def test_record_replay_covers_exhaustion(self):
        ops = self.setup()
        for backend_id in ("a", "b", "c"):
            ops.append({"op": "fs", "id": backend_id, "k": "D",
                        "a": 0, "z": 100, "v": 0})
        ops.append(self.oi(now=0))
        raw = encode_ops(ops)
        run_code, run_stdout, _ = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, b"")


if __name__ == "__main__":
    unittest.main()
