"""balancer.py 黑盒回归测试（仅用标准库）。

通过子进程运行 `python balancer.py run|record|replay`，stdin 发送 UTF-8
JSON，断言退出码、stdout、stderr。不修改被测程序的任何行为。
"""

import base64
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


class PoolSnapshotTest(unittest.TestCase):
    """全池增量快照 mo：seq 游标、增量基线、缓存重报与错误路径。"""

    def mo_ops(self):
        """两后端各记 mr 后连续两次 mo，再同参重报第二次。"""
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "add", "id": "b", "weight": 2},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 1, "remaps": 0, "now": 10},
            {"op": "mr", "id": "a", "ok": False, "ms": 2000,
             "retries": 0, "remaps": 2, "now": 20},
            {"op": "mr", "id": "b", "ok": True, "ms": 500,
             "retries": 0, "remaps": 0, "now": 30},
            {"op": "mo", "seq": 1, "now": 40},
            {"op": "mr", "id": "a", "ok": True, "ms": 50,
             "retries": 3, "remaps": 1, "now": 50},
            {"op": "mo", "seq": 2, "now": 59},
            {"op": "mo", "seq": 2, "now": 59},
        ]

    def test_increment_and_cached_replay(self):
        code, stdout, stderr = run_balancer("run", encode_ops(self.mo_ops()))
        self.assertEqual((code, stderr), (0, b""))
        results = json.loads(stdout.decode("utf-8"))["results"]
        snapshots = [r for r in results if r["op"] == "mo"]
        self.assertEqual(len(snapshots), 3)
        first, second, replayed = snapshots
        # 结果键序 op,seq,window,backends。
        self.assertEqual(list(first), ["op", "seq", "window", "backends"])
        self.assertEqual((first["seq"], first["window"]), (1, 0))
        # backends 按现存加入序；项键序
        # id,requests,qps,concurrency,errors,error_rate,latency,retries,
        # remaps,removed。
        self.assertEqual([b["id"] for b in first["backends"]], ["a", "b"])
        self.assertEqual(
            list(first["backends"][0]),
            ["id", "requests", "qps", "concurrency", "errors",
             "error_rate", "latency", "retries", "remaps", "removed"],
        )
        # 首次基线为零：全量当窗计数。
        self.assertEqual(
            first["backends"][0],
            {"id": "a", "requests": 2, "qps": "0.03", "concurrency": 0,
             "errors": 1, "error_rate": "50.00",
             "latency": [0, 1, 0, 0, 1], "retries": 1, "remaps": 2,
             "removed": None},
        )
        self.assertEqual(first["backends"][1]["requests"], 1)
        # 第二次为上次 mo 后的增量；未再报告的后端各项为 0。
        self.assertEqual(second["seq"], 2)
        self.assertEqual(
            second["backends"][0],
            {"id": "a", "requests": 1, "qps": "0.01", "concurrency": 0,
             "errors": 0, "error_rate": "0.00",
             "latency": [0, 0, 1, 0, 0], "retries": 3, "remaps": 1,
             "removed": None},
        )
        self.assertEqual(second["backends"][1]["requests"], 0)
        self.assertEqual(second["backends"][1]["qps"], "0.00")
        # 同 (seq,now) 重报返回缓存且不推进。
        self.assertEqual(replayed, second)

    def test_empty_pool_and_cross_window(self):
        # 空池 backends 为 []。
        code, stdout, _ = run_balancer(
            "run", encode_ops([{"op": "mo", "seq": 1, "now": 0}])
        )
        self.assertEqual(code, 0)
        result = json.loads(stdout.decode("utf-8"))["results"][0]
        self.assertEqual(result, {"op": "mo", "seq": 1, "window": 0,
                                  "backends": []})
        # 跨窗基线为零：窗 0 的计数不带入窗 1。
        code, stdout, _ = run_balancer("run", encode_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 10},
            {"op": "mo", "seq": 1, "now": 50},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 60},
            {"op": "mo", "seq": 2, "now": 60},
        ]))
        self.assertEqual(code, 0)
        result = json.loads(stdout.decode("utf-8"))["results"][-1]
        self.assertEqual(result["window"], 1)
        self.assertEqual(result["backends"][0]["requests"], 1)

    def test_readded_backend_counts_from_zero(self):
        code, stdout, _ = run_balancer("run", encode_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 10},
            {"op": "mo", "seq": 1, "now": 20},
            {"op": "remove", "id": "a"},
            {"op": "add", "id": "a", "weight": 1},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 30},
            {"op": "mo", "seq": 2, "now": 40},
        ]))
        self.assertEqual(code, 0)
        result = json.loads(stdout.decode("utf-8"))["results"][-1]
        # 同 id 重加按新实例从零计：仅重加后的 1 次。
        self.assertEqual(result["backends"][0]["requests"], 1)

    def test_ci_resets_cursor(self):
        config = {
            "version": 1,
            "backends": [{"id": "a", "weight": 1, "d": 0, "fail": 3,
                          "success": 2, "circuit": None, "drain": None}],
            "vnodes": None, "limits": [], "overload": None,
        }
        code, stdout, _ = run_balancer("run", encode_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 10},
            {"op": "mo", "seq": 1, "now": 20},
            {"op": "ci", "config": config, "now": 30},
            {"op": "mr", "id": "a", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 40},
            {"op": "mo", "seq": 1, "now": 50},
        ]))
        self.assertEqual(code, 0)
        result = json.loads(stdout.decode("utf-8"))["results"][-1]
        # ci 成功清游标：seq 重置为 1，基线随运行态归零。
        self.assertEqual(result["seq"], 1)
        self.assertEqual(result["backends"][0]["requests"], 1)

    def test_seq_errors_are_state(self):
        # 首次 seq!=1、跳号、同 seq 异 now、倒序均报 STATE/4。
        for ops in (
            [{"op": "mo", "seq": 2, "now": 0}],
            [{"op": "mo", "seq": 1, "now": 0},
             {"op": "mo", "seq": 3, "now": 1}],
            [{"op": "mo", "seq": 1, "now": 0},
             {"op": "mo", "seq": 1, "now": 1}],
            [{"op": "mo", "seq": 1, "now": 0},
             {"op": "mo", "seq": 2, "now": 1},
             {"op": "mo", "seq": 1, "now": 2}],
        ):
            code, stdout, stderr = run_balancer("run", encode_ops(ops))
            self.assertEqual((code, stdout), (4, b""))
            self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_invalid_input_and_clock_regression(self):
        # 键序错误：now 先于 seq 出现。
        raw = b'{"ops":[{"now":0,"seq":1,"op":"mo"}]}'
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual((code, stdout), (2, b""))
        self.assertEqual(stderr, b'{"error":"INPUT"}\n')
        # 类型、范围与时钟倒退均报 INPUT/2。
        for ops in (
            [{"op": "mo", "seq": 0, "now": 0}],
            [{"op": "mo", "seq": True, "now": 0}],
            [{"op": "mo", "seq": 10 ** 18 + 1, "now": 0}],
            [{"op": "mo", "seq": 1, "now": 10 ** 9 + 1}],
            [{"op": "mo", "seq": 1, "now": 5},
             {"op": "mo", "seq": 2, "now": 4}],
        ):
            code, stdout, stderr = run_balancer("run", encode_ops(ops))
            self.assertEqual((code, stdout), (2, b""))
            self.assertEqual(stderr, b'{"error":"INPUT"}\n')

    def test_failed_batch_rolls_back_cursor(self):
        # mo 成功后批内后续 op 失败：整批无输出，游标不生效。
        code, stdout, _ = run_balancer("run", encode_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "mo", "seq": 1, "now": 0},
            {"op": "mo", "seq": 3, "now": 1},
        ]))
        self.assertEqual((code, stdout), (4, b""))

    def test_record_replay_covers_mo(self):
        raw = encode_ops(self.mo_ops())
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual(rec_code, 0)
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)


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


if __name__ == "__main__":
    unittest.main()
