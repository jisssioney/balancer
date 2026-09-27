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


def config_v8(weight, faults=(), quotas=(), **overrides):
    """最小 version=8 配置：单后端 a，十键同 v7 且末置 quotas 数组。"""
    config = config_v7(weight, faults, **overrides)
    config["version"] = 8
    config["quotas"] = list(quotas)
    return config


def config_v9(weight, faults=(), quotas=(), dequeue="F", full="T", **overrides):
    """最小 version=9 配置：十一键同 v8 且末置 queue（dequeue,full）。"""
    config = config_v8(weight, faults, quotas, **overrides)
    config["version"] = 9
    config["queue"] = {"dequeue": dequeue, "full": full}
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

    def test_ci_commits_normalized_v9(self):
        # version=1 旧结构成功加载后，提交为规范化 version=9 配置
        # （faults 空计划、quotas 空数组、queue 默认 F/T）。
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
            config_v9(2),
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
        # 回滚后当前配置即 rev=1 的规范化 v9 快照（faults、quotas 均空，
        # queue 默认 F/T）。
        self.assertEqual(results[3]["config"], config_v9(1))

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


class ConfigPrecheckTest(unittest.TestCase):
    """配置预检 cv：只校验并回显规范化配置，不应用、不提交。"""

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        return code, out

    def test_cv_empty_state_applicable(self):
        code, out = self.run_ops(
            [{"op": "cv", "config": config_v9(2), "now": 5}]
        )
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][0]
        # 结果键序 op,applicable,connections,queued,config。
        self.assertEqual(
            list(result), ["op", "applicable", "connections", "queued", "config"]
        )
        self.assertEqual(result["applicable"], True)
        self.assertEqual(result["connections"], 0)
        self.assertEqual(result["queued"], 0)
        self.assertEqual(result["config"], config_v9(2))

    def test_cv_normalizes_v1_to_v9(self):
        # version=1 旧结构回显为规范化 version=9（同 ci 提交格式）。
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
            [{"op": "cv", "config": config_v1, "now": 0}]
        )
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][0]
        self.assertEqual(result["config"], config_v9(2))

    def test_cv_config_matches_ci_export(self):
        # 富 v9 配置：cv 回显与 ci 成功后 ce 导出逐字节同构。
        config = config_v9(
            2,
            faults=[
                {"id": "a", "k": "S", "a": 0, "z": 3, "v": 1},
                {"id": "a", "k": "D", "a": 3, "z": 4, "v": 0},
            ],
            quotas=[{"scope": "B", "id": "a", "limit": 100, "span": 60}],
            dequeue="S",
            full="H",
            vnodes=64,
            limits=[{"scope": "B", "id": "a", "r": 10, "b": 5}],
            overload={"cap": 3, "q": 4, "ttl": 10},
            sticky={"ttl": 100},
            idle={"ttl": 50},
            backpressure={"low": 1, "high": 4},
            scheduler={"pick": "H"},
        )
        code, out = self.run_ops([{"op": "cv", "config": config, "now": 7}])
        self.assertEqual(code, 0)
        cv_config = json.loads(out)["results"][0]["config"]
        code, out = self.run_ops(
            [{"op": "ci", "config": config, "now": 7}, {"op": "ce"}]
        )
        self.assertEqual(code, 0)
        ce_config = json.loads(out)["results"][1]["config"]
        self.assertEqual(cv_config, ce_config)

    def test_cv_does_not_apply_or_commit(self):
        # cv 后 ce 仍为空配置、cl 无提交、backends 为空；随后 ci 仍得 rev 1。
        ops = [
            {"op": "cv", "config": config_v9(2), "now": 0},
            {"op": "ce"},
            {"op": "cl"},
            {"op": "ci", "config": config_v6(1), "now": 1},
            {"op": "cl"},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)
        output = json.loads(out)
        # cv 后的 ce 仍为空配置：cv 未应用任何后端。
        ce_result = output["results"][1]
        self.assertEqual(ce_result["config"]["backends"], [])
        cl_result = output["results"][2]
        self.assertEqual(
            cl_result, {"op": "cl", "current": None, "commits": []}
        )
        final_cl = output["results"][4]
        self.assertEqual(final_cl["current"], 1)
        self.assertEqual(len(final_cl["commits"]), 1)

    def test_cv_with_active_connection_not_applicable(self):
        # 活动连接仅令 applicable=false，不报 STATE。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "cv", "config": config_v9(2), "now": 1},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][-1]
        self.assertEqual(result["applicable"], False)
        self.assertEqual(result["connections"], 1)
        self.assertEqual(result["queued"], 0)

    def test_cv_with_queued_item_not_applicable(self):
        ops = [
            {"op": "add", "id": "o", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 2, "ttl": 10},
            {"op": "open", "cid": "y", "flow": FLOW, "now": 0},
            {
                "op": "oa",
                "cid": "z",
                "flow": FLOW,
                "c": "k",
                "s": "k",
                "key": "k",
                "now": 0,
            },
            {"op": "cv", "config": config_v9(2), "now": 1},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)
        result = json.loads(out)["results"][-1]
        self.assertEqual(result["applicable"], False)
        self.assertEqual(result["connections"], 1)
        self.assertEqual(result["queued"], 1)

    def test_cv_unknown_backend_references_are_backend(self):
        # B 限流、B 配额、faults 引用未知后端均 BACKEND；有活动连接时
        # 仍优先报 BACKEND 而非受连接影响。
        bad_limit = config_v9(
            1, limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}]
        )
        bad_quota = config_v9(
            1, quotas=[{"scope": "B", "id": "ghost", "limit": 1, "span": 1}]
        )
        bad_fault = config_v9(
            1, faults=[{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}]
        )
        for bad in (bad_limit, bad_quota, bad_fault):
            code, out, err = run_balancer(
                "run", encode_ops([{"op": "cv", "config": bad, "now": 0}])
            )
            self.assertEqual((code, out, err), (3, b"", b'{"error":"BACKEND"}\n'))
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "cv", "config": bad_limit, "now": 1},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (3, b"", b'{"error":"BACKEND"}\n'))

    def test_cv_invalid_config_is_input(self):
        bad_version = config_v9(1)
        bad_version["version"] = 8
        missing = config_v9(1)
        del missing["scheduler"]
        for bad in (bad_version, missing, {"version": 9}, []):
            code, out, err = run_balancer(
                "run", encode_ops([{"op": "cv", "config": bad, "now": 0}])
            )
            self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))

    def test_cv_bad_now_is_input(self):
        for now in (-1, 10 ** 9 + 1, True, "0", 1.5, None):
            code, out, err = run_balancer(
                "run",
                encode_ops([{"op": "cv", "config": config_v9(1), "now": now}]),
            )
            self.assertEqual(
                (code, out, err), (2, b"", b'{"error":"INPUT"}\n'), now
            )

    def test_cv_exact_key_set(self):
        code, out, err = run_balancer(
            "run",
            encode_ops(
                [{"op": "cv", "config": config_v9(1), "now": 0, "x": 1}]
            ),
        )
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))
        code, out, err = run_balancer(
            "run", encode_ops([{"op": "cv", "config": config_v9(1)}])
        )
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))

    def test_cv_clock_regression_is_input(self):
        ops = [
            {"op": "cv", "config": config_v9(1), "now": 10},
            {"op": "cv", "config": config_v9(2), "now": 5},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))
        # 同刻不重拨：相等 now 合法。
        ops = [
            {"op": "cv", "config": config_v9(1), "now": 10},
            {"op": "cv", "config": config_v9(2), "now": 10},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)

    def test_cv_failure_rolls_back_batch(self):
        # 批内靠后的 cv 失败：整批无 stdout，前面成功的 cv 也不落任何状态。
        ops = [
            {"op": "cv", "config": config_v9(1), "now": 0},
            {"op": "cv", "config": config_v9(2), "now": 5},
            {"op": "cv", "config": config_v9(3), "now": 1},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))

    def test_record_replay_covers_cv(self):
        ops = [
            {"op": "cv", "config": config_v9(2), "now": 0},
            {"op": "ci", "config": config_v6(1), "now": 1},
            {"op": "cv", "config": config_v9(3), "now": 2},
        ]
        raw = encode_ops(ops)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, base64.b64decode(record["stderr"]))


def digest_of(config):
    """与实现同款：逐层键序紧凑 UTF-8 JSON（非 ASCII 不转义、无末尾
    换行）的 SHA-256 小写十六进制。"""
    canonical = json.dumps(
        config, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


class ConfigDigestTest(unittest.TestCase):
    """配置指纹 ct 与 ci 乐观并发（base）、cv 键序。"""

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

    def test_ct_empty_state_digest(self):
        results = self.run_ops([{"op": "ct"}])
        result = results[0]
        # 结果精确键序 op,digest。
        self.assertEqual(list(result), ["op", "digest"])
        empty_config = config_v9(1)
        empty_config["backends"] = []
        self.assertEqual(result["digest"], digest_of(empty_config))

    def test_ct_matches_ce_config(self):
        # ct 指纹即同一批内 ce.config 规范化 version=9 对象的摘要。
        ops = [
            {"op": "add", "id": "a", "weight": 2},
            {"op": "chash", "vnodes": 8},
            {"op": "ss", "ttl": 100},
            {"op": "ce"},
            {"op": "ct"},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[4]["digest"], digest_of(results[3]["config"]))

    def test_ct_read_only_and_no_clock(self):
        # ct 不推进时钟：夹在两个 now=5 的写操作之间不引入时钟倒退；
        # 不改变配置与提交历史。
        results = self.run_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 5},
            {"op": "ct"},
            {"op": "close", "cid": "x", "now": 5},
            {"op": "ce"},
            {"op": "cl"},
        ])
        self.assertEqual(len(results[4]["config"]["backends"]), 1)
        self.assertEqual(
            results[5], {"op": "cl", "current": None, "commits": []}
        )

    def test_ct_exact_key_set(self):
        self.assert_failure(
            encode_ops([{"op": "ct", "x": 1}]), 2, "INPUT"
        )

    def test_cv_out_of_order_keys_is_input(self):
        # cv 只接受按 op,config,now 出现的对象，乱序报 INPUT/2。
        self.assert_failure(
            encode_ops(
                [{"op": "cv", "now": 0, "config": config_v9(1)}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops(
                [{"config": config_v9(1), "op": "cv", "now": 0}]
            ),
            2, "INPUT",
        )

    def test_ci_base_success_and_output(self):
        # ct 取指纹后 ci 携带匹配 base：沿用全部成功语义与 op,ok=true 输出。
        results = self.run_ops([{"op": "ct"}])
        digest = results[0]["digest"]
        results = self.run_ops([
            {"op": "ci", "config": config_v9(2), "base": digest, "now": 0},
            {"op": "cl"},
        ])
        self.assertEqual(results[0], {"op": "ci", "ok": True})
        self.assertEqual(results[1]["current"], 1)

    def test_ci_base_stale_is_state(self):
        # 配置变更后旧指纹即过期：同一批内第二次携带旧 base 报 STATE/4。
        results = self.run_ops(
            [{"op": "ci", "config": config_v9(1), "now": 0}, {"op": "ct"}]
        )
        digest = results[1]["digest"]
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            {"op": "ci", "config": config_v9(2), "base": digest, "now": 1},
            {"op": "ci", "config": config_v9(3), "base": digest, "now": 2},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_ci_base_mismatch_is_state(self):
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v9(1), "base": "0" * 64,
                 "now": 0},
            ]),
            4, "STATE",
        )

    def test_ci_base_precedes_connection_check(self):
        # base 不等先于活动连接检查报 STATE；base 相等时活动连接仍报 STATE。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            {"op": "ci", "config": config_v9(1), "base": "0" * 64, "now": 1},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        results = self.run_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            {"op": "ct"},
        ])
        live_digest = results[-1]["digest"]
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            {"op": "ci", "config": config_v9(1), "base": live_digest,
             "now": 1},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_ci_base_backend_precedes_state(self):
        # 未知 B 限流引用仍报 BACKEND/3，先于 base 比较。
        bad = config_v9(
            1, limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}]
        )
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": bad, "base": "0" * 64, "now": 0},
            ]),
            3, "BACKEND",
        )

    def test_ci_base_format_is_input(self):
        # base 须为小写 64 位十六进制串：大写、长度、字符集、类型非法均
        # INPUT/2。
        for bad in ("A" * 64, "0" * 63, "0" * 65, "g" * 64, 0, None, True):
            self.assert_failure(
                encode_ops([
                    {"op": "ci", "config": config_v9(1), "base": bad,
                     "now": 0},
                ]),
                2, "INPUT",
            )

    def test_ci_base_exact_key_order(self):
        # 四键形式须精确按 op,config,base,now 出现，乱序报 INPUT/2。
        self.assert_failure(
            encode_ops([
                {"op": "ci", "base": "0" * 64, "config": config_v9(1),
                 "now": 0},
            ]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v9(1), "now": 0,
                 "base": "0" * 64},
            ]),
            2, "INPUT",
        )

    def test_ci_three_key_form_any_order(self):
        # 原三键形式保留：仅键集匹配，键序不限。
        results = self.run_ops(
            [{"now": 0, "op": "ci", "config": config_v9(1)}]
        )
        self.assertEqual(results[0], {"op": "ci", "ok": True})

    def test_ci_base_clock_regression_is_input(self):
        # base 形式的 now 沿用共用非递减时钟。
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 5},
            {"op": "ci", "config": config_v9(2), "base": "0" * 64, "now": 3},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_ci_base_failure_rolls_back_batch(self):
        # base 不等的 ci 失败：整批无 stdout，前面成功的 ci 也不落任何状态。
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            {"op": "ci", "config": config_v9(2), "base": "0" * 64, "now": 1},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_record_replay_covers_ct_and_base(self):
        results = self.run_ops([{"op": "ct"}])
        digest = results[0]["digest"]
        ops = [
            {"op": "ct"},
            {"op": "ci", "config": config_v9(1), "base": digest, "now": 0},
            {"op": "ct"},
        ]
        raw = encode_ops(ops)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, base64.b64decode(record["stderr"]))

    def test_ci_base_now_bound_is_input(self):
        # 乐观并发形式的 now 仅收 0..10^9 非 bool 整数：负数、越界、bool、
        # 字符串、浮点、None 一律 INPUT/2 且整批回滚。
        results = self.run_ops([{"op": "ct"}])
        digest = results[0]["digest"]
        for bad in (-1, 10 ** 9 + 1, True, "0", 1.5, None):
            self.assert_failure(
                encode_ops([
                    {"op": "ci", "config": config_v9(1), "base": digest,
                     "now": bad},
                ]),
                2, "INPUT",
            )
        # 上界 10^9 本身合法。
        results = self.run_ops([
            {"op": "ci", "config": config_v9(1), "base": digest,
             "now": 10 ** 9},
        ])
        self.assertEqual(results[0], {"op": "ci", "ok": True})

    def test_ci_base_bad_now_rolls_back_batch(self):
        # base 形式 now 非法：整批无 stdout，前面成功的 ci 也不落状态。
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            {"op": "ci", "config": config_v9(2), "base": "0" * 64,
             "now": 10 ** 9 + 1},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_ci_three_key_now_still_unbounded(self):
        # 原三键形式的 now 无上界（仅非负非 bool 整数），行为不变。
        results = self.run_ops(
            [{"op": "ci", "config": config_v9(1), "now": 10 ** 9 + 5}]
        )
        self.assertEqual(results[0], {"op": "ci", "ok": True})


class ConfigDiffTest(unittest.TestCase):
    """后端配置变更预览 cd：比较当前与候选规范化配置的 backends，不应用。"""

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

    def backend(self, bid, weight=1, d=0, fail=3, success=2, circuit=None,
                drain=None, endpoint=None):
        return {
            "id": bid,
            "weight": weight,
            "d": d,
            "fail": fail,
            "success": success,
            "circuit": circuit,
            "drain": drain,
            "endpoint": endpoint,
        }

    def make_config(self, backends, **overrides):
        # 以最小 v9 配置为底，整体替换 backends。
        config = config_v9(1, **overrides)
        config["backends"] = list(backends)
        return config

    def cd(self, backends, now=0, **overrides):
        return {"op": "cd", "config": self.make_config(backends, **overrides),
                "now": now}

    def test_cd_empty_state_result_shape(self):
        results = self.run_ops([{"op": "ct"}, self.cd([self.backend("a")])])
        result = results[1]
        # 精确结果键序 op,base,target,added,removed,changed,order。
        self.assertEqual(
            list(result),
            ["op", "base", "target", "added", "removed", "changed", "order"],
        )
        self.assertEqual(result["op"], "cd")
        # base 即同批 ct 对空配置的指纹；target 为候选规范化配置的指纹。
        self.assertEqual(result["base"], results[0]["digest"])
        empty = config_v9(1)
        empty["backends"] = []
        self.assertEqual(result["base"], digest_of(empty))
        self.assertEqual(result["target"], digest_of(self.make_config(
            [self.backend("a")]
        )))
        self.assertRegex(result["base"], r"^[0-9a-f]{64}$")
        self.assertEqual(result["added"], ["a"])
        self.assertEqual(result["removed"], [])
        self.assertEqual(result["changed"], [])
        self.assertEqual(result["order"], True)

    def test_cd_identical_config_is_empty_diff(self):
        config = self.make_config([self.backend("a", weight=2)])
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "ct"},
            self.cd([self.backend("a", weight=2)], now=1),
        ])
        result = results[2]
        self.assertEqual(result["base"], result["target"])
        self.assertEqual(result["base"], results[1]["digest"])
        self.assertEqual(result["added"], [])
        self.assertEqual(result["removed"], [])
        self.assertEqual(result["changed"], [])
        self.assertEqual(result["order"], False)

    def test_cd_added_and_removed_follow_each_side_order(self):
        current = [self.backend("a"), self.backend("x"), self.backend("b")]
        candidate = [
            self.backend("c"), self.backend("a"), self.backend("b"),
            self.backend("d"),
        ]
        results = self.run_ops([
            {"op": "ci", "config": self.make_config(current), "now": 0},
            self.cd(candidate, now=1),
        ])
        result = results[1]
        # added 按候选加入序（跳过共有 a/b），removed 按当前加入序。
        self.assertEqual(result["added"], ["c", "d"])
        self.assertEqual(result["removed"], ["x"])
        self.assertEqual(result["changed"], [])
        self.assertEqual(result["order"], True)

    def test_cd_changed_fields_in_fixed_order(self):
        circuit = {"n": 1, "m": 1, "r": 1, "w": 1, "q": 1}
        endpoint = {"host": "127.0.0.1", "port": 80}
        current = [self.backend("a"), self.backend("b", weight=4)]
        candidate = [
            self.backend(
                "a", weight=5, d=2, fail=4, success=6, circuit=circuit,
                drain=7, endpoint=endpoint,
            ),
            self.backend("b", weight=4),
        ]
        results = self.run_ops([
            {"op": "ci", "config": self.make_config(current), "now": 0},
            self.cd(candidate, now=1),
        ])
        result = results[1]
        self.assertEqual(result["added"], [])
        self.assertEqual(result["removed"], [])
        # changed 按候选序，仅共有且变化者；项键序 id,fields；fields 固定
        # 按 weight,d,fail,success,circuit,drain,endpoint 列差异。
        self.assertEqual(
            result["changed"],
            [{
                "id": "a",
                "fields": ["weight", "d", "fail", "success",
                           "circuit", "drain", "endpoint"],
            }],
        )
        self.assertEqual(list(result["changed"][0]), ["id", "fields"])
        self.assertEqual(result["order"], False)

    def test_cd_changed_field_subset_order_independent(self):
        # 仅 endpoint 与 weight 变化：仍按固定序输出，与登记顺序无关。
        current = [self.backend("a")]
        candidate = [self.backend(
            "a", weight=9, endpoint={"host": "10.0.0.1", "port": 443}
        )]
        results = self.run_ops([
            {"op": "ci", "config": self.make_config(current), "now": 0},
            self.cd(candidate, now=1),
        ])
        self.assertEqual(
            results[1]["changed"], [{"id": "a", "fields": ["weight", "endpoint"]}]
        )

    def test_cd_changed_follows_candidate_order_with_reversal(self):
        current = [self.backend("a", weight=1), self.backend("b", weight=1)]
        candidate = [self.backend("b", weight=8), self.backend("a", weight=9)]
        results = self.run_ops([
            {"op": "ci", "config": self.make_config(current), "now": 0},
            self.cd(candidate, now=1),
        ])
        result = results[1]
        # changed 按候选序：b 先于 a；同 id 集合仅顺序不同故 order=true。
        self.assertEqual(
            result["changed"],
            [
                {"id": "b", "fields": ["weight"]},
                {"id": "a", "fields": ["weight"]},
            ],
        )
        self.assertEqual(result["order"], True)

    def test_cd_pure_reorder_is_order_only(self):
        backends = [self.backend("a"), self.backend("b")]
        results = self.run_ops([
            {"op": "ci", "config": self.make_config(backends), "now": 0},
            self.cd([self.backend("b"), self.backend("a")], now=1),
        ])
        result = results[1]
        self.assertEqual(result["added"], [])
        self.assertEqual(result["removed"], [])
        self.assertEqual(result["changed"], [])
        self.assertEqual(result["order"], True)

    def test_cd_does_not_apply_or_commit(self):
        # cd 后 ce 仍为原配置、cl 无提交；再次 cd 看到的仍是旧现状。
        ops = [
            {"op": "ci", "config": self.make_config([self.backend("a")]),
             "now": 0},
            self.cd([self.backend("a"), self.backend("z")], now=1),
            {"op": "ce"},
            {"op": "cl"},
            self.cd([self.backend("a"), self.backend("z")], now=2),
        ]
        results = self.run_ops(ops)
        self.assertEqual(
            [b["id"] for b in results[2]["config"]["backends"]], ["a"]
        )
        # cd 不产生提交：cl 仍只有先前 ci 的 rev 1。
        self.assertEqual(results[3]["current"], 1)
        self.assertEqual(len(results[3]["commits"]), 1)
        # 候选未应用：z 在第二次 cd 中仍为 added。
        self.assertEqual(results[4]["added"], ["z"])

    def test_cd_active_connection_and_queue_are_not_errors(self):
        # 活动连接不报错：差异照常计算。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            self.cd([self.backend("a", weight=5), self.backend("b")], now=1),
        ]
        results = self.run_ops(ops)
        result = results[-1]
        self.assertEqual(result["added"], ["b"])
        self.assertEqual(
            result["changed"], [{"id": "a", "fields": ["weight"]}]
        )
        # 排队项同样不报错。
        ops = [
            {"op": "add", "id": "o", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 2, "ttl": 10},
            {"op": "open", "cid": "y", "flow": self.FLOW, "now": 0},
            {
                "op": "oa", "cid": "z", "flow": self.FLOW, "c": "k",
                "s": "k", "key": "k", "now": 0,
            },
            self.cd([self.backend("o"), self.backend("q")], now=1),
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["added"], ["q"])

    def test_cd_accepts_v1_and_normalizes(self):
        config_v1 = {
            "version": 1,
            "backends": [{
                "id": "a", "weight": 2, "d": 0, "fail": 3, "success": 2,
                "circuit": None, "drain": None,
            }],
            "vnodes": None,
            "limits": [],
            "overload": None,
        }
        results = self.run_ops([{"op": "cd", "config": config_v1, "now": 0}])
        result = results[0]
        # target 为规范化 v9 配置（endpoint=null、queue 默认 F/T）的指纹。
        normalized = config_v9(2)
        self.assertEqual(result["target"], digest_of(normalized))
        self.assertEqual(result["added"], ["a"])

    def test_cd_unknown_backend_references_are_backend(self):
        # 错误类型与优先级同 cv：B 限流、B 配额、faults 引用未知后端 BACKEND。
        bad_limit = self.make_config(
            [self.backend("a")],
            limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}],
        )
        bad_quota = self.make_config(
            [self.backend("a")],
            quotas=[{"scope": "B", "id": "ghost", "limit": 1, "span": 1}],
        )
        bad_fault = self.make_config(
            [self.backend("a")],
            faults=[{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}],
        )
        for bad in (bad_limit, bad_quota, bad_fault):
            self.assert_failure(
                encode_ops([{"op": "cd", "config": bad, "now": 0}]),
                3, "BACKEND",
            )

    def test_cd_invalid_config_is_input(self):
        bad_version = config_v9(1)
        bad_version["version"] = 8
        missing = config_v9(1)
        del missing["scheduler"]
        for bad in (bad_version, missing, {"version": 9}, []):
            self.assert_failure(
                encode_ops([{"op": "cd", "config": bad, "now": 0}]),
                2, "INPUT",
            )

    def test_cd_bad_now_is_input(self):
        for bad in (-1, 10 ** 9 + 1, True, "0", 1.5, None):
            self.assert_failure(
                encode_ops([{"op": "cd", "config": config_v9(1), "now": bad}]),
                2, "INPUT",
            )

    def test_cd_now_boundary_values_are_ok(self):
        results = self.run_ops([
            {"op": "cd", "config": config_v9(1), "now": 0},
            {"op": "cd", "config": config_v9(2), "now": 10 ** 9},
        ])
        self.assertEqual(len(results), 2)

    def test_cd_exact_key_order(self):
        # 精确键序 op,config,now：乱序、多键、缺键均 INPUT。
        self.assert_failure(
            encode_ops(
                [{"op": "cd", "now": 0, "config": config_v9(1)}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops(
                [{"config": config_v9(1), "op": "cd", "now": 0}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops(
                [{"op": "cd", "config": config_v9(1), "now": 0, "x": 1}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "cd", "config": config_v9(1)}]),
            2, "INPUT",
        )

    def test_cd_clock_regression_is_input_and_advances_clock(self):
        # 时钟倒退 INPUT/2；相等 now 合法。
        self.assert_failure(
            encode_ops([
                {"op": "cd", "config": config_v9(1), "now": 10},
                {"op": "cd", "config": config_v9(2), "now": 5},
            ]),
            2, "INPUT",
        )
        results = self.run_ops([
            {"op": "cd", "config": config_v9(1), "now": 10},
            {"op": "cd", "config": config_v9(2), "now": 10},
        ])
        self.assertEqual(len(results), 2)
        # cd 成功推进共用时钟：其后旧时刻的其他操作报倒退。
        self.assert_failure(
            encode_ops([
                {"op": "cd", "config": config_v9(1), "now": 10},
                {"op": "cv", "config": config_v9(2), "now": 9},
            ]),
            2, "INPUT",
        )

    def test_cd_failure_rolls_back_batch(self):
        # 批内靠后的 cd 失败：整批无 stdout，前面成功的 ci 也不落任何状态。
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            {"op": "cd", "config": config_v9(2), "now": 1},
            {"op": "cd", "config": config_v9(3), "now": "x"},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_record_replay_covers_cd(self):
        ops = [
            {"op": "ci", "config": self.make_config([self.backend("a")]),
             "now": 0},
            self.cd([self.backend("a", weight=7), self.backend("b")], now=1),
            {"op": "ct"},
        ]
        raw = encode_ops(ops)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, base64.b64decode(record["stderr"]))


class PolicyDiffTest(unittest.TestCase):
    """策略配置差异预览 pd：比较当前与候选规范化配置的非 backends 部分。"""

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

    def pd(self, config, now=0):
        return {"op": "pd", "config": config, "now": now}

    def test_pd_result_shape_and_digests(self):
        results = self.run_ops([{"op": "ct"}, self.pd(config_v9(1))])
        result = results[1]
        # 精确结果键序 op,base,target,changes。
        self.assertEqual(list(result), ["op", "base", "target", "changes"])
        self.assertEqual(result["op"], "pd")
        # base 即同批 ct 对当前（空）配置的指纹；target 为候选规范化指纹。
        self.assertEqual(result["base"], results[0]["digest"])
        empty = config_v9(1)
        empty["backends"] = []
        self.assertEqual(result["base"], digest_of(empty))
        self.assertEqual(result["target"], digest_of(config_v9(1)))
        self.assertRegex(result["target"], r"^[0-9a-f]{64}$")
        # 仅 backends 不同：changes 为空。
        self.assertEqual(result["changes"], [])

    def test_pd_identical_config_is_empty_diff(self):
        results = self.run_ops([
            {"op": "ci", "config": config_v9(2), "now": 0},
            {"op": "ct"},
            self.pd(config_v9(2), now=1),
        ])
        result = results[2]
        self.assertEqual(result["base"], result["target"])
        self.assertEqual(result["base"], results[1]["digest"])
        self.assertEqual(result["changes"], [])

    def test_pd_sections_in_fixed_order(self):
        # 候选同时改 vnodes/limits/overload/sticky/idle/backpressure/
        # scheduler/faults/quotas/queue：changes 按固定节序列出，与配置
        # 内登记先后无关；项键序 section,before,after。
        candidate = config_v9(
            1,
            faults=[{"id": "a", "k": "D", "a": 0, "z": 10, "v": 0}],
            quotas=[{"scope": "C", "id": "c", "limit": 5, "span": 60}],
            dequeue="S",
            vnodes=64,
            limits=[{"scope": "S", "id": "s", "r": 2, "b": 3}],
            overload={"cap": 4, "q": 8, "ttl": 30},
            sticky={"ttl": 10},
            idle={"ttl": 20},
            backpressure={"low": 1, "high": 2},
            scheduler={"pick": "L"},
        )
        results = self.run_ops([self.pd(candidate)])
        changes = results[0]["changes"]
        self.assertEqual(
            [item["section"] for item in changes],
            [
                "vnodes", "limits", "overload", "sticky", "idle",
                "backpressure", "scheduler", "faults", "quotas", "queue",
            ],
        )
        for item in changes:
            self.assertEqual(list(item), ["section", "before", "after"])
        by_section = {item["section"]: item for item in changes}
        self.assertEqual(
            by_section["vnodes"], {"section": "vnodes", "before": None, "after": 64}
        )
        self.assertEqual(
            by_section["limits"]["after"],
            [{"scope": "S", "id": "s", "r": 2, "b": 3}],
        )
        self.assertEqual(by_section["overload"]["before"], None)
        self.assertEqual(
            by_section["overload"]["after"], {"cap": 4, "q": 8, "ttl": 30}
        )
        self.assertEqual(by_section["sticky"]["after"], {"ttl": 10})
        self.assertEqual(by_section["idle"]["after"], {"ttl": 20})
        self.assertEqual(
            by_section["backpressure"]["after"], {"low": 1, "high": 2}
        )
        self.assertEqual(
            by_section["scheduler"],
            {"section": "scheduler",
             "before": {"pick": "W"}, "after": {"pick": "L"}},
        )
        self.assertEqual(
            by_section["faults"]["after"],
            [{"id": "a", "k": "D", "a": 0, "z": 10, "v": 0}],
        )
        self.assertEqual(
            by_section["quotas"]["after"],
            [{"scope": "C", "id": "c", "limit": 5, "span": 60}],
        )
        self.assertEqual(
            by_section["queue"],
            {"section": "queue",
             "before": {"dequeue": "F", "full": "T"},
             "after": {"dequeue": "S", "full": "T"}},
        )

    def test_pd_before_reflects_current_config(self):
        # before 取自当前 ce 配置（含运行期 qp/rp 修改后的登记值）。
        ops = [
            {"op": "ci", "config": config_v9(1, vnodes=32), "now": 0},
            {"op": "qp", "mode": "S"},
            self.pd(config_v9(1, vnodes=32), now=1),
        ]
        results = self.run_ops(ops)
        changes = results[-1]["changes"]
        self.assertEqual(
            changes,
            [{
                "section": "queue",
                "before": {"dequeue": "S", "full": "T"},
                "after": {"dequeue": "F", "full": "T"},
            }],
        )

    def test_pd_backends_only_change_is_empty_changes(self):
        # 仅 backends 变化（含增删与字段变更）：changes 为空，base/target
        # 仍为两份完整配置的指纹。
        current = config_v9(1)
        candidate = config_v9(9)
        candidate["backends"].append(
            {"id": "b", "weight": 1, "d": 0, "fail": 3, "success": 2,
             "circuit": None, "drain": None, "endpoint": None}
        )
        results = self.run_ops([
            {"op": "ci", "config": current, "now": 0},
            self.pd(candidate, now=1),
        ])
        result = results[1]
        self.assertEqual(result["changes"], [])
        self.assertEqual(result["base"], digest_of(current))
        self.assertEqual(result["target"], digest_of(candidate))

    def test_pd_does_not_apply_or_commit(self):
        # pd 后 ce 仍为原配置、cl 无提交；再次 pd 看到的仍是旧现状。
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            self.pd(config_v9(1, vnodes=64), now=1),
            {"op": "ce"},
            {"op": "cl"},
            self.pd(config_v9(1, vnodes=64), now=2),
        ]
        results = self.run_ops(ops)
        self.assertIsNone(results[2]["config"]["vnodes"])
        self.assertEqual(results[3]["current"], 1)
        self.assertEqual(len(results[3]["commits"]), 1)
        # 候选未应用：第二次 pd 的 before 仍是 null。
        self.assertEqual(
            results[4]["changes"],
            [{"section": "vnodes", "before": None, "after": 64}],
        )

    def test_pd_active_connection_and_queue_are_not_errors(self):
        # 活动连接不报错：差异照常计算。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            self.pd(config_v9(1, vnodes=8), now=1),
        ]
        results = self.run_ops(ops)
        self.assertEqual(
            results[-1]["changes"],
            [{"section": "vnodes", "before": None, "after": 8}],
        )
        # 排队项同样不报错。
        ops = [
            {"op": "add", "id": "o", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 2, "ttl": 10},
            {"op": "open", "cid": "y", "flow": self.FLOW, "now": 0},
            {
                "op": "oa", "cid": "z", "flow": self.FLOW, "c": "k",
                "s": "k", "key": "k", "now": 0,
            },
            self.pd(config_v9(1, idle={"ttl": 5}), now=1),
        ]
        results = self.run_ops(ops)
        # chash/os 的登记值随 ce 导出，与候选差异一并按节序列出。
        self.assertEqual(
            results[-1]["changes"],
            [
                {"section": "vnodes", "before": 1, "after": None},
                {"section": "overload",
                 "before": {"cap": 1, "q": 2, "ttl": 10}, "after": None},
                {"section": "idle", "before": None, "after": {"ttl": 5}},
            ],
        )

    def test_pd_accepts_v1_and_normalizes(self):
        # v1 候选规范化为 v9：target 为规范化指纹，queue 默认 F/T 不差异。
        config_v1 = {
            "version": 1,
            "backends": [{
                "id": "a", "weight": 2, "d": 0, "fail": 3, "success": 2,
                "circuit": None, "drain": None,
            }],
            "vnodes": None,
            "limits": [],
            "overload": None,
        }
        results = self.run_ops([self.pd(config_v1)])
        result = results[0]
        self.assertEqual(result["target"], digest_of(config_v9(2)))
        self.assertEqual(result["changes"], [])

    def test_pd_unknown_backend_references_are_backend(self):
        # 错误类型与优先级同 cv：B 限流、B 配额、faults 引用未知候选后端。
        bad_limit = config_v9(
            1, limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}]
        )
        bad_quota = config_v9(
            1, quotas=[{"scope": "B", "id": "ghost", "limit": 1, "span": 1}]
        )
        bad_fault = config_v9(
            1, faults=[{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}]
        )
        for bad in (bad_limit, bad_quota, bad_fault):
            self.assert_failure(
                encode_ops([self.pd(bad)]),
                3, "BACKEND",
            )

    def test_pd_invalid_config_is_input(self):
        bad_version = config_v9(1)
        bad_version["version"] = 8
        missing = config_v9(1)
        del missing["scheduler"]
        for bad in (bad_version, missing, {"version": 9}, []):
            self.assert_failure(
                encode_ops([self.pd(bad)]),
                2, "INPUT",
            )

    def test_pd_bad_now_is_input(self):
        for bad in (-1, 10 ** 9 + 1, True, "0", 1.5, None):
            self.assert_failure(
                encode_ops([self.pd(config_v9(1), now=bad)]),
                2, "INPUT",
            )

    def test_pd_now_boundary_values_are_ok(self):
        results = self.run_ops([
            self.pd(config_v9(1), now=0),
            self.pd(config_v9(2), now=10 ** 9),
        ])
        self.assertEqual(len(results), 2)

    def test_pd_exact_key_order(self):
        # 精确键序 op,config,now：乱序、多键、缺键均 INPUT。
        self.assert_failure(
            encode_ops([{"op": "pd", "now": 0, "config": config_v9(1)}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"config": config_v9(1), "op": "pd", "now": 0}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "pd", "config": config_v9(1), "now": 0, "x": 1}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "pd", "config": config_v9(1)}]),
            2, "INPUT",
        )

    def test_pd_clock_regression_is_input_and_advances_clock(self):
        # 时钟倒退 INPUT/2；相等 now 合法。
        self.assert_failure(
            encode_ops([
                self.pd(config_v9(1), now=10),
                self.pd(config_v9(2), now=5),
            ]),
            2, "INPUT",
        )
        results = self.run_ops([
            self.pd(config_v9(1), now=10),
            self.pd(config_v9(2), now=10),
        ])
        self.assertEqual(len(results), 2)
        # pd 成功推进共用时钟：其后旧时刻的其他操作报倒退。
        self.assert_failure(
            encode_ops([
                self.pd(config_v9(1), now=10),
                {"op": "cv", "config": config_v9(2), "now": 9},
            ]),
            2, "INPUT",
        )

    def test_pd_failure_rolls_back_batch(self):
        # 批内靠后的 pd 失败：整批无 stdout，前面成功的 ci 也不落任何状态。
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            self.pd(config_v9(2), now=1),
            self.pd(config_v9(3), now="x"),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_record_replay_covers_pd(self):
        ops = [
            {"op": "ci", "config": config_v9(1), "now": 0},
            self.pd(config_v9(1, vnodes=16, scheduler={"pick": "R"}), now=1),
            {"op": "ct"},
        ]
        raw = encode_ops(ops)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, base64.b64decode(record["stderr"]))
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

    def test_ce_exports_v9_with_faults_quotas_and_queue_last(self):
        results = self.run_ops([{"op": "add", "id": "a", "weight": 1},
                                {"op": "ce"}])
        config = results[-1]["config"]
        self.assertEqual(
            list(config),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue"],
        )
        self.assertEqual(config["version"], 9)
        self.assertEqual(config["faults"], [])
        self.assertEqual(config["quotas"], [])
        # queue 精确键序 dequeue,full，默认 F/T；不含 evicted、last。
        self.assertEqual(list(config["queue"]), ["dequeue", "full"])
        self.assertEqual(config["queue"], {"dequeue": "F", "full": "T"})

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
        self.assertIn(b'"version":9', run_stdout)


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

    def test_ci_v7_clears_quotas_and_ce_exports_quotas(self):
        ops = self.base_ops() + [
            {"op": "qs", "scope": "C", "id": "c",
             "limit": 1, "span": 1, "now": 0},
            {"op": "ci", "config": config_v7(1), "now": 1},
            {"op": "ce"},
        ]
        results = self.run_ops(ops)
        # ce 导出 version=9：精确十二键，queue 末置；v1..v7 热加载视
        # quotas=[]、queue 为默认 F/T，故 quotas 为空。
        self.assertEqual(
            list(results[4]["config"]),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue"],
        )
        self.assertEqual(results[4]["config"]["version"], 9)
        self.assertEqual(results[4]["config"]["quotas"], [])
        self.assertEqual(
            results[4]["config"]["queue"], {"dequeue": "F", "full": "T"}
        )
        # ci（v7）成功后配额已清空。
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


class QuotaHotReloadTest(unittest.TestCase):
    """version=8 quotas 固定窗口配额纳入 ce/ci/cl/cb 热加载与回滚。"""

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

    def quota(self, scope="C", qid="c", limit=5, span=10):
        return {"scope": scope, "id": qid, "limit": limit, "span": span}

    def test_ce_exports_quotas_sorted_without_runtime(self):
        results = self.run_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "qs", "scope": "S", "id": "s", "limit": 7, "span": 3,
             "now": 0},
            {"op": "qs", "scope": "C", "id": "b", "limit": 2, "span": 4,
             "now": 0},
            {"op": "qs", "scope": "B", "id": "a", "limit": 9, "span": 5,
             "now": 0},
            {"op": "qs", "scope": "C", "id": "a", "limit": 1, "span": 2,
             "now": 0},
            {"op": "ce"},
        ])
        config = results[-1]["config"]
        self.assertEqual(config["version"], 9)
        # queue 末置、键序 dequeue,full，默认 F/T。
        self.assertEqual(list(config), [
            "version", "backends", "vnodes", "limits", "overload", "sticky",
            "idle", "backpressure", "scheduler", "faults", "quotas", "queue",
        ])
        self.assertEqual(config["queue"], {"dequeue": "F", "full": "T"})
        # 按 scope 的 B/C/S 序、id 的 UTF-8 字节升序；项键序
        # scope,id,limit,span，不含 window、used。
        self.assertEqual(
            config["quotas"],
            [self.quota("B", "a", 9, 5), self.quota("C", "a", 1, 2),
             self.quota("C", "b", 2, 4), self.quota("S", "s", 7, 3)],
        )
        for item in config["quotas"]:
            self.assertEqual(list(item), ["scope", "id", "limit", "span"])

    def test_ci_v8_loads_quotas_with_fresh_window(self):
        config = config_v8(1, quotas=[self.quota("C", "c", 5, 10)])
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 25},
            {"op": "qg", "scope": "C", "id": "c", "now": 25},
        ])
        # 载入即置 window=now//span、used=0。
        self.assertEqual(
            results[1],
            {"op": "qg", "scope": "C", "id": "c", "limit": 5, "span": 10,
             "window": 2, "used": 0, "remaining": 5},
        )

    def test_ci_v8_quota_replaces_used_state(self):
        # 热加载原子替换：旧配额的已用量不保留。
        config = config_v8(1, quotas=[self.quota("C", "c1", 1, 100)])
        results = self.run_ops([
            {"op": "ci", "config": config, "now": 0},
            {"op": "chash", "vnodes": 4},
            {"op": "la", "c": "c1", "s": "s", "key": "k", "now": 1},
            {"op": "qg", "scope": "C", "id": "c1", "now": 1},
            {"op": "ci", "config": config, "now": 2},
            {"op": "qg", "scope": "C", "id": "c1", "now": 2},
        ])
        self.assertEqual(results[3]["used"], 1)
        self.assertEqual(results[5]["used"], 0)

    def test_v8_requires_exact_quotas_key(self):
        # 十一键结构但 version=7。
        bad_version = config_v8(1)
        bad_version["version"] = 7
        self.assert_failure(
            encode_ops([{"op": "ci", "config": bad_version, "now": 0}]),
            2, "INPUT",
        )
        # version=8 缺 quotas 键（十键）。
        missing = config_v7(1)
        missing["version"] = 8
        self.assert_failure(
            encode_ops([{"op": "ci", "config": missing, "now": 0}]),
            2, "INPUT",
        )

    def test_quotas_validation_errors(self):
        cases = [
            # 重复 (scope,id)。
            dict(quotas=[self.quota(), self.quota()]),
            # scope 越界。
            dict(quotas=[self.quota(scope="X")]),
            # id 空串。
            dict(quotas=[self.quota(qid="")]),
            # id 非字符串。
            dict(quotas=[self.quota(qid=1)]),
            # limit 越界（0、10^18+1、bool）。
            dict(quotas=[self.quota(limit=0)]),
            dict(quotas=[self.quota(limit=10 ** 18 + 1)]),
            dict(quotas=[self.quota(limit=True)]),
            # span 越界（0、10^9+1、bool）。
            dict(quotas=[self.quota(span=0)]),
            dict(quotas=[self.quota(span=10 ** 9 + 1)]),
            dict(quotas=[self.quota(span=False)]),
            # 顺序非法：S 在 B 前。
            dict(quotas=[self.quota("S", "s"), self.quota("B", "a")]),
            # 顺序非法：同 scope 内 id 字节降序。
            dict(quotas=[self.quota("C", "b"), self.quota("C", "a")]),
            # 项含 window 等多余键。
            dict(quotas=[dict(self.quota(), window=0)]),
            # 项缺键。
            dict(quotas=[{"scope": "C", "id": "c", "limit": 1}]),
        ]
        for override in cases:
            self.assert_failure(
                encode_ops([{"op": "ci", "config": config_v8(1, **override),
                             "now": 0}]),
                2, "INPUT",
            )
        # quotas 非数组。
        not_list = config_v8(1)
        not_list["quotas"] = {}
        self.assert_failure(
            encode_ops([{"op": "ci", "config": not_list, "now": 0}]),
            2, "INPUT",
        )
        # 项键序错误（原始 JSON 构造）。
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":'
            + self._raw_v8(
                quotas='[{"scope":"C","limit":1,"id":"c","span":10}]'
            )
            + b"}]}"
        )
        self.assert_failure(raw, 2, "INPUT")
        # id 非 UTF-8（孤立代理）。
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":'
            + self._raw_v8(
                quotas='[{"scope":"C","id":"\\ud800","limit":1,"span":10}]'
            )
            + b"}]}"
        )
        self.assert_failure(raw, 2, "INPUT")

    def test_unknown_quota_backend_is_backend(self):
        config = config_v8(1, quotas=[self.quota("B", "ghost")])
        self.assert_failure(
            encode_ops([{"op": "ci", "config": config, "now": 0}]),
            3, "BACKEND",
        )
        # INPUT 先于 BACKEND：未知 id 但 limit 非法仍判 INPUT。
        bad = config_v8(1, quotas=[self.quota("B", "ghost", limit=0)])
        self.assert_failure(
            encode_ops([{"op": "ci", "config": bad, "now": 0}]),
            2, "INPUT",
        )

    def test_active_connection_is_state(self):
        config = config_v8(1, quotas=[self.quota("C", "c")])
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v8(1), "now": 0},
                {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
                {"op": "ci", "config": config, "now": 2},
            ]),
            4, "STATE",
        )

    def test_failed_ci_batch_is_atomic(self):
        # 失败批原子回滚：同一批内先前的成功 ci（含 quotas 载入）与新 rev
        # 分配均不落盘，整批无 stdout。
        good = config_v8(1, quotas=[self.quota("C", "c", 5, 10)])
        bad = config_v8(1, quotas=[self.quota("B", "ghost")])
        code, out, err = run_balancer(
            "run",
            encode_ops([
                {"op": "ci", "config": good, "now": 5},
                {"op": "qg", "scope": "C", "id": "c", "now": 5},
                {"op": "ci", "config": bad, "now": 6},
            ]),
        )
        self.assertEqual((code, out, err), (3, b"", b'{"error":"BACKEND"}\n'))

    def test_cb_restores_target_quotas_with_fresh_window(self):
        with_quota = config_v8(1, quotas=[self.quota("C", "c", 5, 10)])
        results = self.run_ops([
            {"op": "ci", "config": with_quota, "now": 0},
            {"op": "ci", "config": config_v8(1), "now": 1},
            {"op": "cb", "rev": 1, "now": 25},
            {"op": "qg", "scope": "C", "id": "c", "now": 25},
            {"op": "cl"},
        ])
        self.assertEqual(
            results[2], {"op": "cb", "target": 1, "rev": 3, "ok": True}
        )
        # 以 cb.now 重置 window、used。
        self.assertEqual(
            (results[3]["window"], results[3]["used"]), (2, 0)
        )
        commits = results[4]["commits"]
        self.assertEqual(
            commits[0]["config"]["quotas"], [self.quota("C", "c", 5, 10)]
        )
        self.assertEqual(commits[1]["config"]["quotas"], [])
        self.assertEqual(
            commits[2]["config"]["quotas"], [self.quota("C", "c", 5, 10)]
        )
        self.assertEqual(commits[2]["config"]["version"], 9)
        # cl/cb 快照一律规范化为 version=9，queue 默认 F/T。
        for commit in commits:
            self.assertEqual(commit["config"]["version"], 9)
            self.assertEqual(
                commit["config"]["queue"], {"dequeue": "F", "full": "T"}
            )

    def test_record_replay_covers_v8_quotas(self):
        config = config_v8(
            1, quotas=[self.quota("B", "a", 9, 5), self.quota("C", "c", 5, 10)]
        )
        ops = [
            {"op": "ci", "config": config, "now": 7},
            {"op": "qg", "scope": "B", "id": "a", "now": 7},
            {"op": "ce"},
            {"op": "cl"},
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
        self.assertEqual(run_stdout.count(b"\n"), 1)
        self.assertIn(b'"version":9', run_stdout)
        self.assertIn(b'"quotas":[{', run_stdout)

    @staticmethod
    def _raw_v8(**fields):
        # 构造一个嵌入 quotas 原始 JSON 片段的 v8 config 字节。
        parts = [
            '"version":8',
            '"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            '"circuit":null,"drain":null,"endpoint":null}]',
            '"vnodes":null', '"limits":[]', '"overload":null',
            '"sticky":null', '"idle":null', '"backpressure":null',
            '"scheduler":{"pick":"W"}', '"faults":[]',
        ]
        for key, value in fields.items():
            parts.append('"%s":%s' % (key, value))
        return ("{" + ",".join(parts) + "}").encode("utf-8")


class V9QueueHotReloadTest(unittest.TestCase):
    """version=9 queue（dequeue,full）纳入 ce/ci/cl/cb 热加载与回滚。"""

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

    def oa(self, cid, now, sc=None):
        op = {"op": "oa", "cid": cid, "flow": self.FLOW,
              "c": "c1", "s": "s1", "key": "k" + cid, "now": now}
        if sc is not None:
            op.update({"bc": 0, "cc": 0, "sc": sc})
        return op

    def config(self, dequeue="F", full="T", cap=10, q=4, ttl=1000,
               limits=(), vnodes=1):
        return config_v9(
            1,
            quotas=(),
            dequeue=dequeue,
            full=full,
            vnodes=vnodes,
            limits=list(limits),
            overload={"cap": cap, "q": q, "ttl": ttl},
        )

    # ---- 导出与默认 ----

    def test_ce_twelve_keys_queue_last_with_default_ft(self):
        results = self.run_ops([
            {"op": "ci", "config": config_v8(1), "now": 0},
            {"op": "ce"},
        ])
        config = results[1]["config"]
        self.assertEqual(
            list(config),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue"],
        )
        self.assertEqual(config["version"], 9)
        self.assertEqual(list(config["queue"]), ["dequeue", "full"])
        self.assertEqual(config["queue"], {"dequeue": "F", "full": "T"})

    def test_v1_through_v8_default_to_ft(self):
        be7 = [{
            "id": "a", "weight": 1, "d": 0, "fail": 3,
            "success": 2, "circuit": None, "drain": None,
        }]
        be8 = [dict(be7[0], endpoint=None)]
        for version in (1, 2, 3, 4, 5, 6, 7, 8):
            if version == 1:
                cfg = {
                    "version": 1, "backends": be7, "vnodes": None,
                    "limits": [], "overload": None,
                }
            elif version == 2:
                cfg = {
                    "version": 2, "backends": be7, "vnodes": None,
                    "limits": [], "overload": None,
                    "sticky": None, "idle": None, "backpressure": None,
                }
            else:
                helper = {
                    3: config_v6, 4: config_v6, 5: config_v6,
                    6: config_v6, 7: config_v7, 8: config_v8,
                }[version](1)
                cfg = helper
                cfg["version"] = version
                # v3..v5 后端项不含 endpoint；v6+ 含 endpoint。
                if version <= 5:
                    cfg["backends"] = be7
                else:
                    cfg["backends"] = be8
            results = self.run_ops([
                {"op": "ci", "config": cfg, "now": 0}, {"op": "ce"},
            ])
            self.assertEqual(
                results[1]["config"]["queue"],
                {"dequeue": "F", "full": "T"},
                version,
            )
            self.assertEqual(results[1]["config"]["version"], 9)

    def test_v9_loads_and_exports_sh(self):
        cfg = self.config(dequeue="S", full="H")
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0}, {"op": "ce"},
        ])
        self.assertEqual(
            results[1]["config"]["queue"], {"dequeue": "S", "full": "H"}
        )

    # ---- 非法结构 INPUT/2 ----

    def test_invalid_queue_is_input(self):
        bad_queues = [
            {"dequeue": "X", "full": "H"},
            {"dequeue": "S", "full": "x"},
            {"dequeue": "s", "full": "H"},
            {"dequeue": "S", "full": "t"},
            {"dequeue": 1, "full": "H"},
            {"dequeue": True, "full": "H"},
            {"dequeue": "S", "full": None},
            {"dequeue": None, "full": "H"},
            {"dequeue": "S"},
            {"full": "H"},
            {"full": "H", "dequeue": "S"},      # 键序反
            {"dequeue": "S", "full": "H", "evicted": 0},
            {"dequeue": "S", "full": "H", "last": None},
            None, [], "S", 3,
        ]
        for bad in bad_queues:
            cfg = config_v9(1)
            cfg["queue"] = bad
            self.assert_failure(
                encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
                2, "INPUT",
            )

    def test_version_key_set_and_order_violations_are_input(self):
        # version=9 缺 queue（十一键）。
        missing = config_v8(1)
        missing["version"] = 9
        self.assert_failure(
            encode_ops([{"op": "ci", "config": missing, "now": 0}]),
            2, "INPUT",
        )
        # version=8 含 queue（十二键）。
        extra = config_v9(1)
        extra["version"] = 8
        self.assert_failure(
            encode_ops([{"op": "ci", "config": extra, "now": 0}]),
            2, "INPUT",
        )
        # version 字段非法（字符串、越界、bool）。
        for bad_version in ("9", 0, 10, True):
            cfg = config_v9(1)
            cfg["version"] = bad_version
            self.assert_failure(
                encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
                2, "INPUT",
            )

    def test_top_level_queue_before_quotas_is_input(self):
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":{'
            b'"version":9,'
            b'"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            b'"circuit":null,"drain":null,"endpoint":null}],'
            b'"vnodes":null,"limits":[],"overload":null,'
            b'"sticky":null,"idle":null,"backpressure":null,'
            b'"scheduler":{"pick":"W"},"faults":[],'
            b'"queue":{"dequeue":"S","full":"H"},"quotas":[]'
            b'}}]}'
        )
        self.assert_failure(raw, 2, "INPUT")

    def test_inner_queue_key_swap_is_input(self):
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":{'
            b'"version":9,'
            b'"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            b'"circuit":null,"drain":null,"endpoint":null}],'
            b'"vnodes":null,"limits":[],"overload":null,'
            b'"sticky":null,"idle":null,"backpressure":null,'
            b'"scheduler":{"pick":"W"},"faults":[],"quotas":[],'
            b'"queue":{"full":"H","dequeue":"S"}'
            b'}}]}'
        )
        self.assert_failure(raw, 2, "INPUT")

    # ---- 策略实际生效（不经 qp/rp）----

    def test_config_s_skips_blocking_head(self):
        # S：队首 cX 缺令牌阻塞时跳过，其后 cY 被接纳，阻塞项留队。
        cfg = self.config(
            dequeue="S",
            limits=[{"scope": "S", "id": "s1", "r": 1, "b": 3}],
        )
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            self.oa("c1", 0, sc=3),   # A：耗尽 3 令牌
            self.oa("cX", 0, sc=3),   # Q
            self.oa("cY", 0, sc=1),   # Q
            {"op": "ot", "now": 1},   # 补 1 令牌：跳 cX、纳 cY
            {"op": "og"},
        ])
        self.assertEqual(
            results[4], {"op": "ot", "expired": [], "admitted": ["cY"]}
        )
        self.assertEqual(results[5], {"op": "og", "queue": ["cX"]})

    def test_config_f_stops_at_blocking_head(self):
        # 同结构默认 F：首个阻塞项即停，cY 不被尝试。
        cfg = self.config(
            dequeue="F",
            limits=[{"scope": "S", "id": "s1", "r": 1, "b": 3}],
        )
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            self.oa("c1", 0, sc=3),
            self.oa("cX", 0, sc=3),
            self.oa("cY", 0, sc=1),
            {"op": "ot", "now": 1},
            {"op": "og"},
        ])
        self.assertEqual(results[4]["admitted"], [])
        self.assertEqual(results[5], {"op": "og", "queue": ["cX", "cY"]})

    def test_config_h_head_evicts_without_rp(self):
        # full=H 仅经配置生效：队满头淘汰，rg 读到 H 与淘汰计数。
        cfg = self.config(dequeue="F", full="H", cap=1, q=2, ttl=10)
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            self.oa("c1", 0),   # A
            self.oa("c2", 0),   # Q
            self.oa("c3", 0),   # Q，队满
            self.oa("c4", 0),   # 头淘汰 c2
            {"op": "og"},
            {"op": "rg"},
        ])
        self.assertEqual(results[4]["evicted"], "c2")
        self.assertEqual(results[5], {"op": "og", "queue": ["c3", "c4"]})
        self.assertEqual(
            results[6], {"op": "rg", "mode": "H", "evicted": 1, "last": "c2"}
        )

    def test_ci_clears_queue_and_resets_eviction_state(self):
        # 首次载入 H 并产生一次淘汰；关闭活动连接、取消排队项后第二次 ci
        # （v9 F/T）成功：队列清空、淘汰计数与 last 重置为 0、null，策略
        # 切回 F/T。
        first = self.config(dequeue="F", full="H", cap=1, q=2, ttl=10)
        second = self.config(dequeue="F", full="T", cap=1, q=2, ttl=10)
        results = self.run_ops([
            {"op": "ci", "config": first, "now": 0},
            self.oa("c1", 0),
            self.oa("c2", 0),
            self.oa("c3", 0),
            self.oa("c4", 0),                          # 淘汰 c2
            {"op": "close", "cid": "c1", "now": 1},
            {"op": "oc", "cid": "c3"},
            {"op": "oc", "cid": "c4"},
            {"op": "ci", "config": second, "now": 2},
            {"op": "og"},
            {"op": "rg"},
        ])
        self.assertEqual(results[9], {"op": "og", "queue": []})
        self.assertEqual(
            results[10], {"op": "rg", "mode": "T", "evicted": 0, "last": None}
        )

    # ---- cl/cb 规范化与回滚 ----

    def test_cl_normalizes_to_v9_with_loaded_queue(self):
        cfg = self.config(dequeue="S", full="H")
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0}, {"op": "cl"},
        ])
        commit = results[1]["commits"][0]["config"]
        self.assertEqual(commit["version"], 9)
        self.assertEqual(commit["queue"], {"dequeue": "S", "full": "H"})

    def test_post_commit_qp_rp_change_does_not_mutate_commit(self):
        # 提交时为 S/H；之后 qp/rp 改回 F/T；cl 快照仍为 S/H，当前 ce 为 F/T。
        cfg = self.config(dequeue="S", full="H")
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "qp", "mode": "F"},
            {"op": "rp", "mode": "T"},
            {"op": "cl"},
            {"op": "ce"},
        ])
        snapshot = results[3]["commits"][0]["config"]
        self.assertEqual(snapshot["queue"], {"dequeue": "S", "full": "H"})
        self.assertEqual(
            results[4]["config"]["queue"], {"dequeue": "F", "full": "T"}
        )

    def test_cb_restores_queue_policies_and_clears_eviction(self):
        # 载入 S/H 并产生一次淘汰后，仅以 close/oc 清空活动与排队（不用 ci，
        # 保留 evicted=1、last=c2）；cb 回滚同一快照后策略恢复 S/H、队列空、
        # 淘汰计数与 last 重置为 0、null，并新建 rev。
        sh = self.config(dequeue="S", full="H", cap=1, q=2, ttl=1000)
        results = self.run_ops([
            {"op": "ci", "config": sh, "now": 0},      # rev1 S/H
            self.oa("c1", 0),
            self.oa("c2", 0),
            self.oa("c3", 0),
            self.oa("c4", 0),                          # 淘汰 c2
            {"op": "close", "cid": "c1", "now": 1},
            {"op": "oc", "cid": "c3"},
            {"op": "oc", "cid": "c4"},
            {"op": "cb", "rev": 1, "now": 2},          # 恢复 S/H，新建 rev2
            {"op": "og"},
            {"op": "rg"},
            {"op": "ce"},
        ])
        self.assertEqual(
            results[8], {"op": "cb", "target": 1, "rev": 2, "ok": True}
        )
        self.assertEqual(results[9], {"op": "og", "queue": []})
        self.assertEqual(
            results[10], {"op": "rg", "mode": "H", "evicted": 0, "last": None}
        )
        self.assertEqual(
            results[11]["config"]["queue"], {"dequeue": "S", "full": "H"}
        )

    def test_cb_rollback_owns_new_rev_with_restored_queue(self):
        sh = self.config(dequeue="S", full="H")
        results = self.run_ops([
            {"op": "ci", "config": sh, "now": 0},
            {"op": "qp", "mode": "F"},
            {"op": "rp", "mode": "T"},
            {"op": "cb", "rev": 1, "now": 1},
            {"op": "cl"},
        ])
        self.assertEqual(
            results[3], {"op": "cb", "target": 1, "rev": 2, "ok": True}
        )
        commits = results[4]["commits"]
        # 两个快照均规范化为 v9 且均携带恢复后的 S/H。
        self.assertEqual(len(commits), 2)
        for commit in commits:
            self.assertEqual(commit["config"]["version"], 9)
            self.assertEqual(
                commit["config"]["queue"], {"dequeue": "S", "full": "H"}
            )

    # ---- STATE/4 ----

    def test_active_connection_blocks_ci_and_cb(self):
        cfg = config_v9(1)
        for terminal in ("ci", "cb"):
            ops = [
                {"op": "ci", "config": cfg, "now": 0},
                {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
            ]
            if terminal == "ci":
                ops.append({"op": "ci", "config": cfg, "now": 2})
            else:
                ops.append({"op": "cb", "rev": 1, "now": 2})
            self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_queued_item_blocks_ci_and_cb(self):
        cfg = self.config(cap=1, q=2, ttl=10)
        for terminal in ("ci", "cb"):
            ops = [
                {"op": "ci", "config": cfg, "now": 0},
                self.oa("c1", 0),   # A
                self.oa("c2", 0),   # Q
            ]
            if terminal == "ci":
                ops.append({"op": "ci", "config": cfg, "now": 1})
            else:
                ops.append({"op": "cb", "rev": 1, "now": 1})
            self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_failed_ci_batch_is_atomic(self):
        # 同批内先成功 ci（S/H）并 qp/rp 改策略，随后因活动连接 ci 失败：
        # 整批无 stdout、错误 STATE，中间 rev 与策略变更均不落盘（进程级
        # 原子回滚，与既有 ci 失败语义一致）。
        cfg = self.config(dequeue="S", full="H")
        same = config_v9(1)
        code, out, err = run_balancer(
            "run",
            encode_ops([
                {"op": "ci", "config": cfg, "now": 0},
                {"op": "qp", "mode": "F"},
                {"op": "rp", "mode": "T"},
                {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
                {"op": "ci", "config": same, "now": 2},
            ]),
        )
        self.assertEqual((code, out, err), (4, b"", b'{"error":"STATE"}\n'))

    # ---- 紧凑 JSON 与 record/replay ----

    def test_record_replay_byte_identical(self):
        cfg = self.config(dequeue="S", full="H", cap=1, q=2, ttl=1000)
        ops = [
            {"op": "ci", "config": cfg, "now": 7},
            {"op": "rp", "mode": "T"},
            {"op": "ce"},
            {"op": "cl"},
            {"op": "cb", "rev": 1, "now": 9},
            {"op": "ce"},
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
        # 单换行、紧凑、固定键序。
        self.assertEqual(run_stdout.count(b"\n"), 1)
        self.assertIn(b'"version":9', run_stdout)
        self.assertIn(b'"queue":{"dequeue":"S","full":"H"}', run_stdout)


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


class OdTraceTest(unittest.TestCase):
    """od 只读接纳明细：遍历与判定同 oi，逐尝试输出 trace。"""

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

    def od(self, **overrides):
        op = {
            "op": "od", "key": "k1", "c": "c1", "s": "s1",
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

    def test_accept_first_candidate_trace(self):
        result = self.result(self.setup() + [self.od(now=0)])
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], "c")
        self.assertEqual(
            result["trace"],
            [{"id": "c", "effect": "N", "latency": 0,
              "result": "A", "blocked": []}],
        )

    def test_fault_then_accept_trace(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "fs", "id": order[0], "k": "D",
                    "a": 0, "z": 100, "v": 0})
        ops.append(self.od(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], order[1])
        self.assertEqual(
            result["trace"],
            [{"id": order[0], "effect": "D", "latency": 0,
              "result": "F", "blocked": []},
             {"id": order[1], "effect": "N", "latency": 0,
              "result": "A", "blocked": []}],
        )

    def test_f_phase_effect_and_recovery(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        # F 段 v=2：now=0 为故障相位（effect D），now=2 为非故障相位
        # （effect N）。
        ops.append({"op": "fs", "id": order[0], "k": "F",
                    "a": 0, "z": 100, "v": 2})
        ops.append(self.od(now=0))
        ops.append(self.od(now=2))
        results = self.run_ops(ops)
        self.assertEqual(results[-2]["trace"][0]["effect"], "D")
        self.assertEqual(results[-2]["trace"][0]["result"], "F")
        self.assertEqual(results[-1]["trace"][0]["effect"], "N")
        self.assertEqual(results[-1]["trace"][0]["result"], "A")

    def test_slow_timeout_entry_latency(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "fs", "id": order[0], "k": "S",
                    "a": 0, "z": 100, "v": 9})
        ops.append(self.od(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        # S 且 v>timeout：result=S，latency=min(v,timeout)=timeout。
        self.assertEqual(
            result["trace"][0],
            {"id": order[0], "effect": "S", "latency": 5,
             "result": "S", "blocked": []},
        )

    def test_s_within_timeout_accepts_with_v(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        ops.append({"op": "fs", "id": order[0], "k": "S",
                    "a": 0, "z": 100, "v": 3})
        ops.append(self.od(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(
            result["trace"],
            [{"id": order[0], "effect": "S", "latency": 3,
              "result": "A", "blocked": []}],
        )

    def test_capacity_entry(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        # fr max=1 成功必在 order[0] 建连，占满 os.cap=1。
        ops.append({"op": "fr", "cid": "x", "flow": self.FLOW,
                    "key": "k1", "timeout": 9, "max": 1, "now": 0})
        # 同候选处 S v=3≤timeout：capacity 失败但 latency 仍为 v。
        ops.append({"op": "fs", "id": order[0], "k": "S",
                    "a": 0, "z": 100, "v": 3})
        ops.append(self.od(now=1))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], order[1])
        self.assertEqual(
            result["trace"][0],
            {"id": order[0], "effect": "S", "latency": 3,
             "result": "C", "blocked": []},
        )

    def test_bucket_shortage_blocked_label(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        # B 桶挂在首候选后端：b=1 而 bc=2，投影不足。
        ops.append({"op": "ls", "scope": "B", "id": order[0],
                    "r": 1, "b": 1, "now": 0})
        ops.append(self.od(bc=2, now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "A")
        self.assertEqual(result["backend"], order[1])
        self.assertEqual(
            result["trace"][0],
            {"id": order[0], "effect": "N", "latency": 0,
             "result": "Q", "blocked": ["BT"]},
        )
        self.assertEqual(result["trace"][1]["result"], "A")

    def test_blocked_sequence_order(self):
        ops = self.setup()
        # C 桶投影不足（cc=2>b=1）且 S 配额窗内已耗尽：同尝试两项不足，
        # 按 BT,BQ,CT,CQ,ST,SQ 序列出。
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 1, "now": 0})
        ops.append({"op": "qs", "scope": "S", "id": "s1",
                    "limit": 1, "span": 10, "now": 0})
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k9",
                    "now": 0})
        ops.append(self.od(cc=2, now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertIsNone(result["backend"])
        self.assertEqual(len(result["trace"]), 3)
        for entry in result["trace"]:
            self.assertEqual(entry["result"], "Q")
            self.assertEqual(entry["blocked"], ["CT", "SQ"])

    def test_fixed_window_rolls_projection_only(self):
        ops = self.setup()
        ops.append({"op": "qs", "scope": "C", "id": "c1",
                    "limit": 1, "span": 10, "now": 0})
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k1",
                    "now": 0})
        # now=0 窗内配额已尽：CQ；now=10 窗口推进后投影重置：通过。
        ops.append(self.od(now=0))
        ops.append(self.od(now=10))
        results = self.run_ops(ops)
        self.assertEqual(results[-2]["state"], "R")
        self.assertEqual(results[-2]["trace"][0]["blocked"], ["CQ"])
        self.assertEqual(results[-1]["state"], "A")
        self.assertEqual(results[-1]["trace"][0]["blocked"], [])

    def test_exhaustion_trace_covers_all_attempts(self):
        ops = self.setup()
        for backend_id in ("a", "b", "c"):
            ops.append({"op": "fs", "id": backend_id, "k": "D",
                        "a": 0, "z": 100, "v": 0})
        ops.append(self.od(now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertIsNone(result["backend"])
        self.assertEqual(len(result["trace"]), 3)
        for entry in result["trace"]:
            self.assertEqual(entry["effect"], "D")
            self.assertEqual(entry["result"], "F")
            self.assertEqual(entry["blocked"], [])

    def test_max_bounds_distinct_attempts(self):
        order = self.ring_order(("a", "b", "c"), 8, "k1")
        ops = self.setup()
        for backend_id in ("a", "b", "c"):
            ops.append({"op": "fs", "id": backend_id, "k": "D",
                        "a": 0, "z": 100, "v": 0})
        ops.append(self.od(max=2, now=0))
        result = self.result(ops)
        self.assertEqual(result["state"], "R")
        self.assertEqual(
            [entry["id"] for entry in result["trace"]],
            [order[0], order[1]],
        )

    def test_read_only_does_not_consume_or_connect_or_account(self):
        ops = self.setup()
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 1, "now": 0})
        ops.append(self.od(cc=1, now=0))
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
        ops.append(self.od(now=100))
        ops.append({"op": "fm", "id": "a"})
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["D"]["affected"], 0)

    def test_clock_advances_and_regression_is_input(self):
        ops = self.setup()
        ops.append(self.od(now=10))
        ops.append({"op": "la", "c": "c1", "s": "s1", "key": "k1",
                    "now": 9})
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_state_errors(self):
        # 未 chash：STATE。
        ops = [{"op": "add", "id": "a", "weight": 1},
               {"op": "os", "cap": 1, "q": 2, "ttl": 10},
               self.od()]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        # 已 chash 未 os：STATE。
        ops = [{"op": "add", "id": "a", "weight": 1},
               {"op": "chash", "vnodes": 1}, self.od()]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        # 环内无合格候选（唯一后端不健康）：STATE。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 2, "ttl": 10},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 0},
            self.od(now=0),
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_input_errors(self):
        setup = self.setup()

        def raw_of(op_obj):
            return encode_ops(setup + [op_obj])

        # 键序错误（c 先于 key）。
        self.assert_failure(
            b'{"ops":[{"op":"od","c":"c1","key":"k1","s":"s1",'
            b'"bc":1,"cc":1,"sc":1,"timeout":5,"max":3,"now":0}]}',
            2, "INPUT",
        )
        # 三项成本全零。
        self.assert_failure(raw_of(self.od(bc=0, cc=0, sc=0, now=0)),
                            2, "INPUT")
        # bool 混入 max。
        self.assert_failure(raw_of(self.od(max=True, now=0)), 2, "INPUT")
        # timeout 越界。
        self.assert_failure(raw_of(self.od(timeout=-1, now=0)),
                            2, "INPUT")
        # 多键。
        extra = self.od(now=0)
        extra["x"] = 1
        self.assert_failure(raw_of(extra), 2, "INPUT")
        # 缺键。
        missing = self.od(now=0)
        del missing["sc"]
        self.assert_failure(raw_of(missing), 2, "INPUT")
        # 空 key。
        self.assert_failure(raw_of(self.od(key="", now=0)), 2, "INPUT")

    def test_exact_result_key_order_bytes(self):
        raw = encode_ops(self.setup() + [self.od(now=0)])
        code, out, err = run_balancer("run", raw)
        self.assertEqual((code, err), (0, b""))
        line = next(line for line in out.split(b"\n")
                    if b'"op":"od"' in line)
        expected = (
            b'{"op":"od","state":"A","backend":"c","trace":['
            b'{"id":"c","effect":"N","latency":0,"result":"A",'
            b'"blocked":[]}]}'
        )
        self.assertIn(expected, line)

    def test_record_replay_round_trip(self):
        ops = self.setup()
        ops.append({"op": "ls", "scope": "C", "id": "c1",
                    "r": 1, "b": 1, "now": 0})
        ops.append(self.od(cc=2, now=0))
        raw = encode_ops(ops)
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


class OaQuotaAdmissionTest(unittest.TestCase):
    """固定窗口配额参与 oa/ot 接纳：qs/qg/la/oi/od 行为不变。"""

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

    def base_ops(self, cap=10, q=4, ttl=10):
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": cap, "q": q, "ttl": ttl},
        ]

    def oa(self, cid, now, **extra):
        op = {"op": "oa", "cid": cid, "flow": FLOW,
              "c": "c1", "s": "s1", "key": "k", "now": now}
        op.update(extra)
        return op

    def qs(self, scope, id_, limit, span=100, now=0):
        return {"op": "qs", "scope": scope, "id": id_,
                "limit": limit, "span": span, "now": now}

    def test_admit_consumes_configured_quotas_by_cost(self):
        # 旧键集成本默认 1：B 配额 used 加 1。
        ops = self.base_ops() + [
            self.qs("B", "a", 5),
            self.oa("c1", 0),
            {"op": "qg", "scope": "B", "id": "a", "now": 0},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[4]["state"], "A")
        self.assertEqual(results[4]["backend"], "a")
        self.assertEqual(results[5]["used"], 1)
        # 新键集：C/S 配额按 cc/sc 扣减，B 未配置不限。
        ops = self.base_ops() + [
            self.qs("C", "c1", 10),
            self.qs("S", "s1", 10),
            self.oa("c1", 0, bc=0, cc=2, sc=7),
            {"op": "qg", "scope": "C", "id": "c1", "now": 0},
            {"op": "qg", "scope": "S", "id": "s1", "now": 0},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[6]["used"], 2)
        self.assertEqual(results[7]["used"], 7)

    def test_quota_shortage_queues_q_without_rate(self):
        ops = self.base_ops() + [
            self.qs("B", "a", 1, span=10),
            self.oa("c1", 0),                    # A：used=1
            self.oa("c2", 1),                    # 配额尽：Q，非 RATE
            {"op": "og"},
            {"op": "qg", "scope": "B", "id": "a", "now": 1},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[4], {"op": "oa", "cid": "c1",
                                      "state": "A", "backend": "a"})
        self.assertEqual(results[5], {"op": "oa", "cid": "c2",
                                      "state": "Q", "backend": None})
        self.assertEqual(results[6], {"op": "og", "queue": ["c2"]})
        # 排队不扣配额：used 仍为 1，窗口未推进。
        self.assertEqual(results[7]["used"], 1)
        self.assertEqual(results[7]["window"], 0)

    def test_queued_item_does_not_consume_or_roll_window(self):
        # 阻塞入队只补充/推进检查态，不增 used；跨窗重置只发生在 ot 重试。
        ops = self.base_ops() + [
            self.qs("B", "a", 1, span=10),
            self.oa("c1", 0),
            self.oa("c2", 5),                    # 同窗 Q
            {"op": "qg", "scope": "B", "id": "a", "now": 5},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[6]["window"], 0)
        self.assertEqual(results[6]["used"], 1)

    def test_ot_admits_queued_with_quota_and_cross_window_reset(self):
        ops = self.base_ops() + [
            self.qs("B", "a", 1, span=10),
            self.oa("c1", 0),                    # A used=1
            self.oa("c2", 1),                    # Q
            {"op": "ot", "now": 10},             # 跨窗：used 清零后接纳
            {"op": "og"},
            {"op": "qg", "scope": "B", "id": "a", "now": 10},
        ]
        results = self.run_ops(ops)
        self.assertEqual(
            results[6],
            {"op": "ot", "expired": [], "admitted": ["c2"]},
        )
        self.assertEqual(list(results[6]), ["op", "expired", "admitted"])
        self.assertEqual(results[7], {"op": "og", "queue": []})
        self.assertEqual(results[8]["window"], 1)
        self.assertEqual(results[8]["used"], 1)

    def test_ot_chain_admits_until_quota_blocks_and_keeps_fifo(self):
        # C 配额限 2：c1/c2 接纳，c3（成本 1）与 c4（成本 2）排队；ot 跨窗
        # 后 c3 接纳（used=1），c4 需 used+2=3>2 阻塞，放回队首。
        ops = self.base_ops(ttl=1000) + [
            self.qs("C", "c1", 2, span=10),
            self.oa("c1", 0),
            self.oa("c2", 0),
            self.oa("c3", 0),                    # Q
            self.oa("c4", 0, bc=1, cc=2, sc=1),      # Q
            {"op": "ot", "now": 10},             # 跨窗
            {"op": "og"},
            {"op": "qg", "scope": "C", "id": "c1", "now": 10},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[8]["admitted"], ["c3"])
        self.assertEqual(results[8]["expired"], [])
        self.assertEqual(results[9], {"op": "og", "queue": ["c4"]})
        self.assertEqual(results[10]["used"], 1)
        self.assertEqual(results[10]["window"], 1)

    def test_ot_uses_each_item_enqueued_costs(self):
        # 队首 sc=3 阻塞时其后的 sc=1 项不得被尝试（FIFO 首阻塞即停）；
        # 跨窗后按各自入队成本顺序扣减。
        ops = self.base_ops(ttl=1000) + [
            self.qs("S", "s1", 3),
            self.oa("c1", 0, bc=0, cc=0, sc=3),  # A used=3
            self.oa("cX", 0, bc=0, cc=0, sc=3),  # Q（6>3）
            self.oa("cY", 0, bc=0, cc=0, sc=1),  # Q（4>3）
            {"op": "ot", "now": 50},             # 同窗：cX 阻塞即停
            {"op": "og"},
            {"op": "ot", "now": 100},            # 跨窗 used=0：cX 用 3，cY 需 4>3
            {"op": "og"},
            {"op": "qg", "scope": "S", "id": "s1", "now": 100},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[7]["admitted"], [])
        self.assertEqual(results[8], {"op": "og", "queue": ["cX", "cY"]})
        self.assertEqual(results[9]["admitted"], ["cX"])
        self.assertEqual(results[10], {"op": "og", "queue": ["cY"]})
        self.assertEqual(results[11]["used"], 3)

    def test_ot_expiry_deducts_no_tokens_and_no_quota(self):
        ops = self.base_ops(ttl=10) + [
            self.qs("S", "s1", 3, span=10),
            self.oa("c1", 0, bc=0, cc=0, sc=3),  # A used=3
            self.oa("c2", 0, bc=0, cc=0, sc=3),  # Q
            {"op": "ot", "now": 10},             # now>=0+10 到期，且恰跨窗
            {"op": "og"},
            {"op": "qg", "scope": "S", "id": "s1", "now": 10},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[6],
                         {"op": "ot", "expired": ["c2"], "admitted": []})
        self.assertEqual(results[7], {"op": "og", "queue": []})
        # 到期不扣；ot 的 now 恰跨窗，qg 读到新窗 used=0。
        self.assertEqual(results[8]["window"], 1)
        self.assertEqual(results[8]["used"], 0)

    def test_token_shortage_also_queues_when_quota_free(self):
        ops = self.base_ops() + [
            {"op": "ls", "scope": "B", "id": "a",
             "r": 1, "b": 1, "now": 0},
            self.qs("B", "a", 100),
            self.oa("c1", 0),                    # A：耗尽唯一令牌
            self.oa("c2", 0),                    # 令牌不足：Q
            {"op": "ot", "now": 0},              # 同时刻无补充：仍阻塞
            {"op": "og"},
            {"op": "ot", "now": 1},              # 补充 1 令牌：接纳
            {"op": "qg", "scope": "B", "id": "a", "now": 1},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[6]["state"], "Q")
        self.assertEqual(results[7]["admitted"], [])
        self.assertEqual(results[8], {"op": "og", "queue": ["c2"]})
        self.assertEqual(results[9]["admitted"], ["c2"])
        self.assertEqual(results[10]["used"], 2)

    def test_queue_full_with_quota_block_is_overload(self):
        ops = self.base_ops(cap=10, q=1) + [
            self.qs("B", "a", 1),
            self.oa("c1", 0),                    # A
            self.oa("c2", 0),                    # Q（占满 q=1）
            self.oa("c3", 0),                    # 配额不足本应排队：队满
        ]
        self.assert_failure(ops, 7, "OVERLOAD")
        # 失败批不扣额外配额、不留队列项。
        results = self.run_ops(ops[:-1] + [
            {"op": "og"},
            {"op": "qg", "scope": "B", "id": "a", "now": 0},
        ])
        self.assertEqual(results[-2], {"op": "og", "queue": ["c2"]})
        self.assertEqual(results[-1]["used"], 1)

    def test_backpressure_p_quota_block_is_overload(self):
        ops = self.base_ops() + [
            {"op": "bp", "low": 0, "high": 1},
            self.qs("B", "a", 1),
            self.oa("c1", 0),                    # A
            self.oa("c2", 0),                    # Q，队长达 high 转 P
            {"op": "bq"},
            self.oa("c3", 0),                    # P 态本应排队：OVERLOAD/7
        ]
        results = self.run_ops(ops[:-1])
        self.assertEqual(results[7]["state"], "P")
        self.assert_failure(ops, 7, "OVERLOAD")

    def test_failed_batch_rolls_back_oa_quota_and_clock(self):
        # 同一批次内 oa 接纳已扣配额（used=1），随后 la 把配额打满并再越界：
        # RATE/6、无 stdout，整批回滚（含 oa 的扣减、入队与时钟）。
        ops = self.base_ops() + [
            self.qs("B", "a", 2, now=0),
            self.oa("c1", 0),                    # A：used=1
            {"op": "la", "c": "c1", "s": "s1", "key": "k", "now": 1},
            {"op": "la", "c": "c1", "s": "s1", "key": "k", "now": 2},
        ]
        self.assert_failure(ops, 6, "RATE")
        # 活动 cid 重复同样整批失败：CONNECTION/5。
        ops = self.base_ops() + [
            self.qs("B", "a", 5, now=0),
            self.oa("c1", 0),
            self.oa("c1", 1),
        ]
        self.assert_failure(ops, 5, "CONNECTION")

    def test_clock_regression_on_ot_is_input(self):
        ops = self.base_ops() + [
            self.qs("B", "a", 5, now=5),
            self.oa("c1", 5),
            {"op": "ot", "now": 4},              # 时钟倒退
        ]
        self.assert_failure(ops, 2, "INPUT")

    def test_unconfigured_quota_unlimited(self):
        ops = self.base_ops(cap=2) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[3]["state"], "A")
        self.assertEqual(results[4]["state"], "A")

    def test_record_replay_round_trip(self):
        ops = self.base_ops() + [
            self.qs("B", "a", 1, span=10),
            self.oa("c1", 0),
            self.oa("c2", 0),                    # Q
            {"op": "ot", "now": 10},             # 跨窗接纳
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual((run_code, rep_code), (0, 0))
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class FullQueuePolicyTest(unittest.TestCase):
    """rp/rg FIFO 满载策略：T 尾拒绝（默认）、H 头淘汰、计数与重置规则。"""

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

    def base_ops(self, q=2):
        # 环上唯一后端 a，cap=1 便于制造入队与满载。
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": q, "ttl": 10},
        ]

    def oa(self, cid, now):
        return {"op": "oa", "cid": cid, "flow": FLOW,
                "c": "k", "s": "k", "key": "k", "now": now}

    def test_default_tail_reject_and_rg_initial(self):
        ops = self.base_ops() + [
            self.oa("c1", 0),   # A
            self.oa("c2", 0),   # Q
            self.oa("c3", 0),   # Q，队满
            {"op": "rg"},
            self.oa("c4", 0),   # 默认 T：尾拒绝 OVERLOAD/7
        ]
        results = self.run_ops(ops[:-1])
        rg = results[6]
        self.assertEqual(list(rg), ["op", "mode", "evicted", "last"])
        self.assertEqual(
            rg, {"op": "rg", "mode": "T", "evicted": 0, "last": None}
        )
        # T 模式 oa 结果保持原四键。
        self.assertEqual(list(results[3]), ["op", "cid", "state", "backend"])
        self.assert_failure(ops, 7, "OVERLOAD")

    def test_rp_idempotent_and_input_violations(self):
        ops = [{"op": "rp", "mode": "H"}, {"op": "rp", "mode": "H"},
               {"op": "rg"}]
        results = self.run_ops(ops)
        self.assertEqual(results[0], {"op": "rp", "ok": True})
        self.assertEqual(results[1], {"op": "rp", "ok": True})
        self.assertEqual(results[2]["mode"], "H")
        # 键序、类型或值非法均报 INPUT/2。
        self.assert_failure([{"mode": "H", "op": "rp"}], 2, "INPUT")
        self.assert_failure([{"op": "rp"}], 2, "INPUT")
        self.assert_failure([{"op": "rp", "mode": "X"}], 2, "INPUT")
        self.assert_failure([{"op": "rp", "mode": "h"}], 2, "INPUT")
        self.assert_failure([{"op": "rp", "mode": 1}], 2, "INPUT")
        self.assert_failure([{"op": "rp", "mode": True}], 2, "INPUT")
        self.assert_failure([{"op": "rp", "mode": "H", "x": 1}], 2, "INPUT")
        self.assert_failure([{"op": "rg", "x": 1}], 2, "INPUT")
        self.assert_failure([{"op": "rg", "mode": "T"}], 2, "INPUT")

    def test_head_eviction_flow_and_cid_reuse(self):
        ops = self.base_ops() + [
            {"op": "rp", "mode": "H"},
            self.oa("c1", 0),   # A：evicted=null
            self.oa("c2", 0),   # 普通入队：evicted=null
            self.oa("c3", 0),   # 普通入队，队满：evicted=null
            self.oa("c4", 0),   # 头淘汰 c2，新项落队尾
            {"op": "og"},
            {"op": "rg"},
            self.oa("c2", 0),   # 淘汰 cid 立即复用：头淘汰 c3
            {"op": "og"},
            {"op": "rg"},
        ]
        results = self.run_ops(ops)
        # H 模式结果键序 op,cid,state,backend,evicted。
        self.assertEqual(
            list(results[4]), ["op", "cid", "state", "backend", "evicted"]
        )
        self.assertEqual(
            results[4],
            {"op": "oa", "cid": "c1", "state": "A", "backend": "a",
             "evicted": None},
        )
        self.assertEqual(results[6]["evicted"], None)
        # H 模式 Q 的 backend 同样为路由选中 id（环上唯一后端 a）。
        self.assertEqual(results[6]["backend"], "a")
        self.assertEqual(
            results[7],
            {"op": "oa", "cid": "c4", "state": "Q", "backend": "a",
             "evicted": "c2"},
        )
        self.assertEqual(results[8], {"op": "og", "queue": ["c3", "c4"]})
        self.assertEqual(
            results[9], {"op": "rg", "mode": "H", "evicted": 1, "last": "c2"}
        )
        self.assertEqual(results[10]["evicted"], "c3")
        self.assertEqual(results[11], {"op": "og", "queue": ["c4", "c2"]})
        self.assertEqual(
            results[12],
            {"op": "rg", "mode": "H", "evicted": 2, "last": "c3"},
        )

    def test_eviction_keeps_oh_peak_and_backpressure(self):
        # 淘汰后新入队照常更新 oh 的 queued 与 peak；背压滞回上沿照常触发。
        ops = self.base_ops() + [
            {"op": "bp", "low": 0, "high": 2},
            {"op": "rp", "mode": "H"},
            self.oa("c1", 0),   # A
            self.oa("c2", 0),   # Q
            self.oa("c3", 0),   # Q，队长达 high 转 P
            {"op": "bq"},
            {"op": "oh", "from": 0, "to": 0, "now": 0},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[8]["state"], "P")
        self.assertEqual(
            results[9]["windows"][0],
            {"window": 0, "immediate": 1, "queued": 2,
             "dequeued": 0, "expired": 0, "peak": 2},
        )

    def test_h_mode_p_state_is_overload_without_change(self):
        ops = self.base_ops() + [
            {"op": "bp", "low": 0, "high": 1},
            {"op": "rp", "mode": "H"},
            self.oa("c1", 0),   # A
            self.oa("c2", 0),   # Q，队长达 high 转 P
            self.oa("c3", 0),   # P 态本应排队：OVERLOAD/7，无变更
        ]
        self.assert_failure(ops, 7, "OVERLOAD")
        # 失败批之前的运行态：无淘汰、队列完整。
        results = self.run_ops(ops[:-1] + [{"op": "rg"}, {"op": "og"}])
        self.assertEqual(
            results[7], {"op": "rg", "mode": "H", "evicted": 0, "last": None}
        )
        self.assertEqual(results[8], {"op": "og", "queue": ["c2"]})

    def test_ci_resets_policy_and_counters(self):
        setup = self.base_ops() + [
            {"op": "rp", "mode": "H"},
            self.oa("c1", 0),
            self.oa("c2", 0),
            self.oa("c3", 0),
            self.oa("c4", 0),   # 头淘汰 c2
        ]
        config = self.run_ops(self.base_ops() + [{"op": "ce"}])[3]["config"]
        ops = setup + [
            {"op": "oc", "cid": "c3"},
            {"op": "oc", "cid": "c4"},
            {"op": "close", "cid": "c1", "now": 1},
            {"op": "ci", "config": config, "now": 2},
            {"op": "rg"},
            self.oa("x1", 2),
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[11], {"op": "ci", "ok": True})
        self.assertEqual(
            results[12], {"op": "rg", "mode": "T", "evicted": 0, "last": None}
        )
        # ci 后 oa 恢复 T 模式四键结果。
        self.assertEqual(list(results[13]), ["op", "cid", "state", "backend"])

    def test_record_replay_round_trip(self):
        ops = self.base_ops() + [
            {"op": "rp", "mode": "H"},
            self.oa("c1", 0),
            self.oa("c2", 0),
            self.oa("c3", 0),
            self.oa("c4", 0),
            {"op": "rg"},
            {"op": "og"},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual((run_code, rep_code), (0, 0))
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class OqProjectionTest(unittest.TestCase):
    """oq 等待队列只读投影：FIFO 明细、E/R/C/T/Q 阻塞与只读、时钟契约。"""

    FLOW = ["s", 1, "t", 2, "tcp"]

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

    def base_ops(self, ids=("a",), cap=1, q=8, ttl=10, vnodes=1):
        ops = [{"op": "add", "id": backend_id, "weight": 1}
               for backend_id in ids]
        ops.append({"op": "chash", "vnodes": vnodes})
        ops.append({"op": "os", "cap": cap, "q": q, "ttl": ttl})
        return ops

    def oa(self, cid, now, key="k", c="c1", s="s1", **costs):
        op = {"op": "oa", "cid": cid, "flow": self.FLOW,
              "c": c, "s": s, "key": key, "now": now}
        op.update(costs)
        return op

    def ring_first(self, ids, vnodes, key):
        """复刻 build_ring 自 key 哈希点的首个后端。"""
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
        return tokens[index][3]

    def key_routing_to(self, ids, vnodes, target):
        for n in range(1000):
            key = "k%d" % n
            if self.ring_first(ids, vnodes, key) == target:
                return key
        self.fail("no key routing to %s" % target)

    def test_empty_queue_and_exact_outer_key_order(self):
        results = self.run_ops(self.base_ops() + [{"op": "oq", "now": 0}])
        self.assertEqual(results[-1], {"op": "oq", "items": []})
        self.assertEqual(list(results[-1]), ["op", "items"])

    def test_unblocked_item_has_empty_blocked_and_route_backend(self):
        # cap 充足、无桶无配额：未到期项 backend 为路由目标、blocked 为 []。
        # 制造 Q（B 配额尽），oq 跨窗投影时配额随窗口恢复。
        ops = self.base_ops(cap=10, ttl=100)
        ops.append({"op": "qs", "scope": "B", "id": "a",
                    "limit": 1, "span": 10, "now": 0})
        ops.append(self.oa("c1", 0))          # A，used=1
        ops.append(self.oa("c2", 0))          # Q
        ops.append({"op": "oq", "now": 10})   # 跨窗投影：配额足、无阻塞
        results = self.run_ops(ops)
        item = results[-1]["items"][0]
        self.assertEqual(
            item,
            {"cid": "c2", "backend": "a", "expires": 100,
             "expired": False, "blocked": []},
        )
        self.assertEqual(
            list(item),
            ["cid", "backend", "expires", "expired", "blocked"],
        )

    def test_expired_boundary_and_E_block(self):
        ops = self.base_ops(ttl=10)
        ops.append(self.oa("c1", 0))          # A 占满 cap=1
        ops.append(self.oa("c2", 3))          # Q，expires=13
        ops.append({"op": "oq", "now": 12})   # 未到期：C
        ops.append({"op": "oq", "now": 13})   # now==expires：到期 E
        results = self.run_ops(ops)
        self.assertEqual(
            results[5]["items"],
            [{"cid": "c2", "backend": "a", "expires": 13,
              "expired": False, "blocked": ["C"]}],
        )
        self.assertEqual(
            results[6]["items"],
            [{"cid": "c2", "backend": None, "expires": 13,
              "expired": True, "blocked": ["E"]}],
        )

    def test_expired_is_E_even_without_eligible_backend(self):
        # 到期判定先于环：后端失格仍报 E 而非 R。
        ops = self.base_ops(ttl=10)
        ops.append(self.oa("c1", 0))
        ops.append(self.oa("c2", 0))
        ops.append({"op": "hset", "id": "a", "fail": 1, "success": 1})
        ops.append({"op": "probe", "id": "a", "ok": False, "now": 10})
        ops.append({"op": "oq", "now": 10})
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["items"][0]["blocked"], ["E"])
        self.assertIsNone(results[-1]["items"][0]["backend"])

    def test_capacity_C_block_clears_after_close(self):
        ops = self.base_ops(cap=1)
        ops.append(self.oa("c1", 0))
        ops.append(self.oa("c2", 0))           # Q（cap 满）
        ops.append({"op": "oq", "now": 0})
        ops.append({"op": "close", "cid": "c1", "now": 1})
        ops.append({"op": "oq", "now": 1})     # 连接释放：无阻塞
        results = self.run_ops(ops)
        self.assertEqual(results[5]["items"][0]["blocked"], ["C"])
        self.assertEqual(results[7]["items"][0]["blocked"], [])
        # c2 仍在队列（oq 不建连）。
        self.assertEqual(results[7]["items"][0]["cid"], "c2")

    def test_token_T_projection_without_consuming_or_refilling(self):
        # C 桶 b=1：真实 la 耗尽后入队项 T；oq 不补充真实桶（lg 可见 at 不
        # 变），也不消耗；时钟推进后投影补足则无阻塞。
        ops = self.base_ops(cap=10)
        ops.append({"op": "ls", "scope": "C", "id": "cx",
                    "r": 1, "b": 1, "now": 0})
        ops.append({"op": "la", "c": "cx", "s": "s1",
                    "key": "x", "now": 0})       # t 1->0
        ops.append(self.oa("c2", 0, c="cx"))     # Q：投影 t=0<1
        ops.append({"op": "oq", "now": 0})
        ops.append({"op": "lg", "scope": "C", "id": "cx", "now": 0})
        ops.append({"op": "oq", "now": 1})       # 投影补足为 1：无 T
        results = self.run_ops(ops)
        self.assertEqual(results[6]["items"][0]["blocked"], ["T"])
        # oq 未推进真实桶的补充时刻（lg 前 at 仍为 0；lg 以 now=0 补充）。
        self.assertEqual(results[7]["at"], 0)
        self.assertEqual(results[7]["t"], 0)
        self.assertEqual(results[8]["items"][0]["blocked"], [])

    def test_quota_Q_projection_without_roll_or_consume(self):
        ops = self.base_ops(cap=10, ttl=100)
        ops.append({"op": "qs", "scope": "B", "id": "a",
                    "limit": 1, "span": 10, "now": 0})
        ops.append(self.oa("c1", 0))             # A，used=1
        ops.append(self.oa("c2", 0))             # Q
        ops.append({"op": "oq", "now": 5})       # 同窗：Q
        ops.append({"op": "qg", "scope": "B", "id": "a", "now": 5})
        ops.append({"op": "oq", "now": 10})      # 跨窗投影：配额足
        ops.append({"op": "qg", "scope": "B", "id": "a", "now": 10})
        results = self.run_ops(ops)
        self.assertEqual(results[6]["items"][0]["blocked"], ["Q"])
        # oq 未推进窗口、未消耗：qg now=5 仍是窗 0、used=1。
        self.assertEqual((results[7]["window"], results[7]["used"]), (0, 1))
        self.assertEqual(results[8]["items"][0]["blocked"], [])
        # 第二次 qg 才真正跨窗（now=10），used 归 0。
        self.assertEqual((results[9]["window"], results[9]["used"]), (1, 0))

    def test_CTQ_ordered_together(self):
        ops = self.base_ops(cap=1)
        ops.append({"op": "ls", "scope": "C", "id": "cx",
                    "r": 1, "b": 1, "now": 0})
        ops.append({"op": "la", "c": "cx", "s": "s1",
                    "key": "x", "now": 0})       # C 桶耗尽
        ops.append({"op": "qs", "scope": "B", "id": "a",
                    "limit": 1, "span": 1000, "now": 0})
        ops.append(self.oa("c1", 0))             # A：cap 满、B 配额 used=1
        ops.append(self.oa("c2", 0, c="cx",
                           bc=1, cc=1, sc=0))    # Q：C、T、Q
        ops.append({"op": "oq", "now": 0})
        results = self.run_ops(ops)
        item = results[-1]["items"][0]
        self.assertEqual(item["backend"], "a")
        self.assertEqual(item["blocked"], ["C", "T", "Q"])

    def test_fifo_order_and_independent_projections(self):
        # 两项阻塞来源不同但各自独立投影：前项不消耗后项可见的令牌；items
        # 严格按入队 FIFO。la 一次性耗尽 C 桶 cx 与 S 桶 sx（无 B 配额，
        # la 不建连），随后两项分别因 C、S 令牌不足入队。
        ops = self.base_ops(cap=10, ttl=100)
        ops.append({"op": "ls", "scope": "C", "id": "cx",
                    "r": 1, "b": 1, "now": 0})
        ops.append({"op": "ls", "scope": "S", "id": "sx",
                    "r": 1, "b": 1, "now": 0})
        ops.append({"op": "la", "c": "cx", "s": "sx",
                    "key": "x", "now": 0})       # 两桶各 1->0
        ops.append(self.oa("c2", 0, c="cx", s="sZ",
                           bc=0, cc=1, sc=0))    # C 不足：T
        ops.append(self.oa("c3", 0, c="cZ", s="sx",
                           bc=0, cc=0, sc=1))    # S 不足：T
        ops.append({"op": "oq", "now": 0})
        results = self.run_ops(ops)
        items = results[-1]["items"]
        self.assertEqual([item["cid"] for item in items], ["c2", "c3"])
        self.assertEqual(items[0]["blocked"], ["T"])
        self.assertEqual(items[1]["blocked"], ["T"])
        self.assertTrue(all(item["backend"] == "a" for item in items))
        # 真实桶未被 oq 消耗或补充：再查 lg 两桶 t 仍为 0、at 仍为 0。
        ops.append({"op": "lg", "scope": "C", "id": "cx", "now": 0})
        ops.append({"op": "lg", "scope": "S", "id": "sx", "now": 0})
        results = self.run_ops(ops)
        self.assertEqual((results[-2]["t"], results[-2]["at"]), (0, 0))
        self.assertEqual((results[-1]["t"], results[-1]["at"]), (0, 0))

    def test_R_when_no_eligible_backend(self):
        ids = ("a", "b")
        ops = self.base_ops(ids=ids, cap=10, vnodes=8)
        ops.append({"op": "qs", "scope": "B", "id": "a",
                    "limit": 1, "span": 1000, "now": 0})
        ops.append({"op": "qs", "scope": "B", "id": "b",
                    "limit": 1, "span": 1000, "now": 0})
        key_a = self.key_routing_to(ids, 8, "a")
        key_b = self.key_routing_to(ids, 8, "b")
        ops.append(self.oa("c1", 0, key=key_a))  # A（used a=1）
        ops.append(self.oa("c2", 0, key=key_a))  # Q：配额尽
        ops.append(self.oa("c3", 0, key=key_b))  # A（used b=1）
        ops.append(self.oa("c4", 0, key=key_b))  # Q
        ops.append({"op": "hset", "id": "a", "fail": 1, "success": 1})
        ops.append({"op": "hset", "id": "b", "fail": 1, "success": 1})
        ops.append({"op": "probe", "id": "a", "ok": False, "now": 1})
        ops.append({"op": "probe", "id": "b", "ok": False, "now": 1})
        ops.append({"op": "oq", "now": 1})
        results = self.run_ops(ops)
        items = results[-1]["items"]
        self.assertEqual([item["cid"] for item in items], ["c2", "c4"])
        for item in items:
            self.assertEqual(
                item["blocked"], ["R"],
            )
            self.assertIsNone(item["backend"])
            self.assertFalse(item["expired"])

    def test_remap_onto_ring_when_sticky_target_lost(self):
        # 入队时粘性指向 a；a 失格、b 合格：oq 只读后投影到 b（不写粘性），
        # 再次 oq 结果完全一致（证明未改写映射）。
        ids = ("a", "b")
        ops = self.base_ops(ids=ids, cap=10, vnodes=8)
        ops.append({"op": "qs", "scope": "B", "id": "a",
                    "limit": 1, "span": 1000, "now": 0})
        key_a = self.key_routing_to(ids, 8, "a")
        ops.append(self.oa("c1", 0, key=key_a))  # A
        ops.append(self.oa("c2", 0, key=key_a))  # Q（a 配额尽）
        ops.append({"op": "hset", "id": "a", "fail": 1, "success": 1})
        ops.append({"op": "probe", "id": "a", "ok": False, "now": 1})
        ops.append({"op": "oq", "now": 1})
        ops.append({"op": "oq", "now": 1})
        results = self.run_ops(ops)
        first = results[-2]["items"][0]
        second = results[-1]["items"][0]
        self.assertEqual(first["backend"], "b")
        self.assertEqual(first["blocked"], [])
        self.assertEqual(second, first)

    def test_read_only_preserves_queue_and_history(self):
        # oq 不接纳、不入账：队列不变；oh 在 oq 前后结果一致。
        ops = self.base_ops(cap=1)
        ops.append(self.oa("c1", 0))
        ops.append(self.oa("c2", 0))              # Q
        ops.append({"op": "oq", "now": 5})
        ops.append({"op": "og"})
        ops.append({"op": "oh", "from": 0, "to": 0, "now": 5})
        results = self.run_ops(ops)
        self.assertEqual(results[5]["items"][0]["blocked"], ["C"])
        self.assertEqual(results[6], {"op": "og", "queue": ["c2"]})
        self.assertEqual(
            results[7]["windows"][0],
            {"window": 0, "immediate": 1, "queued": 1,
             "dequeued": 0, "expired": 0, "peak": 1},
        )

    def test_clock_advances_and_regression_is_input(self):
        ops = self.base_ops()
        ops.append({"op": "oq", "now": 5})
        ops.append({"op": "oq", "now": 5})        # 非递减：合法
        ops.append({"op": "og"})
        ops.append({"op": "oq", "now": 4})        # 倒退：INPUT
        self.assert_failure(ops, 2, "INPUT")

    def test_state_errors(self):
        # 未 os：STATE（即便环已配）。
        ops = [{"op": "add", "id": "a", "weight": 1},
               {"op": "chash", "vnodes": 1},
               {"op": "oq", "now": 0}]
        self.assert_failure(ops, 4, "STATE")
        # 全新状态未 os：STATE。
        self.assert_failure([{"op": "oq", "now": 0}], 4, "STATE")

    def test_input_errors(self):
        base = self.base_ops()

        def raw_of(op_obj):
            return base + [op_obj]

        # 键序反：直接发原始字节（绕过 encode_ops 的键序规范化）。
        code, stdout, stderr = run_balancer(
            "run", b'{"ops":[{"now":0,"op":"oq"}]}'
        )
        self.assertEqual((code, stdout), (2, b""))
        self.assertEqual(stderr, b'{"error":"INPUT"}\n')
        # 多键、缺键。
        self.assert_failure(base + [{"op": "oq", "now": 0, "x": 1}],
                            2, "INPUT")
        self.assert_failure(base + [{"op": "oq"}], 2, "INPUT")
        # now 类型/范围：bool、负数、超 10^9、字符串、浮点、null。
        for bad in (True, False, -1, 10 ** 9 + 1, "0", 1.0, None):
            self.assert_failure(
                raw_of({"op": "oq", "now": bad}), 2, "INPUT"
            )

    def test_result_byte_layout(self):
        raw = encode_ops(
            self.base_ops(ttl=10)
            + [self.oa("c1", 0), self.oa("c2", 3),
               {"op": "oq", "now": 13}]
        )
        code, out, err = run_balancer("run", raw)
        self.assertEqual((code, err), (0, b""))
        self.assertIn(
            b'{"op":"oq","items":[{"cid":"c2","backend":null,'
            b'"expires":13,"expired":true,"blocked":["E"]}]}',
            out,
        )

    def test_record_replay_round_trip(self):
        ops = (
            self.base_ops(cap=1, ttl=10)
            + [self.oa("c1", 0), self.oa("c2", 0),
               {"op": "oq", "now": 0}, {"op": "oq", "now": 10}]
        )
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual((run_code, rep_code), (0, 0))
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class BrSnapshotTest(unittest.TestCase):
    """全池运行态快照 br：四态合成、ready/blocked 规则与输入校验。"""

    def assert_failure(self, ops, exit_code, label):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def br_of(self, ops, now):
        code, stdout, stderr = run_balancer(
            "run", encode_ops(ops + [{"op": "br", "now": now}])
        )
        self.assertEqual((code, stderr), (0, b""))
        results = json.loads(stdout.decode("utf-8"))["results"]
        return results[-1]

    def test_empty_pool(self):
        snap = self.br_of([], 0)
        self.assertEqual(snap, {"op": "br", "now": 0, "backends": []})

    def test_healthy_defaults_and_join_order(self):
        ops = [
            {"op": "add", "id": "b", "weight": 2},
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
        ]
        snap = self.br_of(ops, 0)
        # 加入序 b,a；open 选权重高者 b，connections 随之反映。
        self.assertEqual([item["id"] for item in snap["backends"]],
                         ["b", "a"])
        self.assertEqual(
            snap["backends"][0],
            {
                "id": "b", "health": "healthy", "circuit": "C",
                "drain": "A", "fault": "N", "connections": 1,
                "ready": True, "blocked": [],
            },
        )
        self.assertEqual(snap["backends"][1]["connections"], 0)

    def test_each_blocked_reason(self):
        ops = [
            {"op": "add", "id": "h", "weight": 1},
            {"op": "hset", "id": "h", "fail": 1, "success": 1},
            {"op": "probe", "id": "h", "ok": False, "now": 0},
            {"op": "add", "id": "c", "weight": 1},
            {"op": "cs", "id": "c", "n": 1, "m": 1, "r": 1, "w": 100,
             "q": 1},
            {"op": "cr", "id": "c", "ok": False, "now": 0},
            {"op": "add", "id": "d", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "ds", "id": "d", "t": 10},
            {"op": "dr", "id": "d", "now": 0},
            {"op": "add", "id": "f", "weight": 1},
            {"op": "fs", "id": "f", "k": "D", "a": 0, "z": 50, "v": 0},
        ]
        snap = self.br_of(ops, 5)
        blocked = {
            item["id"]: item["blocked"] for item in snap["backends"]
        }
        self.assertEqual(blocked, {
            "h": ["health"],
            "c": ["circuit"],
            "d": ["drain"],
            "f": ["fault"],
        })
        for item in snap["backends"]:
            self.assertFalse(item["ready"])
        # d 持有一个连接，drain 为 D（非 X）。
        states = {item["id"]: item for item in snap["backends"]}
        self.assertEqual(states["d"]["drain"], "D")
        self.assertEqual(states["d"]["connections"], 1)
        self.assertEqual(states["c"]["circuit"], "O")
        self.assertEqual(states["h"]["health"], "unhealthy")
        self.assertEqual(states["f"]["fault"], "D")

    def test_blocked_multiple_in_fixed_order(self):
        # 同一后端同时未满足四项：按 health,circuit,drain,fault 序列出。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 0},
            {"op": "cs", "id": "a", "n": 1, "m": 1, "r": 1, "w": 100,
             "q": 1},
            {"op": "cr", "id": "a", "ok": False, "now": 0},
            {"op": "ds", "id": "a", "t": 10},
            {"op": "dr", "id": "a", "now": 0},
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 50, "v": 0},
        ]
        snap = self.br_of(ops, 5)
        item = snap["backends"][0]
        self.assertEqual(
            item["blocked"], ["health", "circuit", "drain", "fault"]
        )
        self.assertFalse(item["ready"])
        self.assertEqual(item["drain"], "X")  # 无连接，dr 直接转 X。

    def test_slow_and_flap_non_fault_phase_do_not_block(self):
        ops = [
            {"op": "add", "id": "s", "weight": 1},
            {"op": "fs", "id": "s", "k": "S", "a": 0, "z": 50, "v": 7},
            {"op": "add", "id": "f", "weight": 1},
            {"op": "fs", "id": "f", "k": "F", "a": 0, "z": 100, "v": 10},
        ]
        # now=5：S 段为 S 不阻断；F 段 (5//10)%2=0 故障相位，阻断。
        snap = self.br_of(ops, 5)
        states = {item["id"]: item for item in snap["backends"]}
        self.assertEqual(states["s"]["fault"], "S")
        self.assertTrue(states["s"]["ready"])
        self.assertEqual(states["s"]["blocked"], [])
        self.assertEqual(states["f"]["fault"], "D")
        self.assertEqual(states["f"]["blocked"], ["fault"])
        # now=15：F 段 (15//10)%2=1 非故障相位，恢复 ready。
        snap = self.br_of(ops, 15)
        states = {item["id"]: item for item in snap["backends"]}
        self.assertEqual(states["f"]["fault"], "N")
        self.assertTrue(states["f"]["ready"])

    def test_fault_gap_and_expired_segment_are_n(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "D", "a": 10, "z": 20, "v": 0},
        ]
        # 段前与段后（含端点 z）均为 N。
        for now in (0, 9, 20, 100):
            snap = self.br_of(ops, now)
            item = snap["backends"][0]
            self.assertEqual(item["fault"], "N")
            self.assertTrue(item["ready"])
        snap = self.br_of(ops, 10)
        self.assertEqual(snap["backends"][0]["fault"], "D")

    def test_clock_shared_and_non_decreasing(self):
        # 等值 now 合法（非递减）；倒退报 INPUT。
        ops = [
            {"op": "br", "now": 5},
            {"op": "br", "now": 5},
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stderr), (0, b""))
        self.assert_failure(
            [{"op": "br", "now": 5}, {"op": "br", "now": 4}], 2, "INPUT"
        )
        # 与其他带 now 操作共用同一时钟。
        self.assert_failure(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "probe", "id": "a", "ok": True, "now": 5},
                {"op": "br", "now": 4},
            ],
            2,
            "INPUT",
        )

    def test_input_errors(self):
        # 键序反：直接发原始字节（绕过 encode_ops 的键序规范化）。
        code, stdout, stderr = run_balancer(
            "run", b'{"ops":[{"now":0,"op":"br"}]}'
        )
        self.assertEqual((code, stdout), (2, b""))
        self.assertEqual(stderr, b'{"error":"INPUT"}\n')
        # 多键、缺键。
        self.assert_failure([{"op": "br", "now": 0, "x": 1}], 2, "INPUT")
        self.assert_failure([{"op": "br"}], 2, "INPUT")
        # now 类型/范围：bool、负数、超 10^9、字符串、浮点、null。
        for bad in (True, False, -1, 10 ** 9 + 1, "0", 1.0, None):
            self.assert_failure([{"op": "br", "now": bad}], 2, "INPUT")
        # 边界 0 与 10^9 合法。
        for good in (0, 10 ** 9):
            code, stdout, stderr = run_balancer(
                "run", encode_ops([{"op": "br", "now": good}])
            )
            self.assertEqual((code, stderr), (0, b""))

    def test_failure_rolls_back_batch(self):
        # 批内后续操作失败：整批无 stdout，时钟与状态不留痕。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "br", "now": 3},
            {"op": "br", "now": 2},
        ]
        self.assert_failure(ops, 2, "INPUT")

    def test_result_byte_layout(self):
        raw = encode_ops(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "br", "now": 0},
            ]
        )
        code, out, err = run_balancer("run", raw)
        self.assertEqual((code, err), (0, b""))
        self.assertIn(
            b'{"op":"br","now":0,"backends":[{"id":"a","health":"healthy",'
            b'"circuit":"C","drain":"A","fault":"N","connections":0,'
            b'"ready":true,"blocked":[]}]}',
            out,
        )
        self.assertTrue(out.endswith(b"\n"))
        self.assertNotIn(b"\n", out[:-1])

    def test_record_replay_round_trip(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "F", "a": 0, "z": 100, "v": 10},
            {"op": "br", "now": 5},
            {"op": "br", "now": 15},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual((run_code, rep_code), (0, 0))
        self.assertEqual(
            (rep_code, rep_stdout, rep_stderr),
            (run_code, run_stdout, run_stderr),
        )


class HashDryRunTest(unittest.TestCase):
    """哈希配置预演 hd：以当前/候选配置的 backends、vnodes 建环（忽略运行态
    与粘性），按 keys 原序预演 chash 映射，不应用配置。"""

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

    def backend(self, bid, **overrides):
        item = {
            "id": bid, "weight": 1, "d": 0, "fail": 3, "success": 2,
            "circuit": None, "drain": None, "endpoint": None,
        }
        item.update(overrides)
        return item

    def make_config(self, ids, vnodes, backends=None, **overrides):
        config = config_v9(1, **overrides)
        config["backends"] = (
            list(backends)
            if backends is not None
            else [self.backend(bid) for bid in ids]
        )
        config["vnodes"] = vnodes
        return config

    def hd(self, ids, vnodes, keys, now):
        return {"op": "hd", "config": self.make_config(ids, vnodes),
                "keys": list(keys), "now": now}

    def load(self, ids, vnodes, now=0):
        return {"op": "ci", "config": self.make_config(ids, vnodes), "now": now}

    @staticmethod
    def reference_map(ids, vnodes, key):
        """与实现同款 chash 规则的独立参考实现。"""
        tokens = []
        for join_index, bid in enumerate(ids):
            encoded = bid.encode("utf-8")
            for i in range(vnodes):
                digest = hashlib.sha256(
                    encoded + b"\x00" + str(i).encode("ascii")
                ).digest()
                tokens.append(
                    (int.from_bytes(digest, "big"), join_index, i, bid)
                )
        tokens.sort(key=lambda token: (token[0], token[1], token[2]))
        digests = [token[0] for token in tokens]
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0
        return tokens[index][3]

    def keys_for_each_backend(self, ids, vnodes):
        """确定性地为每个后端各取一个映射到它的 key（扫描生成串）。"""
        found = {}
        n = 0
        while len(found) < len(ids):
            key = "probe-key-%d" % n
            target = self.reference_map(ids, vnodes, key)
            found.setdefault(target, key)
            n += 1
        return found

    def test_result_shape_and_key_order(self):
        results = self.run_ops([
            self.load(["a", "b", "c"], 4),
            self.hd(["a", "b", "c", "d"], 8, ["k1", "k2", "k1"], 5),
        ])
        result = results[1]
        self.assertEqual(
            list(result), ["op", "base", "target", "cases", "summary"]
        )
        self.assertEqual(result["op"], "hd")
        self.assertRegex(result["base"], r"^[0-9a-f]{64}$")
        self.assertRegex(result["target"], r"^[0-9a-f]{64}$")
        self.assertEqual(len(result["cases"]), 3)
        for case in result["cases"]:
            self.assertEqual(list(case), ["key", "before", "after", "changed"])
            self.assertIsInstance(case["before"], str)
            self.assertIsInstance(case["after"], str)
            self.assertIsInstance(case["changed"], bool)
            self.assertEqual(case["changed"],
                             case["before"] != case["after"])
        self.assertEqual(
            list(result["summary"]), ["total", "stable", "remapped"]
        )
        summary = result["summary"]
        self.assertEqual(summary["total"], 3)
        self.assertEqual(
            summary["stable"] + summary["remapped"], summary["total"]
        )
        self.assertEqual(summary["remapped"],
                         sum(1 for c in result["cases"] if c["changed"]))
        for value in summary.values():
            self.assertIsInstance(value, int)
            self.assertNotIsInstance(value, bool)
            self.assertGreaterEqual(value, 0)

    def test_digests_match_ct_and_candidate(self):
        results = self.run_ops([
            {"op": "ct"},
            self.load(["a", "b"], 4),
            {"op": "ct"},
            self.hd(["a", "b", "c"], 8, ["k"], 6),
        ])
        result = results[3]
        # base 为操作前（当前配置）指纹，与同批 ct 一致。
        self.assertEqual(result["base"], results[2]["digest"])
        # target 为候选规范化配置的指纹。
        self.assertEqual(
            result["target"],
            digest_of(self.make_config(["a", "b", "c"], 8)),
        )
        self.assertNotEqual(result["base"], result["target"])
        # 首个 ct 是空初始配置，与当前指纹不同。
        self.assertNotEqual(result["base"], results[0]["digest"])

    def test_mappings_match_independent_reference(self):
        keys = ["alpha", "beta", "gamma", "x", "y", "z", "k1", "k2",
                "日本語", "Ω", "repeat", "repeat"]
        results = self.run_ops([
            self.load(["a", "b", "c", "e"], 6),
            self.hd(["a", "b", "c", "d"], 9, keys, 1),
        ])
        cases = results[1]["cases"]
        self.assertEqual([case["key"] for case in cases], keys)
        for case in cases:
            self.assertEqual(
                case["before"],
                self.reference_map(["a", "b", "c", "e"], 6, case["key"]),
            )
            self.assertEqual(
                case["after"],
                self.reference_map(["a", "b", "c", "d"], 9, case["key"]),
            )
        # 重复键结果逐字相同。
        self.assertEqual(cases[-1], cases[-2])
        self.assertEqual(results[1]["summary"]["total"], len(keys))

    def test_identical_rings_are_all_stable(self):
        results = self.run_ops([
            self.load(["a", "b", "c"], 7),
            self.hd(["a", "b", "c"], 7, ["q%d" % i for i in range(20)], 3),
        ])
        result = results[1]
        self.assertEqual(result["base"], result["target"])
        self.assertTrue(all(not case["changed"] for case in result["cases"]))
        self.assertEqual(result["summary"],
                         {"total": 20, "stable": 20, "remapped": 0})

    def test_ignores_health_circuit_and_drain_runtime_state(self):
        # 当前配置登记值：a 阈值 1/1、b 配熔断、c 登记排空时限；运行态上
        # a unhealthy、b 熔断 O、c 排空 D。候选配置与登记值逐字相同——hd
        # 必须忽略这些运行态：三环后端全部上环，前后映射完全一致。
        candidate = self.make_config(
            ["a", "b", "c"], 4,
            backends=[
                self.backend("a", fail=1, success=1),
                self.backend("b", circuit={
                    "n": 1, "m": 1, "r": 1, "w": 1, "q": 1}),
                self.backend("c", drain=10),
            ],
        )
        picks = self.keys_for_each_backend(["a", "b", "c"], 4)
        keys = [picks["a"], picks["b"], picks["c"]]
        results = self.run_ops([
            self.load(["a", "b", "c"], 4),
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 1},
            {"op": "cs", "id": "b", "n": 1, "m": 1, "r": 1, "w": 1, "q": 1},
            {"op": "cr", "id": "b", "ok": False, "now": 2},
            {"op": "ds", "id": "c", "t": 10},
            {"op": "dr", "id": "c", "now": 3},
            {"op": "hd", "config": candidate, "keys": keys, "now": 4},
        ])
        result = results[-1]
        self.assertEqual(result["base"], result["target"])
        # 失格运行态后端在 hd 环上依旧可选，前后均不改变。
        self.assertEqual(
            [(case["before"], case["after"], case["changed"]) for case in
             result["cases"]],
            [("a", "a", False), ("b", "b", False), ("c", "c", False)],
        )

    def test_does_not_apply_candidate(self):
        results = self.run_ops([
            self.load(["a", "b"], 2),
            self.hd(["a", "b", "c"], 9, ["k"], 1),
            {"op": "ce"},
        ])
        config = results[2]["config"]
        self.assertEqual([b["id"] for b in config["backends"]], ["a", "b"])
        self.assertEqual(config["vnodes"], 2)

    def test_keys_size_boundaries(self):
        results = self.run_ops([
            self.load(["a", "b"], 2),
            self.hd(["a", "b"], 2, ["only"], 0),
            self.hd(["a", "b"], 2, ["k%d" % i for i in range(256)], 10 ** 9),
        ])
        self.assertEqual(results[1]["summary"]["total"], 1)
        self.assertEqual(results[2]["summary"]["total"], 256)

    def test_now_boundary_values_are_ok(self):
        results = self.run_ops([
            self.load(["a", "b"], 2),
            self.hd(["a", "b"], 2, ["k"], 0),
            self.hd(["a", "b"], 2, ["k"], 10 ** 9),
        ])
        self.assertEqual(len(results), 3)

    def test_state_when_ring_unavailable(self):
        # 初始空状态：当前 vnodes 为 null（先 ci 置时钟亦可复现，此处直接
        # 在 now=0 调用）。
        self.assert_failure(
            encode_ops([self.hd(["a"], 2, ["k"], 0)]), 4, "STATE"
        )
        # 当前 vnodes 为 null（add 但未 chash/ci 配环）。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "a", "weight": 1},
                self.hd(["a", "b"], 2, ["k"], 0),
            ]),
            4, "STATE",
        )
        # ci 载入 vnodes=null 的配置。
        self.assert_failure(
            encode_ops([
                self.load(["a"], None),
                self.hd(["a"], 2, ["k"], 1),
            ]),
            4, "STATE",
        )
        # 候选 vnodes 为 null。
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                self.hd(["a"], None, ["k"], 1),
            ]),
            4, "STATE",
        )
        # 候选 backends 为空。
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                self.hd([], 2, ["k"], 1),
            ]),
            4, "STATE",
        )
        # 当前 backends 为空（ci 允许空 backends 配 vnodes）。
        self.assert_failure(
            encode_ops([
                self.load([], 2),
                self.hd(["a"], 2, ["k"], 1),
            ]),
            4, "STATE",
        )

    def test_unknown_backend_references_are_backend(self):
        base = [self.load(["a"], 2)]
        bad_limit = self.make_config(
            ["a"], 2,
            limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}])
        self.assert_failure(
            encode_ops(base + [
                {"op": "hd", "config": bad_limit, "keys": ["k"], "now": 1}]),
            3, "BACKEND",
        )
        bad_quota = self.make_config(
            ["a"], 2,
            quotas=[{"scope": "B", "id": "ghost", "limit": 1, "span": 1}])
        self.assert_failure(
            encode_ops(base + [
                {"op": "hd", "config": bad_quota, "keys": ["k"], "now": 1}]),
            3, "BACKEND",
        )
        bad_fault = self.make_config(
            ["a"], 2,
            faults=[{"id": "ghost", "k": "D", "a": 0, "z": 5, "v": 0}])
        self.assert_failure(
            encode_ops(base + [
                {"op": "hd", "config": bad_fault, "keys": ["k"], "now": 1}]),
            3, "BACKEND",
        )

    def test_backend_takes_priority_over_state(self):
        # 同一候选既引用未知 B 限流、vnodes 又为 null：BACKEND 先于 STATE。
        bad = self.make_config(
            ["a"], None,
            limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}])
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                {"op": "hd", "config": bad, "keys": ["k"], "now": 1},
            ]),
            3, "BACKEND",
        )

    def test_exact_key_order(self):
        config = self.make_config(["a"], 2)

        def raw(segment):
            return ('{"ops":[' + segment + ']}').encode("utf-8")

        canonical = '{"op":"hd","config":%s,"keys":["k"],"now":0}' % (
            json.dumps(config, separators=(",", ":")))
        # 键序乱序（now 提前、config/keys 交换）均 INPUT。
        self.assert_failure(
            raw('{"op":"hd","now":0,"config":%s,"keys":["k"]}'
                % json.dumps(config, separators=(",", ":"))),
            2, "INPUT",
        )
        self.assert_failure(
            raw('{"op":"hd","config":%s,"now":0,"keys":["k"]}'
                % json.dumps(config, separators=(",", ":"))),
            2, "INPUT",
        )
        # 多余键、缺键。
        self.assert_failure(
            raw(canonical[:-1] + ',"x":1}'), 2, "INPUT"
        )
        self.assert_failure(
            raw('{"op":"hd","config":%s,"now":0}'
                % json.dumps(config, separators=(",", ":"))),
            2, "INPUT",
        )

    def test_invalid_keys_are_input(self):
        config = self.make_config(["a"], 2)

        def raw(keys_segment):
            return encode_ops([{
                "op": "hd", "config": config, "keys": json.loads(keys_segment),
                "now": 0,
            }])

        self.assert_failure(raw("[]"), 2, "INPUT")
        self.assert_failure(raw(json.dumps(["k"] * 257)), 2, "INPUT")
        self.assert_failure(raw('[""]'), 2, "INPUT")
        self.assert_failure(raw("[1]"), 2, "INPUT")
        self.assert_failure(raw("[true]"), 2, "INPUT")
        self.assert_failure(raw('[["k"]]'), 2, "INPUT")
        self.assert_failure(raw("null"), 2, "INPUT")
        self.assert_failure(raw('"k"'), 2, "INPUT")
        # 孤立代理项不可直接 UTF-8 编码：INPUT。
        config_json = json.dumps(config, separators=(",", ":"))
        self.assert_failure(
            ('{"ops":[{"op":"hd","config":%s,"keys":["\\ud800"],"now":0}]}'
             % config_json).encode("utf-8"),
            2, "INPUT",
        )

    def test_invalid_now_is_input(self):
        for bad_now in (True, -1, 10 ** 9 + 1, "0", 1.5, None):
            self.assert_failure(
                encode_ops([{
                    "op": "hd", "config": self.make_config(["a"], 2),
                    "keys": ["k"], "now": bad_now,
                }]),
                2, "INPUT",
            )

    def test_invalid_config_is_input(self):
        # 重复后端 id：config 校验同 cv/cd。
        config = self.make_config(["a", "a"], 2)
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                {"op": "hd", "config": config, "keys": ["k"], "now": 1},
            ]),
            2, "INPUT",
        )

    def test_clock_regression_input_and_advances_clock(self):
        # 时钟倒退 INPUT；相等 now 合法。
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                self.hd(["a"], 2, ["k"], 10),
                self.hd(["a"], 2, ["k"], 9),
            ]),
            2, "INPUT",
        )
        results = self.run_ops([
            self.load(["a"], 2),
            self.hd(["a"], 2, ["k"], 10),
            self.hd(["a"], 2, ["k"], 10),
        ])
        self.assertEqual(len(results), 3)
        # 成功 hd 推进共用时钟：其后旧时刻 br 报倒退。
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                self.hd(["a"], 2, ["k"], 10),
                {"op": "br", "now": 9},
            ]),
            2, "INPUT",
        )

    def test_failure_rolls_back_batch(self):
        # 靠后的 hd 非法：整批无 stdout，前面成功的 ci 也不落任何状态。
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                self.hd(["a"], 2, ["k"], 1),
                self.hd(["a"], 2, ["k"], "x"),
            ]),
            2, "INPUT",
        )
        # 靠后的 hd 因 STATE 失败同样整批回滚。
        self.assert_failure(
            encode_ops([
                self.load(["a"], 2),
                self.hd(["a"], 2, ["k"], 1),
                self.hd(["a"], None, ["k"], 2),
            ]),
            4, "STATE",
        )

    def test_non_ascii_keys_compact_single_newline(self):
        code, out, err = run_balancer(
            "run",
            encode_ops([
                self.load(["a", "b"], 2),
                self.hd(["a", "b"], 2, ["日本語", "Ω", "emoji😀"], 1),
            ]),
        )
        self.assertEqual((code, err), (0, b""))
        self.assertTrue(out.endswith(b"\n"))
        self.assertEqual(out.count(b"\n"), 1)
        # 非 ASCII 不转义、无多余空白。
        self.assertIn("日本語".encode("utf-8"), out)
        self.assertIn("😀".encode("utf-8"), out)
        self.assertNotIn(b"\\u", out)
        self.assertNotIn(b" ", out)

    def test_record_replay_covers_hd(self):
        ops = [
            self.load(["a", "b"], 2),
            self.hd(["a", "b", "c"], 9, ["k1", "k2", "k2"], 1),
            {"op": "ct"},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, base64.b64decode(record["stderr"]))
        self.assertEqual(rep_stderr, run_stderr)
        # 失败 hd（STATE）同样逐字节覆盖。
        failing = encode_ops([self.hd(["a"], 2, ["k"], 0)])
        _, rec_fail, _ = run_balancer("record", failing)
        record = json.loads(rec_fail.decode("utf-8"))
        rep_code, _, rep_stderr = run_balancer("replay", rec_fail)
        self.assertEqual((rep_code, record["exit"]), (4, 4))
        self.assertEqual(rep_stderr, b'{"error":"STATE"}\n')


class PerBackendCapOverrideTest(unittest.TestCase):
    """pc/pg：每后端接纳容量覆盖；oa/ot 接纳与 oi/od/oq 投影用有效容量。"""

    def run_ops(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(err, b"")
        self.assertEqual(code, 0)
        return code, out, err

    def results(self, ops):
        _, out, _ = self.run_ops(ops)
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, ops, exit_code, label):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def setup(self, cap=3, q=5, ttl=10, ids=("b",)):
        ops = [{"op": "add", "id": backend_id, "weight": 1}
               for backend_id in ids]
        ops.append({"op": "chash", "vnodes": 1})
        ops.append({"op": "os", "cap": cap, "q": q, "ttl": ttl})
        return ops

    def oa(self, cid, now=0):
        return {"op": "oa", "cid": cid, "flow": FLOW, "c": "c",
                "s": "s", "key": "k", "now": now}

    def test_pc_pg_ok_and_exact_key_order(self):
        code, out, err = self.run_ops([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 3},
            {"op": "pg", "id": "b"},
        ])
        results = json.loads(out.decode("utf-8"))["results"]
        self.assertEqual(results[1], {"op": "pc", "ok": True})
        self.assertEqual(
            results[2],
            {"op": "pg", "id": "b", "cap": 3,
             "connections": 0, "available": 3},
        )
        # 紧凑 JSON 固定键序逐字节。
        self.assertIn(
            b'{"op":"pg","id":"b","cap":3,"connections":0,"available":3}',
            out,
        )
        self.assertTrue(out.endswith(b"\n"))

    def test_same_value_idempotent_different_overrides(self):
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 3},
            {"op": "pc", "id": "b", "cap": 3},
            {"op": "pg", "id": "b"},
            {"op": "pc", "id": "b", "cap": 7},
            {"op": "pg", "id": "b"},
        ])
        self.assertEqual(results[3]["cap"], 3)
        self.assertEqual(results[5]["cap"], 7)

    def test_pc_works_without_os(self):
        # 覆盖登记不要求已 os。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 2},
            {"op": "pg", "id": "b"},
        ])
        self.assertEqual(results[2]["cap"], 2)

    def test_cap_bounds_and_types(self):
        base = [{"op": "add", "id": "b", "weight": 1}]
        for cap in (0, -1, 10 ** 6 + 1, True, 1.0, "1", None):
            self.assert_failure(
                base + [{"op": "pc", "id": "b", "cap": cap}], 2, "INPUT"
            )

    def test_pc_pg_key_order_and_keyset_input(self):
        for op in (
            {"op": "pc", "id": "b"},
            {"op": "pc", "id": "b", "cap": 1, "x": 0},
            {"op": "pc", "cap": 1},
            {"op": "pc", "id": "", "cap": 1},
            {"cap": 1, "id": "b", "op": "pc"},
            {"op": "pg", "id": ""},
            {"op": "pg"},
            {"op": "pg", "id": "b", "x": 0},
            {"id": "b", "op": "pg"},
        ):
            self.assert_failure([op], 2, "INPUT")

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            [{"op": "pc", "id": "z", "cap": 1}], 3, "BACKEND"
        )
        self.assert_failure([{"op": "pg", "id": "z"}], 3, "BACKEND")

    def test_error_precedence_input_before_backend(self):
        # cap 越界先于未知 id：INPUT/2。
        self.assert_failure(
            [{"op": "pc", "id": "z", "cap": 0}], 2, "INPUT"
        )
        # 键序非法先于未知 id：INPUT/2。
        self.assert_failure(
            [{"id": "z", "op": "pg"}], 2, "INPUT"
        )

    def test_pg_without_override_is_state(self):
        self.assert_failure([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pg", "id": "b"},
        ], 4, "STATE")

    def test_lower_cap_keeps_connections_and_clamps_available(self):
        results = self.results(self.setup(cap=3) + [
            {"op": "open", "cid": "c1", "flow": FLOW, "now": 0},
            {"op": "open", "cid": "c2", "flow": FLOW, "now": 0},
            {"op": "pc", "id": "b", "cap": 1},
            {"op": "pg", "id": "b"},
        ])
        # 不关闭既有连接：connections=2，available 截零不为负。
        self.assertEqual(
            results[-1],
            {"op": "pg", "id": "b", "cap": 1,
             "connections": 2, "available": 0},
        )

    def test_lower_cap_blocks_new_oa_until_release(self):
        ops = self.setup(cap=3) + [
            self.oa("c1"),
            {"op": "pc", "id": "b", "cap": 1},
            self.oa("c2"),                       # 满载入队 Q
        ]
        results = self.results(ops)
        self.assertEqual(results[3]["state"], "A")
        self.assertEqual(results[5]["state"], "Q")
        # 连接释放后 ot 自然恢复接纳，不主动动 FIFO 项。
        ops += [
            {"op": "close", "cid": "c1", "now": 5},
            {"op": "ot", "now": 6},
        ]
        results = self.results(ops)
        self.assertEqual(results[-1]["admitted"], ["c2"])

    def test_raise_cap_admits_beyond_os_cap(self):
        results = self.results(self.setup(cap=1) + [
            {"op": "pc", "id": "b", "cap": 2},
            self.oa("c1"),
            self.oa("c2"),
            self.oa("c3"),
        ])
        states = [r["state"] for r in results[-3:]]
        self.assertEqual(states, ["A", "A", "Q"])

    def test_ot_admits_after_cap_raised(self):
        results = self.results(self.setup(cap=2, ttl=100) + [
            self.oa("c1"),
            {"op": "pc", "id": "b", "cap": 1},
            self.oa("c2"),                       # Q
            {"op": "pc", "id": "b", "cap": 3},
            {"op": "ot", "now": 5},
        ])
        self.assertEqual(results[5]["state"], "Q")
        self.assertEqual(
            results[-1], {"op": "ot", "expired": [], "admitted": ["c2"]}
        )

    def test_oi_od_capacity_uses_effective_cap(self):
        oi_op = {"op": "oi", "key": "k", "c": "c", "s": "s",
                 "bc": 1, "cc": 1, "sc": 1, "timeout": 5, "max": 3, "now": 0}
        od_op = dict(oi_op, op="od")
        results = self.results(self.setup(cap=5) + [
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "pc", "id": "b", "cap": 1},
            oi_op,
            od_op,
        ])
        oi_res = [r for r in results if r["op"] == "oi"][0]
        od_res = [r for r in results if r["op"] == "od"][0]
        self.assertEqual(oi_res["state"], "R")
        self.assertEqual(oi_res["capacity"], 1)
        self.assertEqual(od_res["trace"][0]["result"], "C")
        # 提高覆盖后投影恢复 A。
        results = self.results(self.setup(cap=5) + [
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "pc", "id": "b", "cap": 1},
            {"op": "pc", "id": "b", "cap": 5},
            oi_op,
        ])
        self.assertEqual(results[-1]["state"], "A")

    def test_oq_blocked_c_uses_effective_cap(self):
        results = self.results(self.setup(cap=5, ttl=100) + [
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "pc", "id": "b", "cap": 1},
            self.oa("q1"),
            {"op": "oq", "now": 0},
        ])
        item = results[-1]["items"][0]
        self.assertEqual(item["cid"], "q1")
        self.assertEqual(item["backend"], "b")
        self.assertEqual(item["blocked"], ["C"])
        # 调高后不再 C。
        results = self.results(self.setup(cap=5, ttl=100) + [
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "pc", "id": "b", "cap": 1},
            self.oa("q1"),
            {"op": "pc", "id": "b", "cap": 5},
            {"op": "oq", "now": 1},
        ])
        self.assertEqual(results[-1]["items"][0]["blocked"], [])

    def test_remove_deletes_override_and_readd_does_not_inherit(self):
        self.assert_failure([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 7},
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pg", "id": "b"},
        ], 4, "STATE")

    def test_failed_batch_rolls_back_override(self):
        # 前序 pc 成功，后续 BACKEND 失败：整批无 stdout、无部分生效。
        code, stdout, stderr = run_balancer("run", encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 2},
            {"op": "pg", "id": "nope"},
        ]))
        self.assertEqual(code, 3)
        self.assertEqual(stdout, b"")
        self.assertEqual(stderr, b'{"error":"BACKEND"}\n')

    def ci_config(self):
        return {
            "version": 9,
            "backends": [{
                "id": "b", "weight": 1, "d": 0, "fail": 3, "success": 2,
                "circuit": None, "drain": None, "endpoint": None,
            }],
            "vnodes": None, "limits": [], "overload": None,
            "sticky": None, "idle": None, "backpressure": None,
            "scheduler": {"pick": "W"}, "faults": [], "quotas": [],
            "queue": {"dequeue": "F", "full": "T"},
        }

    def test_ci_clears_override_and_ce_does_not_export_it(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 4},
            {"op": "ce"},
        ]
        _, out, _ = self.run_ops(ops)
        config = json.loads(out.decode("utf-8"))["results"][-1]["config"]
        # 覆盖为纯运行态，不出现在 ce 导出。
        self.assertNotIn("cap_overrides", config)
        self.assertNotIn("pc", json.dumps(config))
        self.assert_failure(ops + [
            {"op": "ci", "config": self.ci_config(), "now": 0},
            {"op": "pg", "id": "b"},
        ], 4, "STATE")

    def test_cb_clears_override(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 2},
            {"op": "ci", "config": self.ci_config(), "now": 0},
            {"op": "cl"},
        ]
        _, out, _ = self.run_ops(ops)
        rev = json.loads(out.decode("utf-8"))["results"][-1]["current"]
        self.assert_failure(ops + [
            {"op": "cb", "rev": rev, "now": 1},
            {"op": "pg", "id": "b"},
        ], 4, "STATE")

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 2},
            {"op": "pg", "id": "b"},
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, b"")


if __name__ == "__main__":
    unittest.main()
