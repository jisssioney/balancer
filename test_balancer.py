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


def config_v10(weight, faults=(), quotas=(), dequeue="F", full="T",
               capacities=(), **overrides):
    """最小 version=10 配置：十二键同 v9 且末置 capacities（id,cap 数组）。"""
    config = config_v8(weight, faults, quotas, **overrides)
    config["version"] = 10
    config["queue"] = {"dequeue": dequeue, "full": full}
    config["capacities"] = list(capacities)
    return config


def config_v11(weight, faults=(), quotas=(), dequeue="F", full="T",
               capacities=(), lifetime=None, **overrides):
    """最小 version=11 配置：十三键同 v10 且末置 lifetime（null 或
    {"ttl":整数}）。"""
    config = config_v10(
        weight, faults, quotas, dequeue, full, capacities, **overrides
    )
    config["version"] = 11
    config["lifetime"] = (
        None if lifetime is None else {"ttl": lifetime}
    )
    return config


def config_v9_input(weight, faults=(), quotas=(), dequeue="F", full="T",
                    **overrides):
    """最小 version=9 输入：十一键同 v8 且末置 queue（dequeue,full）。"""
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

    def test_ci_commits_normalized_v11(self):
        # version=1 旧结构成功加载后，提交为规范化 version=11 配置
        # （faults 空计划、quotas 空数组、queue 默认 F/T、capacities 空、
        # lifetime=null）。
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
            config_v11(2),
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
        # 回滚后当前配置即 rev=1 的规范化 v11 快照（faults、quotas 均空，
        # queue 默认 F/T、capacities 空、lifetime=null）。
        self.assertEqual(results[3]["config"], config_v11(1))

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
        # v10 候选仍合法，预检回显规范化 v11（追加末置 lifetime=null）。
        code, out = self.run_ops(
            [{"op": "cv", "config": config_v10(2), "now": 5}]
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
        self.assertEqual(result["config"], config_v11(2))

    def test_cv_normalizes_v1_to_v11(self):
        # version=1 旧结构回显为规范化 version=11（同 ci 提交格式）。
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
        self.assertEqual(result["config"], config_v11(2))

    def test_cv_config_matches_ci_export(self):
        # 富 v10 配置：cv 回显与 ci 成功后 ce 导出逐字节同构。
        config = config_v10(
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
            {"op": "cv", "config": config_v10(2), "now": 0},
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
            {"op": "cv", "config": config_v10(2), "now": 1},
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
            {"op": "cv", "config": config_v10(2), "now": 1},
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
        bad_limit = config_v10(
            1, limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}]
        )
        bad_quota = config_v10(
            1, quotas=[{"scope": "B", "id": "ghost", "limit": 1, "span": 1}]
        )
        bad_fault = config_v10(
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
        bad_version = config_v10(1)
        bad_version["version"] = 8
        missing = config_v10(1)
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
                encode_ops([{"op": "cv", "config": config_v10(1), "now": now}]),
            )
            self.assertEqual(
                (code, out, err), (2, b"", b'{"error":"INPUT"}\n'), now
            )

    def test_cv_exact_key_set(self):
        code, out, err = run_balancer(
            "run",
            encode_ops(
                [{"op": "cv", "config": config_v10(1), "now": 0, "x": 1}]
            ),
        )
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))
        code, out, err = run_balancer(
            "run", encode_ops([{"op": "cv", "config": config_v10(1)}])
        )
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))

    def test_cv_clock_regression_is_input(self):
        ops = [
            {"op": "cv", "config": config_v10(1), "now": 10},
            {"op": "cv", "config": config_v10(2), "now": 5},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))
        # 同刻不重拨：相等 now 合法。
        ops = [
            {"op": "cv", "config": config_v10(1), "now": 10},
            {"op": "cv", "config": config_v10(2), "now": 10},
        ]
        code, out = self.run_ops(ops)
        self.assertEqual(code, 0)

    def test_cv_failure_rolls_back_batch(self):
        # 批内靠后的 cv 失败：整批无 stdout，前面成功的 cv 也不落任何状态。
        ops = [
            {"op": "cv", "config": config_v10(1), "now": 0},
            {"op": "cv", "config": config_v10(2), "now": 5},
            {"op": "cv", "config": config_v10(3), "now": 1},
        ]
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, out, err), (2, b"", b'{"error":"INPUT"}\n'))

    def test_record_replay_covers_cv(self):
        ops = [
            {"op": "cv", "config": config_v10(2), "now": 0},
            {"op": "ci", "config": config_v6(1), "now": 1},
            {"op": "cv", "config": config_v10(3), "now": 2},
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
        empty_config = config_v11(1)
        empty_config["backends"] = []
        self.assertEqual(result["digest"], digest_of(empty_config))

    def test_ct_matches_ce_config(self):
        # ct 指纹即同一批内 ce.config 规范化 version=10 对象的摘要。
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
                [{"op": "cv", "now": 0, "config": config_v10(1)}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops(
                [{"config": config_v10(1), "op": "cv", "now": 0}]
            ),
            2, "INPUT",
        )

    def test_ci_base_success_and_output(self):
        # ct 取指纹后 ci 携带匹配 base：沿用全部成功语义与 op,ok=true 输出。
        results = self.run_ops([{"op": "ct"}])
        digest = results[0]["digest"]
        results = self.run_ops([
            {"op": "ci", "config": config_v10(2), "base": digest, "now": 0},
            {"op": "cl"},
        ])
        self.assertEqual(results[0], {"op": "ci", "ok": True})
        self.assertEqual(results[1]["current"], 1)

    def test_ci_base_stale_is_state(self):
        # 配置变更后旧指纹即过期：同一批内第二次携带旧 base 报 STATE/4。
        results = self.run_ops(
            [{"op": "ci", "config": config_v10(1), "now": 0}, {"op": "ct"}]
        )
        digest = results[1]["digest"]
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            {"op": "ci", "config": config_v10(2), "base": digest, "now": 1},
            {"op": "ci", "config": config_v10(3), "base": digest, "now": 2},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_ci_base_mismatch_is_state(self):
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v10(1), "base": "0" * 64,
                 "now": 0},
            ]),
            4, "STATE",
        )

    def test_ci_base_precedes_connection_check(self):
        # base 不等先于活动连接检查报 STATE；base 相等时活动连接仍报 STATE。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            {"op": "ci", "config": config_v10(1), "base": "0" * 64, "now": 1},
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
            {"op": "ci", "config": config_v10(1), "base": live_digest,
             "now": 1},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_ci_base_backend_precedes_state(self):
        # 未知 B 限流引用仍报 BACKEND/3，先于 base 比较。
        bad = config_v10(
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
                    {"op": "ci", "config": config_v10(1), "base": bad,
                     "now": 0},
                ]),
                2, "INPUT",
            )

    def test_ci_base_exact_key_order(self):
        # 四键形式须精确按 op,config,base,now 出现，乱序报 INPUT/2。
        self.assert_failure(
            encode_ops([
                {"op": "ci", "base": "0" * 64, "config": config_v10(1),
                 "now": 0},
            ]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": config_v10(1), "now": 0,
                 "base": "0" * 64},
            ]),
            2, "INPUT",
        )

    def test_ci_three_key_form_any_order(self):
        # 原三键形式保留：仅键集匹配，键序不限。
        results = self.run_ops(
            [{"now": 0, "op": "ci", "config": config_v10(1)}]
        )
        self.assertEqual(results[0], {"op": "ci", "ok": True})

    def test_ci_base_clock_regression_is_input(self):
        # base 形式的 now 沿用共用非递减时钟。
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 5},
            {"op": "ci", "config": config_v10(2), "base": "0" * 64, "now": 3},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_ci_base_failure_rolls_back_batch(self):
        # base 不等的 ci 失败：整批无 stdout，前面成功的 ci 也不落任何状态。
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            {"op": "ci", "config": config_v10(2), "base": "0" * 64, "now": 1},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_record_replay_covers_ct_and_base(self):
        results = self.run_ops([{"op": "ct"}])
        digest = results[0]["digest"]
        ops = [
            {"op": "ct"},
            {"op": "ci", "config": config_v10(1), "base": digest, "now": 0},
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
                    {"op": "ci", "config": config_v10(1), "base": digest,
                     "now": bad},
                ]),
                2, "INPUT",
            )
        # 上界 10^9 本身合法。
        results = self.run_ops([
            {"op": "ci", "config": config_v10(1), "base": digest,
             "now": 10 ** 9},
        ])
        self.assertEqual(results[0], {"op": "ci", "ok": True})

    def test_ci_base_bad_now_rolls_back_batch(self):
        # base 形式 now 非法：整批无 stdout，前面成功的 ci 也不落状态。
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            {"op": "ci", "config": config_v10(2), "base": "0" * 64,
             "now": 10 ** 9 + 1},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_ci_three_key_now_still_unbounded(self):
        # 原三键形式的 now 无上界（仅非负非 bool 整数），行为不变。
        results = self.run_ops(
            [{"op": "ci", "config": config_v10(1), "now": 10 ** 9 + 5}]
        )
        self.assertEqual(results[0], {"op": "ci", "ok": True})


class ConfigSectionHotReloadTest(unittest.TestCase):
    """单字段配置热加载 cu：以当前配置为底稿仅替换一个顶层段。"""

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

    def cu(self, base, section, value, now):
        return {
            "op": "cu", "base": base, "section": section,
            "value": value, "now": now,
        }

    def test_replace_vnodes_output_and_commit(self):
        base = digest_of(config_v11(1))
        results = self.run_ops(
            [
                {"op": "ci", "config": config_v11(1), "now": 0},
                self.cu(base, "vnodes", 4, 1),
                {"op": "ce"},
                {"op": "cl"},
            ]
        )
        result = results[1]
        # 结果精确键序 op,base,target,rev,ok。
        self.assertEqual(
            list(result), ["op", "base", "target", "rev", "ok"]
        )
        self.assertEqual(result["op"], "cu")
        self.assertEqual(result["base"], base)
        expected = config_v11(1, vnodes=4)
        self.assertEqual(result["target"], digest_of(expected))
        self.assertEqual(result["rev"], 2)
        self.assertIs(result["ok"], True)
        # 当前配置只替换 vnodes，其余段不变。
        self.assertEqual(results[2]["config"], expected)
        # 成功另建提交 rev=2，快照即替换后配置。
        self.assertEqual(results[3]["current"], 2)
        self.assertEqual(len(results[3]["commits"]), 2)
        self.assertEqual(results[3]["commits"][1]["rev"], 2)
        self.assertEqual(results[3]["commits"][1]["config"], expected)

    def test_works_from_initial_empty_state(self):
        # 初始空配置即可作为底稿：ct 取指纹后用 backends 段整体载入。
        results = self.run_ops([{"op": "ct"}])
        base = results[0]["digest"]
        backends = config_v11(1)["backends"]
        results = self.run_ops(
            [self.cu(base, "backends", backends, 0), {"op": "ce"}]
        )
        self.assertEqual(results[0]["rev"], 1)
        self.assertEqual(results[1]["config"], config_v11(1))

    def test_unchanged_value_still_creates_rev(self):
        base = digest_of(config_v11(1))
        results = self.run_ops(
            [
                {"op": "ci", "config": config_v11(1), "now": 0},
                self.cu(base, "vnodes", None, 1),
                {"op": "cl"},
            ]
        )
        # 值未变：base 与 target 相同，但仍新建 rev。
        self.assertEqual(results[1]["base"], results[1]["target"])
        self.assertEqual(results[1]["rev"], 2)
        self.assertEqual(results[2]["current"], 2)

    def test_chained_cu_uses_fresh_digest(self):
        # 段替换逐条生效：后一条 cu 的 base 为前一条的 target。
        base1 = digest_of(config_v11(1))
        mid = config_v11(1, vnodes=4)
        base2 = digest_of(mid)
        final = config_v11(1, vnodes=4, sticky={"ttl": 9})
        results = self.run_ops(
            [
                {"op": "ci", "config": config_v11(1), "now": 0},
                self.cu(base1, "vnodes", 4, 1),
                self.cu(base2, "sticky", {"ttl": 9}, 2),
                {"op": "ce"},
            ]
        )
        self.assertEqual(results[1]["target"], base2)
        self.assertEqual(results[2]["target"], digest_of(final))
        self.assertEqual((results[1]["rev"], results[2]["rev"]), (2, 3))
        self.assertEqual(results[3]["config"], final)

    def test_replace_backends_drops_old_backend(self):
        # backends 段整体替换：旧 id a 消失，新 id b 生效，其余段保持。
        base = digest_of(config_v11(1))
        new_backends = [
            {
                "id": "b", "weight": 2, "d": 0, "fail": 1, "success": 1,
                "circuit": None, "drain": None, "endpoint": None,
            }
        ]
        results = self.run_ops(
            [
                {"op": "ci", "config": config_v11(1), "now": 0},
                self.cu(base, "backends", new_backends, 1),
                {"op": "hget", "id": "b"},
            ]
        )
        target = config_v11(1)
        target["backends"] = new_backends
        self.assertEqual(results[1]["target"], digest_of(target))
        self.assertEqual(results[2]["id"], "b")

    def test_runtime_rebuilt_after_section_replace(self):
        # limits 段被替换为空：旧 B 桶消失（lg 报 STATE），默认运行态重建。
        with_limits = config_v11(
            1, limits=[{"scope": "B", "id": "a", "r": 3, "b": 9}]
        )
        base = digest_of(with_limits)
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": with_limits, "now": 0},
                    self.cu(base, "limits", [], 1),
                    {"op": "lg", "scope": "B", "id": "a", "now": 1},
                ]
            ),
            4, "STATE",
        )
        # 替换前同配置桶存在且满令牌。
        results = self.run_ops(
            [
                {"op": "ci", "config": with_limits, "now": 0},
                {"op": "lg", "scope": "B", "id": "a", "now": 0},
            ]
        )
        self.assertEqual((results[1]["r"], results[1]["b"], results[1]["t"]),
                         (3, 9, 9))

    def test_cu_clears_reservation(self):
        candidate = config_v11(1, vnodes=4)
        base = digest_of(config_v11(1))
        results = self.run_ops(
            [
                {"op": "ci", "config": config_v11(1), "now": 0},
                {"op": "cp", "config": candidate, "at": 10, "now": 1},
                self.cu(base, "sticky", {"ttl": 5}, 2),
                {"op": "cq", "now": 2},
            ]
        )
        # cu 成功清除既有配置预约。
        self.assertEqual(results[3]["pending"], False)
        self.assertIsNone(results[3]["digest"])

    def test_eviction_keeps_recent_16(self):
        # 16 次 ci 占 rev 1..16，再 cu 建 rev 17：历史仅留 2..17。
        ops = [
            {"op": "ci", "config": config_v11(i), "now": i}
            for i in range(1, 17)
        ]
        ops.append(self.cu(digest_of(config_v11(16)), "vnodes", 7, 16))
        ops.append({"op": "cl"})
        results = self.run_ops(ops)
        self.assertEqual(results[-1]["current"], 17)
        self.assertEqual(
            [commit["rev"] for commit in results[-1]["commits"]],
            list(range(2, 18)),
        )
        self.assertEqual(results[-2]["target"],
                         digest_of(config_v11(16, vnodes=7)))

    def test_stale_base_is_state(self):
        base = digest_of(config_v11(1))
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": config_v11(1), "now": 0},
                    self.cu(base, "vnodes", 4, 1),
                    self.cu(base, "vnodes", 5, 2),
                ]
            ),
            4, "STATE",
        )

    def test_base_check_precedes_connection_check(self):
        # base 不匹配先于活动连接检查报 STATE。
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": config_v11(1), "now": 0},
                    {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
                    self.cu("0" * 64, "vnodes", 4, 2),
                ]
            ),
            4, "STATE",
        )

    def test_active_connection_is_state(self):
        base = digest_of(config_v11(1))
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": config_v11(1), "now": 0},
                    {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
                    self.cu(base, "vnodes", 4, 2),
                ]
            ),
            4, "STATE",
        )

    def test_queued_item_is_state(self):
        cfg = config_v11(1, vnodes=1,
                         overload={"cap": 1, "q": 2, "ttl": 10})
        base = digest_of(cfg)
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": cfg, "now": 0},
                    {"op": "open", "cid": "y", "flow": self.FLOW, "now": 1},
                    {"op": "oa", "cid": "z", "flow": self.FLOW,
                     "c": "k", "s": "k", "key": "k", "now": 1},
                    self.cu(base, "vnodes", 2, 2),
                ]
            ),
            4, "STATE",
        )

    def test_unknown_backend_references_are_backend(self):
        base = digest_of(config_v11(1))
        # B 限流、B 配额、faults、capacities 引用未知候选后端均 BACKEND。
        cases = [
            ("limits", [{"scope": "B", "id": "ghost", "r": 1, "b": 1}]),
            ("quotas", [{"scope": "B", "id": "ghost",
                         "limit": 1, "span": 1}]),
            ("faults", [{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}]),
            ("capacities", [{"id": "ghost", "cap": 1}]),
        ]
        for section, value in cases:
            self.assert_failure(
                encode_ops(
                    [
                        {"op": "ci", "config": config_v11(1), "now": 0},
                        self.cu(base, section, value, 1),
                    ]
                ),
                3, "BACKEND",
            )

    def test_backend_precedes_base_check(self):
        # 未知后端引用（BACKEND）先于 base 不匹配（STATE）判定。
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": config_v11(1), "now": 0},
                    self.cu(
                        "0" * 64, "faults",
                        [{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}],
                        1,
                    ),
                ]
            ),
            3, "BACKEND",
        )

    def test_exact_key_order_required(self):
        base = digest_of(config_v11(1))
        # 乱序键序一律 INPUT（键集相同）。
        for raw in (
            b'{"ops":[{"op":"cu","section":"vnodes","base":"' + base.encode()
            + b'","value":4,"now":1}]}',
            b'{"ops":[{"op":"cu","base":"' + base.encode()
            + b'","now":1,"section":"vnodes","value":4}]}',
            b'{"ops":[{"op":"cu","base":"' + base.encode()
            + b'","section":"vnodes","value":4,"now":1,"x":1}]}',
        ):
            self.assert_failure(raw, 2, "INPUT")

    def test_bad_section_is_input(self):
        base = digest_of(config_v11(1))
        for section in ("version", "Vnodes", "nope", 1, None, True):
            self.assert_failure(
                encode_ops(
                    [self.cu(base, section, None, 0)]
                ),
                2, "INPUT",
            )

    def test_bad_base_format_is_input(self):
        for bad in ("0" * 63, "g" * 64, "A" * 64, "0" * 64 + "0", 1, True):
            self.assert_failure(
                encode_ops([self.cu(bad, "vnodes", 4, 0)]),
                2, "INPUT",
            )

    def test_bad_now_is_input(self):
        base = digest_of(config_v11(1))
        for now in (-1, 10 ** 9 + 1, True, "1", 1.5, None):
            self.assert_failure(
                encode_ops([self.cu(base, "vnodes", 4, now)]),
                2, "INPUT",
            )

    def test_clock_regression_is_input(self):
        base = digest_of(config_v11(1))
        self.assert_failure(
            encode_ops(
                [
                    {"op": "ci", "config": config_v11(1), "now": 10},
                    self.cu(base, "vnodes", 4, 5),
                ]
            ),
            2, "INPUT",
        )

    def test_bad_value_structure_is_input(self):
        base = digest_of(config_v11(1))
        cfg_ops = [
            {"op": "ci", "config": config_v11(1), "now": 0},
        ]
        cases = [
            # vnodes：bool 不是整数、超范围。
            ("vnodes", True),
            ("vnodes", 1025),
            # scheduler：非法 pick；H 而 vnodes 为 null 的交叉约束。
            ("scheduler", {"pick": "X"}),
            ("scheduler", {"pick": "H"}),
            # queue：键序乱、枚举非法。
            ("queue", {"full": "T", "dequeue": "F"}),
            ("queue", {"dequeue": "P", "full": "T"}),
            # lifetime：ttl 越界与错误包装。
            ("lifetime", {"ttl": 0}),
            ("lifetime", 7),
            # backpressure 交叉约束：overload 为 null 时不可登记。
            ("backpressure", {"low": 1, "high": 2}),
            # sticky/idle ttl 范围。
            ("sticky", {"ttl": 0}),
            ("idle", {"ttl": -1}),
            # backends：重复 id 与缺 endpoint 键。
            ("backends", [
                {"id": "a", "weight": 1, "d": 0, "fail": 3, "success": 2,
                 "circuit": None, "drain": None, "endpoint": None},
                {"id": "a", "weight": 1, "d": 0, "fail": 3, "success": 2,
                 "circuit": None, "drain": None, "endpoint": None},
            ]),
            ("backends", [
                {"id": "a", "weight": 1, "d": 0, "fail": 3, "success": 2,
                 "circuit": None, "drain": None},
            ]),
            # faults：同 id 段重叠。
            ("faults", [
                {"id": "a", "k": "D", "a": 0, "z": 5, "v": 0},
                {"id": "a", "k": "D", "a": 4, "z": 8, "v": 0},
            ]),
            # quotas：未按 scope/id 排序。
            ("quotas", [
                {"scope": "S", "id": "x", "limit": 1, "span": 1},
                {"scope": "B", "id": "a", "limit": 1, "span": 1},
            ]),
            # capacities：id 重复。
            ("capacities", [{"id": "a", "cap": 1}, {"id": "a", "cap": 2}]),
        ]
        for section, value in cases:
            self.assert_failure(
                encode_ops(cfg_ops + [self.cu(base, section, value, 1)]),
                2, "INPUT",
            )

    def test_backpressure_section_requires_overload(self):
        # overload 非 null 且 low<high≤q 时 backpressure 段可替换成功。
        cfg = config_v11(1, overload={"cap": 1, "q": 4, "ttl": 10})
        base = digest_of(cfg)
        results = self.run_ops(
            [
                {"op": "ci", "config": cfg, "now": 0},
                self.cu(base, "backpressure", {"low": 1, "high": 3}, 1),
                {"op": "ce"},
            ]
        )
        self.assertEqual(results[2]["config"]["backpressure"],
                         {"low": 1, "high": 3})

    def test_failed_cu_rolls_back_everything(self):
        # cu 失败整批回滚：此前成功结果也不产生 stdout，时钟/配置/提交不变。
        raw = encode_ops(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "probe", "id": "a", "ok": True, "now": 0},
                {"op": "cu", "base": "0" * 64, "section": "vnodes",
                 "value": 4, "now": 1},
            ]
        )
        self.assert_failure(raw, 4, "STATE")
        # 同一成功前缀下 ce/cl 保持原样，证明无部分应用。
        results = self.run_ops(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "probe", "id": "a", "ok": True, "now": 0},
                {"op": "cl"},
                {"op": "ce"},
            ]
        )
        self.assertEqual(results[2]["commits"], [])
        self.assertEqual(results[3]["config"]["vnodes"], None)

    def test_record_replay_covers_cu(self):
        base = digest_of(config_v11(1))
        ops = [
            {"op": "ci", "config": config_v11(1), "now": 0},
            self.cu(base, "vnodes", 4, 1),
            self.cu(digest_of(config_v11(1, vnodes=4)),
                    "queue", {"dequeue": "S", "full": "H"}, 2),
            {"op": "cl"},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        record = json.loads(rec_stdout.decode("utf-8"))
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


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
        # 以最小 v11 配置为底，整体替换 backends。
        config = config_v11(1, **overrides)
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
        empty = config_v11(1)
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
        # target 为规范化 v11 配置（endpoint=null、queue 默认 F/T、
        # lifetime=null）的指纹。
        normalized = config_v11(2)
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
        bad_version = config_v10(1)
        bad_version["version"] = 8
        missing = config_v10(1)
        del missing["scheduler"]
        for bad in (bad_version, missing, {"version": 9}, []):
            self.assert_failure(
                encode_ops([{"op": "cd", "config": bad, "now": 0}]),
                2, "INPUT",
            )

    def test_cd_bad_now_is_input(self):
        for bad in (-1, 10 ** 9 + 1, True, "0", 1.5, None):
            self.assert_failure(
                encode_ops([{"op": "cd", "config": config_v10(1), "now": bad}]),
                2, "INPUT",
            )

    def test_cd_now_boundary_values_are_ok(self):
        results = self.run_ops([
            {"op": "cd", "config": config_v10(1), "now": 0},
            {"op": "cd", "config": config_v10(2), "now": 10 ** 9},
        ])
        self.assertEqual(len(results), 2)

    def test_cd_exact_key_order(self):
        # 精确键序 op,config,now：乱序、多键、缺键均 INPUT。
        self.assert_failure(
            encode_ops(
                [{"op": "cd", "now": 0, "config": config_v10(1)}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops(
                [{"config": config_v10(1), "op": "cd", "now": 0}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops(
                [{"op": "cd", "config": config_v10(1), "now": 0, "x": 1}]
            ),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "cd", "config": config_v10(1)}]),
            2, "INPUT",
        )

    def test_cd_clock_regression_is_input_and_advances_clock(self):
        # 时钟倒退 INPUT/2；相等 now 合法。
        self.assert_failure(
            encode_ops([
                {"op": "cd", "config": config_v10(1), "now": 10},
                {"op": "cd", "config": config_v10(2), "now": 5},
            ]),
            2, "INPUT",
        )
        results = self.run_ops([
            {"op": "cd", "config": config_v10(1), "now": 10},
            {"op": "cd", "config": config_v10(2), "now": 10},
        ])
        self.assertEqual(len(results), 2)
        # cd 成功推进共用时钟：其后旧时刻的其他操作报倒退。
        self.assert_failure(
            encode_ops([
                {"op": "cd", "config": config_v10(1), "now": 10},
                {"op": "cv", "config": config_v10(2), "now": 9},
            ]),
            2, "INPUT",
        )

    def test_cd_failure_rolls_back_batch(self):
        # 批内靠后的 cd 失败：整批无 stdout，前面成功的 ci 也不落任何状态。
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            {"op": "cd", "config": config_v10(2), "now": 1},
            {"op": "cd", "config": config_v10(3), "now": "x"},
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
        results = self.run_ops([{"op": "ct"}, self.pd(config_v11(1))])
        result = results[1]
        # 精确结果键序 op,base,target,changes。
        self.assertEqual(list(result), ["op", "base", "target", "changes"])
        self.assertEqual(result["op"], "pd")
        # base 即同批 ct 对当前（空）配置的指纹；target 为候选规范化指纹。
        self.assertEqual(result["base"], results[0]["digest"])
        empty = config_v11(1)
        empty["backends"] = []
        self.assertEqual(result["base"], digest_of(empty))
        self.assertEqual(result["target"], digest_of(config_v11(1)))
        self.assertRegex(result["target"], r"^[0-9a-f]{64}$")
        # 仅 backends 不同：changes 为空。
        self.assertEqual(result["changes"], [])

    def test_pd_identical_config_is_empty_diff(self):
        results = self.run_ops([
            {"op": "ci", "config": config_v10(2), "now": 0},
            {"op": "ct"},
            self.pd(config_v10(2), now=1),
        ])
        result = results[2]
        self.assertEqual(result["base"], result["target"])
        self.assertEqual(result["base"], results[1]["digest"])
        self.assertEqual(result["changes"], [])

    def test_pd_sections_in_fixed_order(self):
        # 候选同时改 vnodes/limits/overload/sticky/idle/backpressure/
        # scheduler/faults/quotas/queue：changes 按固定节序列出，与配置
        # 内登记先后无关；项键序 section,before,after。
        candidate = config_v10(
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
            {"op": "ci", "config": config_v10(1, vnodes=32), "now": 0},
            {"op": "qp", "mode": "S"},
            self.pd(config_v10(1, vnodes=32), now=1),
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
        current = config_v11(1)
        candidate = config_v11(9)
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
            {"op": "ci", "config": config_v10(1), "now": 0},
            self.pd(config_v10(1, vnodes=64), now=1),
            {"op": "ce"},
            {"op": "cl"},
            self.pd(config_v10(1, vnodes=64), now=2),
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
            self.pd(config_v10(1, vnodes=8), now=1),
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
            self.pd(config_v10(1, idle={"ttl": 5}), now=1),
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
        # v1 候选规范化为 v11：target 为规范化指纹，queue 默认 F/T、
        # lifetime=null 不差异。
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
        self.assertEqual(result["target"], digest_of(config_v11(2)))
        self.assertEqual(result["changes"], [])

    def test_pd_unknown_backend_references_are_backend(self):
        # 错误类型与优先级同 cv：B 限流、B 配额、faults 引用未知候选后端。
        bad_limit = config_v10(
            1, limits=[{"scope": "B", "id": "ghost", "r": 1, "b": 1}]
        )
        bad_quota = config_v10(
            1, quotas=[{"scope": "B", "id": "ghost", "limit": 1, "span": 1}]
        )
        bad_fault = config_v10(
            1, faults=[{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}]
        )
        for bad in (bad_limit, bad_quota, bad_fault):
            self.assert_failure(
                encode_ops([self.pd(bad)]),
                3, "BACKEND",
            )

    def test_pd_invalid_config_is_input(self):
        bad_version = config_v10(1)
        bad_version["version"] = 8
        missing = config_v10(1)
        del missing["scheduler"]
        for bad in (bad_version, missing, {"version": 9}, []):
            self.assert_failure(
                encode_ops([self.pd(bad)]),
                2, "INPUT",
            )

    def test_pd_bad_now_is_input(self):
        for bad in (-1, 10 ** 9 + 1, True, "0", 1.5, None):
            self.assert_failure(
                encode_ops([self.pd(config_v10(1), now=bad)]),
                2, "INPUT",
            )

    def test_pd_now_boundary_values_are_ok(self):
        results = self.run_ops([
            self.pd(config_v10(1), now=0),
            self.pd(config_v10(2), now=10 ** 9),
        ])
        self.assertEqual(len(results), 2)

    def test_pd_exact_key_order(self):
        # 精确键序 op,config,now：乱序、多键、缺键均 INPUT。
        self.assert_failure(
            encode_ops([{"op": "pd", "now": 0, "config": config_v10(1)}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"config": config_v10(1), "op": "pd", "now": 0}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "pd", "config": config_v10(1), "now": 0, "x": 1}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "pd", "config": config_v10(1)}]),
            2, "INPUT",
        )

    def test_pd_clock_regression_is_input_and_advances_clock(self):
        # 时钟倒退 INPUT/2；相等 now 合法。
        self.assert_failure(
            encode_ops([
                self.pd(config_v10(1), now=10),
                self.pd(config_v10(2), now=5),
            ]),
            2, "INPUT",
        )
        results = self.run_ops([
            self.pd(config_v10(1), now=10),
            self.pd(config_v10(2), now=10),
        ])
        self.assertEqual(len(results), 2)
        # pd 成功推进共用时钟：其后旧时刻的其他操作报倒退。
        self.assert_failure(
            encode_ops([
                self.pd(config_v10(1), now=10),
                {"op": "cv", "config": config_v10(2), "now": 9},
            ]),
            2, "INPUT",
        )

    def test_pd_failure_rolls_back_batch(self):
        # 批内靠后的 pd 失败：整批无 stdout，前面成功的 ci 也不落任何状态。
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            self.pd(config_v10(2), now=1),
            self.pd(config_v10(3), now="x"),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_record_replay_covers_pd(self):
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            self.pd(config_v10(1, vnodes=16, scheduler={"pick": "R"}), now=1),
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

    def test_ce_exports_v11_with_lifetime_last(self):
        results = self.run_ops([{"op": "add", "id": "a", "weight": 1},
                                {"op": "ce"}])
        config = results[-1]["config"]
        self.assertEqual(
            list(config),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue", "capacities", "lifetime"],
        )
        self.assertEqual(config["version"], 11)
        self.assertEqual(config["faults"], [])
        self.assertEqual(config["quotas"], [])
        # queue 精确键序 dequeue,full，默认 F/T；不含 evicted、last。
        self.assertEqual(list(config["queue"]), ["dequeue", "full"])
        self.assertEqual(config["queue"], {"dequeue": "F", "full": "T"})
        # capacities 无 pc 覆盖为空 []。
        self.assertEqual(config["capacities"], [])
        # lifetime 末置、默认 null（未 tm 且无 v11 快照）。
        self.assertEqual(list(config)[-1], "lifetime")
        self.assertIsNone(config["lifetime"])

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
        self.assertIn(b'"version":11', run_stdout)


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
        # ce 导出 version=11：精确十四键，lifetime 末置；v1..v7 热加载视
        # quotas=[]、queue 为默认 F/T、capacities 为空、lifetime=null，故
        # quotas 为空。
        self.assertEqual(
            list(results[4]["config"]),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue", "capacities", "lifetime"],
        )
        self.assertEqual(results[4]["config"]["version"], 11)
        self.assertEqual(results[4]["config"]["quotas"], [])
        self.assertEqual(
            results[4]["config"]["queue"], {"dequeue": "F", "full": "T"}
        )
        self.assertEqual(results[4]["config"]["capacities"], [])
        self.assertIsNone(results[4]["config"]["lifetime"])
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
        self.assertEqual(config["version"], 11)
        # capacities 后为末置 lifetime，默认 null。
        self.assertEqual(list(config), [
            "version", "backends", "vnodes", "limits", "overload", "sticky",
            "idle", "backpressure", "scheduler", "faults", "quotas", "queue",
            "capacities", "lifetime",
        ])
        self.assertEqual(config["queue"], {"dequeue": "F", "full": "T"})
        self.assertEqual(config["capacities"], [])
        self.assertIsNone(config["lifetime"])
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
        self.assertEqual(commits[2]["config"]["version"], 11)
        # cl/cb 快照一律规范化为 version=11（末置 lifetime=null），queue 默认 F/T、capacities 空。
        for commit in commits:
            self.assertEqual(commit["config"]["version"], 11)
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
        self.assertIn(b'"version":11', run_stdout)
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
        return config_v10(
            1,
            quotas=(),
            dequeue=dequeue,
            full=full,
            vnodes=vnodes,
            limits=list(limits),
            overload={"cap": cap, "q": q, "ttl": ttl},
        )

    # ---- 导出与默认 ----

    def test_ce_fourteen_keys_lifetime_last_with_default_ft(self):
        results = self.run_ops([
            {"op": "ci", "config": config_v8(1), "now": 0},
            {"op": "ce"},
        ])
        config = results[1]["config"]
        self.assertEqual(
            list(config),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue", "capacities", "lifetime"],
        )
        self.assertEqual(config["version"], 11)
        self.assertEqual(list(config["queue"]), ["dequeue", "full"])
        self.assertEqual(config["queue"], {"dequeue": "F", "full": "T"})
        self.assertEqual(config["capacities"], [])
        self.assertIsNone(config["lifetime"])

    def test_v1_through_v8_default_to_ft(self):
        be7 = [{
            "id": "a", "weight": 1, "d": 0, "fail": 3,
            "success": 2, "circuit": None, "drain": None,
        }]
        be8 = [dict(be7[0], endpoint=None)]
        for version in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11):
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
                    9: config_v9_input, 10: config_v10, 11: config_v11,
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
            # 各版本均规范化导出为当前版本 11，lifetime=null。
            self.assertEqual(results[1]["config"]["version"], 11)
            self.assertIsNone(results[1]["config"]["lifetime"])

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
            cfg = config_v10(1)
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
        # version=8 含 queue、capacities（十三键）。
        extra = config_v10(1)
        extra["version"] = 8
        self.assert_failure(
            encode_ops([{"op": "ci", "config": extra, "now": 0}]),
            2, "INPUT",
        )
        # version 字段非法（字符串、0、bool）；v10 键形上的 11 为版本不符；
        # 10/11 均为合法版本号。
        for bad_version in ("10", 0, 11, True):
            cfg = config_v10(1)
            cfg["version"] = bad_version
            self.assert_failure(
                encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
                2, "INPUT",
            )
        # 12 越界：v10/v11 两种键形均判 INPUT。
        for cfg in (config_v10(1), config_v11(1)):
            cfg["version"] = 12
            self.assert_failure(
                encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
                2, "INPUT",
            )
        # version=11 但缺末置 lifetime（十三键）：键形为 v10，版本不符。
        missing_lifetime = config_v10(1)
        missing_lifetime["version"] = 11
        self.assert_failure(
            encode_ops([{"op": "ci", "config": missing_lifetime, "now": 0}]),
            2, "INPUT",
        )
        # version=10 但含 lifetime（十四键）：键形为 v11，版本不符。
        with_lifetime = config_v11(1)
        with_lifetime["version"] = 10
        self.assert_failure(
            encode_ops([{"op": "ci", "config": with_lifetime, "now": 0}]),
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

    def test_cl_normalizes_to_v11_with_loaded_queue(self):
        cfg = self.config(dequeue="S", full="H")
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0}, {"op": "cl"},
        ])
        commit = results[1]["commits"][0]["config"]
        self.assertEqual(commit["version"], 11)
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
        # 两个快照均规范化为 v11 且均携带恢复后的 S/H。
        self.assertEqual(len(commits), 2)
        for commit in commits:
            self.assertEqual(commit["config"]["version"], 11)
            self.assertEqual(
                commit["config"]["queue"], {"dequeue": "S", "full": "H"}
            )

    # ---- STATE/4 ----

    def test_active_connection_blocks_ci_and_cb(self):
        cfg = config_v10(1)
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
        same = config_v10(1)
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
        self.assertIn(b'"version":11', run_stdout)
        self.assertIn(b'"queue":{"dequeue":"S","full":"H"}', run_stdout)


def v10_backend(bid, weight=1):
    return {
        "id": bid, "weight": weight, "d": 0, "fail": 3, "success": 2,
        "circuit": None, "drain": None, "endpoint": None,
    }


class V10CapacitiesHotReloadTest(unittest.TestCase):
    """version=10 capacities：每后端接纳容量覆盖纳入配置与热加载/回滚。"""

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

    def two_backend_config(self, capacities=(), **overrides):
        return config_v10(
            1,
            capacities=list(capacities),
            **dict(overrides, backends=[v10_backend("a"), v10_backend("b")])
        )

    # ---- 导出与规范化 ----

    def test_ce_exports_lifetime_last_empty_by_default(self):
        # 初始空配置：lifetime 末置为 null，capacities 为空 []。
        results = self.run_ops([{"op": "ce"}])
        config = results[0]["config"]
        self.assertEqual(config["version"], 11)
        self.assertEqual(list(config)[-2], "capacities")
        self.assertEqual(config["capacities"], [])
        self.assertEqual(list(config)[-1], "lifetime")
        self.assertIsNone(config["lifetime"])

    def test_ci_loads_and_ce_exports_capacities_in_backend_order(self):
        # 输入顺序不限：乱序提交按后端加入序（a 在 b 前）输出，仅列显式项。
        cfg = self.two_backend_config(
            capacities=[{"id": "b", "cap": 5}, {"id": "a", "cap": 2}]
        )
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0}, {"op": "ce"},
        ])
        config = results[1]["config"]
        self.assertEqual(
            list(config),
            ["version", "backends", "vnodes", "limits", "overload", "sticky",
             "idle", "backpressure", "scheduler", "faults", "quotas",
             "queue", "capacities", "lifetime"],
        )
        self.assertEqual(
            config["capacities"],
            [{"id": "a", "cap": 2}, {"id": "b", "cap": 5}],
        )
        for item in config["capacities"]:
            self.assertEqual(list(item), ["id", "cap"])

    def test_capacity_boundary_values_ok(self):
        cfg = self.two_backend_config(
            capacities=[{"id": "a", "cap": 1}, {"id": "b", "cap": 10 ** 6}]
        )
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0}, {"op": "ce"},
        ])
        self.assertEqual(
            results[1]["config"]["capacities"],
            [{"id": "a", "cap": 1}, {"id": "b", "cap": 10 ** 6}],
        )

    def test_only_explicit_overrides_listed(self):
        # 未列入 capacities 的后端不输出覆盖，pg 判 STATE。
        cfg = self.two_backend_config(capacities=[{"id": "b", "cap": 5}])
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "pg", "id": "b"},
        ])
        self.assertEqual(
            results[1],
            {"op": "pg", "id": "b", "cap": 5, "connections": 0, "available": 5},
        )
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": cfg, "now": 0},
                {"op": "pg", "id": "a"},
            ]),
            4, "STATE",
        )

    def test_cv_normalizes_to_v10_without_applying(self):
        cfg = self.two_backend_config(
            capacities=[{"id": "b", "cap": 9}, {"id": "a", "cap": 3}]
        )
        results = self.run_ops([{"op": "cv", "config": cfg, "now": 0}])
        result = results[0]
        self.assertEqual(result["applicable"], True)
        self.assertEqual(
            result["config"]["capacities"],
            [{"id": "a", "cap": 3}, {"id": "b", "cap": 9}],
        )
        self.assertEqual(result["config"]["version"], 11)
        # cv 不应用：ce 仍为空。
        results = self.run_ops([{"op": "cv", "config": cfg, "now": 0},
                                {"op": "ce"}])
        self.assertEqual(results[1]["config"]["capacities"], [])

    def test_utf8_ids_sorted_by_backend_order(self):
        cfg = config_v10(
            1,
            capacities=[{"id": "Ω", "cap": 2}, {"id": "a", "cap": 1}],
            backends=[v10_backend("a"), v10_backend("Ω")],
        )
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0}, {"op": "ce"},
        ])
        self.assertEqual(
            results[1]["config"]["capacities"],
            [{"id": "a", "cap": 1}, {"id": "Ω", "cap": 2}],
        )

    # ---- 覆盖实际生效 ----

    def test_loaded_capacity_drives_admission(self):
        # overload.cap=4，但 capacities 把唯一后端覆盖为 1：第 2 个连接到达
        # 容量即排队（vnodes=1 使 ci 后即可经一致性环路由）。
        cfg = config_v10(
            1,
            vnodes=1,
            capacities=[{"id": "a", "cap": 1}],
            overload={"cap": 4, "q": 4, "ttl": 100},
        )
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "open", "cid": "c1", "flow": self.FLOW, "now": 0},
            {"op": "oa", "cid": "c2", "flow": self.FLOW, "c": "k",
             "s": "k", "key": "k", "now": 0},
        ])
        self.assertEqual(results[2]["state"], "Q")

    # ---- 非法结构 INPUT/2 ----

    def test_capacities_container_and_items_are_input(self):
        item_cases = [
            [{"id": "a", "cap": "1"}],  # cap 类型
            [{"id": "a", "cap": 1.0}],
            [{"id": "a", "cap": 0}],    # 下界
            [{"id": "a", "cap": 10 ** 6 + 1}],  # 上界
            [{"id": "a", "cap": True}],          # bool
            [{"id": "a", "cap": None}],
            [{"id": 1, "cap": 1}],      # id 类型
            [{"id": "", "cap": 1}],     # id 空串
            [{"cap": 1, "id": "a"}],    # 项键序反
            [{"id": "a"}],              # 缺 cap
            [{"cap": 1}],               # 缺 id
            [{"id": "a", "cap": 1, "x": 0}],  # 多项
            [{"id": "a", "cap": 1}, {"id": "a", "cap": 2}],  # 重复 id
        ]
        for bad in item_cases:
            self.assert_failure(
                encode_ops([
                    {"op": "ci", "config": config_v10(1, capacities=bad),
                     "now": 0},
                ]),
                2, "INPUT",
            )
        # 非数组容器：dict/None/字符串/整数，整体替换 capacities 字段。
        for bad in ({}, None, "x", 1):
            cfg = config_v10(1)
            cfg["capacities"] = bad
            self.assert_failure(
                encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
                2, "INPUT",
            )

    def test_capacities_id_not_utf8_encodable_is_input(self):
        # 孤立代理无法 UTF-8 编码：INPUT。
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":{'
            b'"version":10,'
            b'"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            b'"circuit":null,"drain":null,"endpoint":null}],'
            b'"vnodes":null,"limits":[],"overload":null,'
            b'"sticky":null,"idle":null,"backpressure":null,'
            b'"scheduler":{"pick":"W"},"faults":[],"quotas":[],'
            b'"queue":{"dequeue":"F","full":"T"},'
            b'"capacities":[{"id":"\\ud800","cap":1}]'
            b'}}]}'
        )
        self.assert_failure(raw, 2, "INPUT")

    def test_top_level_capacities_order_and_keyset_are_input(self):
        # v10 缺 capacities（十二键）。
        missing = config_v9_input(1)
        missing["version"] = 10
        self.assert_failure(
            encode_ops([{"op": "ci", "config": missing, "now": 0}]),
            2, "INPUT",
        )
        # v9 含 capacities（十三键）。
        extra = config_v10(1)
        extra["version"] = 9
        self.assert_failure(
            encode_ops([{"op": "ci", "config": extra, "now": 0}]),
            2, "INPUT",
        )
        # version 字段非法：字符串、越界 12、bool。
        for bad in ("10", 12, False):
            cfg = config_v10(1)
            cfg["version"] = bad
            self.assert_failure(
                encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
                2, "INPUT",
            )

    def test_capacities_before_queue_is_input(self):
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":{'
            b'"version":10,'
            b'"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            b'"circuit":null,"drain":null,"endpoint":null}],'
            b'"vnodes":null,"limits":[],"overload":null,'
            b'"sticky":null,"idle":null,"backpressure":null,'
            b'"scheduler":{"pick":"W"},"faults":[],"quotas":[],'
            b'"capacities":[],"queue":{"dequeue":"F","full":"T"}'
            b'}}]}'
        )
        self.assert_failure(raw, 2, "INPUT")

    def test_inner_capacity_key_swap_is_input(self):
        raw = (
            b'{"ops":[{"op":"ci","now":0,"config":{'
            b'"version":10,'
            b'"backends":[{"id":"a","weight":1,"d":0,"fail":3,"success":2,'
            b'"circuit":null,"drain":null,"endpoint":null}],'
            b'"vnodes":null,"limits":[],"overload":null,'
            b'"sticky":null,"idle":null,"backpressure":null,'
            b'"scheduler":{"pick":"W"},"faults":[],"quotas":[],'
            b'"queue":{"dequeue":"F","full":"T"},'
            b'"capacities":[{"cap":1,"id":"a"}]'
            b'}}]}'
        )
        self.assert_failure(raw, 2, "INPUT")

    # ---- 未知 id：BACKEND/3 ----

    def test_unknown_capacity_id_is_backend_for_all_preview_ops(self):
        cfg = config_v10(1, capacities=[{"id": "ghost", "cap": 1}])
        for opname in ("ci", "cv", "cd", "pd"):
            self.assert_failure(
                encode_ops([{"op": opname, "config": cfg, "now": 0}]),
                3, "BACKEND",
            )
        cfg_hd = config_v10(
            1, capacities=[{"id": "ghost", "cap": 1}], vnodes=4
        )
        self.assert_failure(
            encode_ops([{"op": "hd", "config": cfg_hd, "keys": ["k"],
                         "now": 0}]),
            3, "BACKEND",
        )

    def test_input_precedes_backend(self):
        # 未知 id 但 cap 非法：仍判 INPUT。
        cfg = config_v10(1, capacities=[{"id": "ghost", "cap": 0}])
        self.assert_failure(
            encode_ops([{"op": "ci", "config": cfg, "now": 0}]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "pd", "config": cfg, "now": 0}]),
            2, "INPUT",
        )

    def test_backend_precedes_state(self):
        # 活动连接存在时未知 id 仍报 BACKEND（先于活动连接 STATE）。
        cfg_bad = config_v10(1, capacities=[{"id": "ghost", "cap": 1}])
        ops = [
            {"op": "ci", "config": config_v10(1), "now": 0},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
            {"op": "ci", "config": cfg_bad, "now": 2},
        ]
        self.assert_failure(encode_ops(ops), 3, "BACKEND")

    # ---- 活动连接/排队：STATE/4 ----

    def test_active_connection_or_queue_blocks_ci(self):
        cfg = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        ops = [
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 1},
            {"op": "ci", "config": cfg, "now": 2},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        queued = config_v10(
            1, capacities=[{"id": "a", "cap": 1}],
            overload={"cap": 1, "q": 2, "ttl": 10},
        )
        ops = [
            {"op": "ci", "config": queued, "now": 0},
            {"op": "open", "cid": "c1", "flow": self.FLOW, "now": 0},
            {"op": "oa", "cid": "c2", "flow": self.FLOW, "c": "k",
             "s": "k", "key": "k", "now": 0},
            {"op": "ci", "config": queued, "now": 1},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    # ---- 原子替换、旧版清空、cb 恢复、pc 隔离 ----

    def test_atomic_replace_drops_unlisted_overrides(self):
        first = self.two_backend_config(
            capacities=[{"id": "a", "cap": 2}, {"id": "b", "cap": 7}]
        )
        # 第二次仅覆盖 a：b 的旧覆盖必须消失。
        second = self.two_backend_config(capacities=[{"id": "a", "cap": 3}])
        results = self.run_ops([
            {"op": "ci", "config": first, "now": 0},
            {"op": "ci", "config": second, "now": 1},
            {"op": "pg", "id": "a"},
        ])
        self.assertEqual(results[2]["cap"], 3)
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": first, "now": 0},
                {"op": "ci", "config": second, "now": 1},
                {"op": "pg", "id": "b"},
            ]),
            4, "STATE",
        )

    def test_legacy_versions_clear_overrides(self):
        with_override = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        v1_config = {
            "version": 1,
            "backends": [{
                "id": "a", "weight": 1, "d": 0, "fail": 3, "success": 2,
                "circuit": None, "drain": None,
            }],
            "vnodes": None, "limits": [], "overload": None,
        }
        legacy_configs = [
            v1_config,
            config_v6(1), config_v7(1), config_v8(1), config_v9_input(1),
        ]
        for cfg in legacy_configs:
            with self.subTest(version=cfg["version"]):
                ops = [
                    {"op": "ci", "config": with_override, "now": 0},
                    {"op": "ci", "config": cfg, "now": 1},
                    {"op": "pg", "id": "a"},
                ]
                self.assert_failure(encode_ops(ops), 4, "STATE")
        # 显式空 capacities 同样清空。
        ops = [
            {"op": "ci", "config": with_override, "now": 0},
            {"op": "ci", "config": config_v10(1, capacities=[]), "now": 1},
            {"op": "pg", "id": "a"},
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_cb_restores_overrides_and_new_snapshot_is_v11(self):
        first = self.two_backend_config(
            capacities=[{"id": "a", "cap": 2}, {"id": "b", "cap": 7}]
        )
        empty = self.two_backend_config(capacities=[])
        results = self.run_ops([
            {"op": "ci", "config": first, "now": 0},   # rev 1
            {"op": "ci", "config": empty, "now": 1},   # rev 2 清空
            {"op": "cb", "rev": 1, "now": 2},          # rev 3 恢复
            {"op": "pg", "id": "a"},
            {"op": "pg", "id": "b"},
            {"op": "cl"},
            {"op": "ce"},
        ])
        self.assertEqual(
            results[2], {"op": "cb", "target": 1, "rev": 3, "ok": True}
        )
        self.assertEqual(results[3]["cap"], 2)
        self.assertEqual(results[4]["cap"], 7)
        commits = results[5]["commits"]
        self.assertEqual(commits[0]["config"]["capacities"],
                         [{"id": "a", "cap": 2}, {"id": "b", "cap": 7}])
        self.assertEqual(commits[1]["config"]["capacities"], [])
        self.assertEqual(commits[2]["config"]["capacities"],
                         [{"id": "a", "cap": 2}, {"id": "b", "cap": 7}])
        for commit in commits:
            self.assertEqual(commit["config"]["version"], 11)
        self.assertEqual(
            results[6]["config"]["capacities"],
            [{"id": "a", "cap": 2}, {"id": "b", "cap": 7}],
        )

    def test_cb_to_empty_snapshot_clears_overrides(self):
        with_override = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        empty = config_v10(1, capacities=[])
        ops = [
            {"op": "ci", "config": with_override, "now": 0},
            {"op": "ci", "config": empty, "now": 1},
            {"op": "cb", "rev": 1, "now": 2},  # 回滚到有覆盖快照后……
            {"op": "cb", "rev": 2, "now": 3},  # ……再回到空快照。
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[3]["rev"], 4)
        # 第二次 cb 已清空覆盖：再 pg 判 STATE。
        self.assert_failure(
            encode_ops(ops + [{"op": "pg", "id": "a"}]),
            4, "STATE",
        )

    def test_pc_after_ci_changes_only_current_ce(self):
        cfg = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "pc", "id": "a", "cap": 99},
            {"op": "cl"},
            {"op": "ce"},
            {"op": "pg", "id": "a"},
        ])
        # 提交快照仍为 cap=2，当前 ce 与 pg 为 cap=99。
        self.assertEqual(
            results[2]["commits"][0]["config"]["capacities"],
            [{"id": "a", "cap": 2}],
        )
        self.assertEqual(
            results[3]["config"]["capacities"], [{"id": "a", "cap": 99}]
        )
        self.assertEqual(results[4]["cap"], 99)

    # ---- ct 摘要与 pd differences ----

    def test_ct_digest_changes_with_capacities(self):
        one = config_v10(1, capacities=[{"id": "a", "cap": 1}])
        two = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        results = self.run_ops([
            {"op": "ci", "config": one, "now": 0},
            {"op": "ct"},
            {"op": "pd", "config": two, "now": 1},
        ])
        self.assertNotEqual(results[1]["digest"], results[2]["target"])
        self.assertEqual(results[1]["digest"], results[2]["base"])

    def test_pd_capacities_section_after_queue(self):
        current = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        candidate = config_v10(
            1, dequeue="S", capacities=[{"id": "a", "cap": 5}]
        )
        results = self.run_ops([
            {"op": "ci", "config": current, "now": 0},
            {"op": "pd", "config": candidate, "now": 1},
        ])
        sections = [item["section"] for item in results[1]["changes"]]
        self.assertEqual(sections, ["queue", "capacities"])
        capacities_change = results[1]["changes"][1]
        self.assertEqual(
            list(capacities_change), ["section", "before", "after"]
        )
        self.assertEqual(capacities_change["before"],
                         [{"id": "a", "cap": 2}])
        self.assertEqual(capacities_change["after"],
                         [{"id": "a", "cap": 5}])

    def test_pd_before_reflects_runtime_pc_overrides(self):
        cfg = config_v10(1, capacities=[])
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "pc", "id": "a", "cap": 8},
            {"op": "pd", "config": cfg, "now": 1},
        ])
        self.assertEqual(
            results[2]["changes"],
            [{
                "section": "capacities",
                "before": [{"id": "a", "cap": 8}],
                "after": [],
            }],
        )

    def test_pd_legacy_candidate_normalizes_capacities_empty(self):
        # v9 候选规范化为 capacities=[]：与当前显式覆盖产生一节差异。
        current = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        results = self.run_ops([
            {"op": "ci", "config": current, "now": 0},
            {"op": "pd", "config": config_v9_input(1), "now": 1},
        ])
        self.assertEqual(
            results[1]["changes"],
            [{
                "section": "capacities",
                "before": [{"id": "a", "cap": 2}],
                "after": [],
            }],
        )

    def test_pd_identical_capacities_is_empty_diff(self):
        cfg = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "pd", "config": cfg, "now": 1},
        ])
        self.assertEqual(results[1]["changes"], [])
        self.assertEqual(results[1]["base"], results[1]["target"])

    # ---- 失败回滚与 record/replay ----

    def test_failed_ci_rolls_back_everything(self):
        # 先成功载入覆盖并 rev=1，随后未知 id 失败：整批无 stdout。
        good = config_v10(1, capacities=[{"id": "a", "cap": 2}])
        bad = config_v10(1, capacities=[{"id": "ghost", "cap": 1}])
        self.assert_failure(
            encode_ops([
                {"op": "ci", "config": good, "now": 5},
                {"op": "ci", "config": bad, "now": 6},
            ]),
            3, "BACKEND",
        )

    def test_record_replay_byte_identical(self):
        cfg = self.two_backend_config(
            capacities=[{"id": "b", "cap": 5}, {"id": "a", "cap": 2}]
        )
        empty = self.two_backend_config(capacities=[])
        ops = [
            {"op": "ci", "config": cfg, "now": 7},
            {"op": "pg", "id": "b"},
            {"op": "pc", "id": "a", "cap": 9},
            {"op": "ce"},
            {"op": "cl"},
            {"op": "cb", "rev": 1, "now": 9},
            {"op": "ci", "config": empty, "now": 10},
            {"op": "pd", "config": cfg, "now": 11},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual((run_code, rec_code), (0, 0))
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)
        # 单换行、紧凑、固定键序：capacities 数组逐项键序 id,cap。
        self.assertEqual(run_stdout.count(b"\n"), 1)
        self.assertIn(
            b'"capacities":[{"id":"a","cap":2},{"id":"b","cap":5}]',
            run_stdout,
        )


class V11LifetimeHotReloadTest(unittest.TestCase):
    """version=11 末置 lifetime：tm 硬时限纳入配置与 ce/ci/cl/cb/cp/ca。"""

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

    def te(self, cid, now):
        return {"op": "te", "cid": cid, "now": now}

    # ---- 结构与校验 ----

    def test_v11_requires_exact_trailing_lifetime_and_order(self):
        # version=11 缺末置 lifetime（十三键）：INPUT。
        missing = config_v10(1)
        missing["version"] = 11
        self.assert_failure(
            [{"op": "ci", "config": missing, "now": 0}], 2, "INPUT"
        )
        # version=10 含 lifetime（十四键）：INPUT。
        extra = config_v11(1)
        extra["version"] = 10
        self.assert_failure(
            [{"op": "ci", "config": extra, "now": 0}], 2, "INPUT"
        )
        # 越界版本 12（两种键形）：INPUT。
        for cfg in (config_v10(1), config_v11(1)):
            cfg["version"] = 12
            self.assert_failure(
                [{"op": "ci", "config": cfg, "now": 0}], 2, "INPUT"
            )

    def test_lifetime_key_order_is_input(self):
        # lifetime 不在末置（与 capacities 交换）：INPUT。
        ordered = config_v11(1, lifetime=5)
        items = list(ordered.items())
        items[-1], items[-2] = items[-2], items[-1]
        self.assert_failure(
            [{"op": "ci", "config": dict(items), "now": 0}], 2, "INPUT"
        )

    def test_invalid_lifetime_is_input_for_all_config_ops(self):
        for opname in ("ci", "cv", "cd", "pd"):
            for bad in (
                0, -1, 10 ** 9 + 1, True, False, 1.0, "5", [], {},
                {"x": 1}, {"ttl": 0}, {"ttl": True}, {"ttl": "5"},
                {"ttl": None}, {"ttl": 5, "x": 1},
            ):
                cfg = config_v11(1)
                cfg["lifetime"] = bad
                with self.subTest(op=opname, bad=bad):
                    self.assert_failure(
                        [{"op": opname, "config": cfg, "now": 0}],
                        2, "INPUT",
                    )

    def test_lifetime_boundaries_ok(self):
        for ttl in (1, 10 ** 9):
            results = self.run_ops([
                {"op": "cv", "config": config_v11(1, lifetime=ttl), "now": 0},
            ])
            self.assertEqual(
                results[0]["config"]["lifetime"], {"ttl": ttl}
            )

    def test_legacy_versions_1_to_10_normalize_lifetime_null(self):
        # 各版本候选均合法，cv 回显 v11 且 lifetime=null。
        candidates = [
            {
                "version": 1,
                "backends": [{
                    "id": "a", "weight": 1, "d": 0, "fail": 3, "success": 2,
                    "circuit": None, "drain": None,
                }],
                "vnodes": None, "limits": [], "overload": None,
            },
            config_v6(1), config_v7(1), config_v8(1),
            config_v9_input(1), config_v10(1),
        ]
        for cfg in candidates:
            with self.subTest(version=cfg["version"]):
                results = self.run_ops([
                    {"op": "cv", "config": cfg, "now": 0},
                ])
                normalized = results[0]["config"]
                self.assertEqual(normalized["version"], 11)
                self.assertEqual(list(normalized)[-1], "lifetime")
                self.assertIsNone(normalized["lifetime"])

    # ---- ce/ct/cl 导出 ----

    def test_ce_ct_export_loaded_lifetime(self):
        cfg = config_v11(1, lifetime=5)
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "ce"},
            {"op": "ct"},
        ])
        config = results[1]["config"]
        self.assertEqual(list(config)[-1], "lifetime")
        self.assertEqual(config["version"], 11)
        self.assertEqual(config["lifetime"], {"ttl": 5})
        self.assertEqual(results[2]["digest"], digest_of(config))

    def test_ct_changes_when_tm_changes_current_config(self):
        cfg = config_v11(1, lifetime=5)
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "ct"},
            {"op": "tm", "ttl": 9},
            {"op": "ce"},
            {"op": "ct"},
        ])
        self.assertNotEqual(results[1]["digest"], results[4]["digest"])
        self.assertEqual(results[3]["config"]["lifetime"], {"ttl": 9})

    # ---- ci 载入与清除 ----

    def test_ci_loads_lifetime_applied_to_new_connections(self):
        cfg = config_v11(1, lifetime=5)
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "open", "cid": "c", "flow": self.FLOW, "now": 0},
            self.te("c", 4),
            self.te("c", 5),
        ])
        self.assertEqual(
            {k: results[2][k] for k in
             ("idle", "lifetime", "deadline", "state", "reason")},
            {"idle": None, "lifetime": 5, "deadline": 5,
             "state": "A", "reason": None},
        )
        self.assertEqual(
            {k: results[3][k] for k in
             ("lifetime", "deadline", "state", "reason")},
            {"lifetime": 5, "deadline": 5, "state": "E", "reason": "L"},
        )

    def test_loaded_lifetime_combined_with_idle_hard_deadline_wins(self):
        # idle=3（ts 登记）且 lifetime=5：tk 刷新 last 后空闲截止推迟到 5，
        # 硬截止仍为 opened_at+5；二者同时到期取 L。
        cfg = config_v11(1, idle={"ttl": 3}, lifetime=5)
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "open", "cid": "c", "flow": self.FLOW, "now": 0},
            {"op": "tk", "cid": "c", "now": 2},
            self.te("c", 5),
        ])
        self.assertEqual(results[2]["ok"], True)
        self.assertEqual(
            {k: results[3][k] for k in
             ("idle", "lifetime", "state", "reason")},
            {"idle": 5, "lifetime": 5, "state": "E", "reason": "L"},
        )

    def test_loaded_lifetime_drives_tk_and_tx(self):
        cfg = config_v11(1, lifetime=5)
        results = self.run_ops([
            {"op": "ci", "config": cfg, "now": 0},
            {"op": "open", "cid": "c", "flow": self.FLOW, "now": 0},
            {"op": "tx", "now": 5},
        ])
        self.assertEqual(results[2], {"op": "tx", "expired": ["c"]})
        # tk 还要求已 ts：配 idle=100 使 tk 可用，命中硬到期（opened_at+5）
        # 同样报 CONNECTION/5 且不刷新 last。
        cfg_both = config_v11(1, idle={"ttl": 100}, lifetime=5)
        self.assert_failure(
            [
                {"op": "ci", "config": cfg_both, "now": 0},
                {"op": "open", "cid": "c", "flow": self.FLOW, "now": 0},
                {"op": "tk", "cid": "c", "now": 5},
            ],
            5, "CONNECTION",
        )
        # 硬到期前 tk 正常刷新 last。
        results = self.run_ops([
            {"op": "ci", "config": cfg_both, "now": 0},
            {"op": "open", "cid": "c", "flow": self.FLOW, "now": 0},
            {"op": "tk", "cid": "c", "now": 4},
        ])
        self.assertEqual(results[2]["ok"], True)

    def test_ci_null_lifetime_clears_current_tm(self):
        results = self.run_ops([
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 0},
            {"op": "tm", "ttl": 99},
            {"op": "ci", "config": config_v11(1), "now": 1},
            {"op": "ce"},
        ])
        self.assertIsNone(results[3]["config"]["lifetime"])

    def test_ci_v10_candidate_clears_current_tm(self):
        # v1..v10 候选规范化 lifetime=null：成功热加载清除既有 tm 登记。
        results = self.run_ops([
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 0},
            {"op": "tm", "ttl": 99},
            {"op": "ci", "config": config_v10(1), "now": 1},
            {"op": "ce"},
        ])
        self.assertEqual(results[3]["config"]["version"], 11)
        self.assertIsNone(results[3]["config"]["lifetime"])

    # ---- tm 只影响当前 ce/ct ----

    def test_tm_after_commit_does_not_change_stored_snapshot(self):
        results = self.run_ops([
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 0},
            {"op": "tm", "ttl": 99},
            {"op": "cl"},
            {"op": "ce"},
            {"op": "ct"},
        ])
        commit = results[2]["commits"][0]["config"]
        self.assertEqual(commit["version"], 11)
        self.assertEqual(commit["lifetime"], {"ttl": 5})
        self.assertEqual(results[3]["config"]["lifetime"], {"ttl": 99})
        # 当前 ct 反映 tm，提交快照指纹不随之变化。
        self.assertEqual(results[4]["digest"], digest_of(results[3]["config"]))
        self.assertNotEqual(
            results[4]["digest"], digest_of(commit)
        )

    # ---- cb 回滚 ----

    def test_cb_restores_snapshot_lifetime(self):
        flow = self.FLOW
        results = self.run_ops([
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 0},
            {"op": "open", "cid": "c", "flow": flow, "now": 0},
            {"op": "close", "cid": "c", "now": 0},
            {"op": "tm", "ttl": 99},
            {"op": "cb", "rev": 1, "now": 1},
            {"op": "open", "cid": "d", "flow": flow, "now": 1},
            self.te("d", 6),
            {"op": "ce"},
        ])
        self.assertEqual(
            results[4], {"op": "cb", "target": 1, "rev": 2, "ok": True}
        )
        # 恢复 rev1 的 ttl=5：opened_at=1，lifetime=6，now=6 硬到期。
        self.assertEqual(
            {k: results[6][k] for k in
             ("lifetime", "deadline", "state", "reason")},
            {"lifetime": 6, "deadline": 6, "state": "E", "reason": "L"},
        )
        self.assertEqual(results[7]["config"]["lifetime"], {"ttl": 5})

    def test_cb_to_null_lifetime_clears_tm(self):
        flow = self.FLOW
        results = self.run_ops([
            {"op": "ci", "config": config_v11(1), "now": 0},
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 1},
            {"op": "open", "cid": "c", "flow": flow, "now": 1},
            {"op": "close", "cid": "c", "now": 1},
            {"op": "cb", "rev": 1, "now": 2},
            {"op": "ce"},
        ])
        self.assertEqual(results[4]["rev"], 3)
        self.assertIsNone(results[5]["config"]["lifetime"])

    # ---- cp/cq/ca 预约 ----

    def test_cp_stores_lifetime_and_ca_loads_it(self):
        flow = self.FLOW
        results = self.run_ops([
            {"op": "cp", "config": config_v11(1, lifetime=5),
             "at": 5, "now": 0},
            {"op": "tm", "ttl": 99},
            {"op": "cq", "now": 1},
            {"op": "ca", "now": 5},
            {"op": "open", "cid": "c", "flow": flow, "now": 5},
            self.te("c", 10),
            {"op": "ce"},
        ])
        self.assertTrue(results[2]["pending"])
        # 预约生效按快照 ttl=5（非 tm 的 99）：opened_at=5，lifetime=10。
        self.assertEqual(
            results[3],
            {"op": "ca", "digest": results[2]["digest"],
             "rev": 1, "ok": True},
        )
        self.assertEqual(
            {k: results[5][k] for k in
             ("lifetime", "deadline", "state", "reason")},
            {"lifetime": 10, "deadline": 10, "state": "E", "reason": "L"},
        )
        self.assertEqual(results[6]["config"]["lifetime"], {"ttl": 5})

    def test_cp_snapshot_digest_is_v11_normalization(self):
        results = self.run_ops([
            {"op": "cp", "config": config_v11(1, lifetime=7),
             "at": 1, "now": 0},
            {"op": "ct"},
            {"op": "cv", "config": config_v11(1, lifetime=7), "now": 0},
        ])
        self.assertEqual(
            results[0]["digest"],
            digest_of(results[2]["config"]),
        )
        self.assertNotEqual(results[0]["digest"], results[1]["digest"])

    # ---- pd.changes：lifetime 在 capacities 之后 ----

    def test_pd_lifetime_change_listed_after_capacities(self):
        current = config_v11(1)
        candidate = config_v11(
            1, capacities=[{"id": "a", "cap": 4}], lifetime=7
        )
        results = self.run_ops([
            {"op": "ci", "config": current, "now": 0},
            {"op": "pd", "config": candidate, "now": 1},
        ])
        sections = [item["section"] for item in results[1]["changes"]]
        self.assertEqual(sections, ["capacities", "lifetime"])
        self.assertEqual(
            results[1]["changes"][1],
            {"section": "lifetime",
             "before": None, "after": {"ttl": 7}},
        )

    def test_pd_lifetime_reflects_current_tm(self):
        # 当前 ce 的 lifetime 含 tm 后续修改；候选仅 lifetime 差异时列出。
        results = self.run_ops([
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 0},
            {"op": "tm", "ttl": 9},
            {"op": "pd", "config": config_v11(1, lifetime=5), "now": 1},
        ])
        self.assertEqual(
            results[2]["changes"],
            [{"section": "lifetime",
              "before": {"ttl": 9}, "after": {"ttl": 5}}],
        )

    # ---- 错误优先级 ----

    def test_v11_unknown_backend_references_are_backend(self):
        for field in (
            {"limits": [{"scope": "B", "id": "ghost", "r": 1, "b": 1}]},
            {"quotas": [{"scope": "B", "id": "ghost",
                         "limit": 1, "span": 1}]},
            {"faults": [{"id": "ghost", "k": "D", "a": 0, "z": 1, "v": 0}]},
            {"capacities": [{"id": "ghost", "cap": 1}]},
        ):
            cfg = config_v11(1, lifetime=5)
            cfg.update(field)
            with self.subTest(field=field):
                self.assert_failure(
                    [{"op": "ci", "config": cfg, "now": 0}],
                    3, "BACKEND",
                )

    def test_v11_ci_active_connection_and_queue_are_state(self):
        cfg = config_v11(1, lifetime=5)
        self.assert_failure(
            [
                {"op": "ci", "config": cfg, "now": 0},
                {"op": "open", "cid": "c", "flow": self.FLOW, "now": 1},
                {"op": "ci", "config": cfg, "now": 2},
            ],
            4, "STATE",
        )

    # ---- record/replay 逐字节 ----

    def test_record_replay_byte_identical(self):
        ops = [
            {"op": "ci", "config": config_v11(1, lifetime=5), "now": 0},
            {"op": "open", "cid": "c", "flow": self.FLOW, "now": 0},
            self.te("c", 5),
            {"op": "cl"},
        ]
        raw = encode_ops(ops)
        run_code, run_stdout, run_stderr = run_balancer("run", raw)
        rec_code, rec_stdout, _ = run_balancer("record", raw)
        self.assertEqual((run_code, rec_code), (0, 0))
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)
        self.assertEqual(run_stdout.count(b"\n"), 1)
        self.assertIn(b'"lifetime":{"ttl":5}', run_stdout)


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


class WaitHistoryTest(unittest.TestCase):
    """wh 排队等待历史：离队等待时长五桶、四类事件、只读查询、清空与回滚。"""

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

    def base_ops(self, ttl=10, cap=1, q=10):
        # 环上唯一后端 a，cap=1 便于制造入队。
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": cap, "q": q, "ttl": ttl},
        ]

    def oa(self, cid, now, key="k"):
        return {"op": "oa", "cid": cid, "flow": FLOW,
                "c": "k", "s": "k", "key": key, "now": now}

    ZERO = [0, 0, 0, 0, 0]

    def test_wh_empty_windows_and_key_order(self):
        ops = self.base_ops() + [
            {"op": "wh", "from": 0, "to": 0, "now": 0},
        ]
        result = self.run_ops(ops)[3]
        self.assertEqual(list(result), ["op", "windows"])
        self.assertEqual(len(result["windows"]), 1)
        window = result["windows"][0]
        self.assertEqual(
            list(window),
            ["window", "admitted", "expired", "cancelled", "evicted"],
        )
        self.assertEqual(
            window,
            {"window": 0, "admitted": self.ZERO, "expired": self.ZERO,
             "cancelled": self.ZERO, "evicted": self.ZERO},
        )
        # 四项均为长度 5 的非负整数数组，空窗全零。
        for field in ("admitted", "expired", "cancelled", "evicted"):
            self.assertEqual(window[field], [0, 0, 0, 0, 0])

    def test_wh_admitted_delay_buckets(self):
        # c1 占唯一连接，c2..c6 均于 now=0 入队；逐个 close+ot，接纳延迟
        # 0/1/10/100/101 分别落桶 ≤0、≤1、≤10、≤100、>100。
        ops = self.base_ops(ttl=100000) + [
            self.oa("c1", 0),
            self.oa("c2", 0), self.oa("c3", 0), self.oa("c4", 0),
            self.oa("c5", 0), self.oa("c6", 0),
            {"op": "close", "cid": "c1", "now": 0},
            {"op": "ot", "now": 0},
            {"op": "close", "cid": "c2", "now": 1},
            {"op": "ot", "now": 1},
            {"op": "close", "cid": "c3", "now": 10},
            {"op": "ot", "now": 10},
            {"op": "close", "cid": "c4", "now": 100},
            {"op": "ot", "now": 100},
            {"op": "close", "cid": "c5", "now": 101},
            {"op": "ot", "now": 101},
            {"op": "wh", "from": 0, "to": 1, "now": 101},
        ]
        windows = self.run_ops(ops)[-1]["windows"]
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0]["window"], 0)
        # 延迟 0/1/10 在窗 0，分别落桶 0/1/2。
        self.assertEqual(windows[0]["admitted"], [1, 1, 1, 0, 0])
        self.assertEqual(windows[0]["expired"], self.ZERO)
        self.assertEqual(windows[0]["cancelled"], self.ZERO)
        self.assertEqual(windows[0]["evicted"], self.ZERO)
        self.assertEqual(windows[1]["window"], 1)
        # 延迟 100/101 在窗 1，分别落桶 3/4。
        self.assertEqual(windows[1]["admitted"], [0, 0, 0, 1, 1])
        for field in ("expired", "cancelled", "evicted"):
            self.assertEqual(windows[1][field], self.ZERO)

    def test_wh_cancelled_uses_logical_clock(self):
        # c2 于 now=0 入队；oc 无 now，按当前逻辑时钟记 cancelled。oq 把时钟
        # 推进到 5 后取消：d=5 落 ≤10 桶，事件归窗 0。
        ops = self.base_ops() + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "oq", "now": 5},
            {"op": "oc", "cid": "c2"},
            {"op": "wh", "from": 0, "to": 0, "now": 5},
        ]
        window = self.run_ops(ops)[-1]["windows"][0]
        self.assertEqual(window["cancelled"], [0, 0, 1, 0, 0])
        self.assertEqual(window["admitted"], self.ZERO)
        self.assertEqual(window["expired"], self.ZERO)
        self.assertEqual(window["evicted"], self.ZERO)

    def test_wh_evicted_uses_head_oa_now(self):
        # H 模式 q=1：c0 建连、c1 排队占满队，c2 于 now=5 入队头淘汰 c1，
        # evicted 按该次 oa.now=5（d=5，≤10 桶）记账。
        ops = self.base_ops(q=1) + [
            {"op": "rp", "mode": "H"},
            self.oa("c0", 0, key="k0"),
            self.oa("c1", 0, key="k1"),
            self.oa("c2", 5, key="k2"),
            {"op": "wh", "from": 0, "to": 0, "now": 5},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[6]["evicted"], "c1")
        window = results[-1]["windows"][0]
        self.assertEqual(window["evicted"], [0, 0, 1, 0, 0])
        self.assertEqual(window["admitted"], self.ZERO)
        self.assertEqual(window["expired"], self.ZERO)
        self.assertEqual(window["cancelled"], self.ZERO)

    def test_wh_expired_delay_bucket(self):
        # c2 于 now=0 入队、ttl=10，c1 仍占连接；ot@10 时 c2 过期：d=10 落
        # ≤10 桶，expired 归离队时刻窗 0。
        ops = self.base_ops(ttl=10) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "ot", "now": 10},
            {"op": "wh", "from": 0, "to": 0, "now": 10},
        ]
        results = self.run_ops(ops)
        self.assertEqual(results[5]["expired"], ["c2"])
        window = results[-1]["windows"][0]
        # d=10 落 ≤10 桶（索引 2）。
        self.assertEqual(window["expired"], [0, 0, 1, 0, 0])
        self.assertEqual(window["admitted"], self.ZERO)

    def test_wh_only_successful_dequeue_counted(self):
        # 立即接纳（A）从不入队，不记；ot 遇阻塞放回（无成功离队）不记。
        ops = self.base_ops(ttl=100000) + [
            self.oa("c1", 0),                  # A：不记 wh
            self.oa("c2", 0),                  # Q
            {"op": "ot", "now": 5},            # c2 阻塞放回：不记
            {"op": "wh", "from": 0, "to": 0, "now": 5},
        ]
        window = self.run_ops(ops)[-1]["windows"][0]
        self.assertEqual(window["admitted"], self.ZERO)
        self.assertEqual(window["expired"], self.ZERO)
        self.assertEqual(window["cancelled"], self.ZERO)
        self.assertEqual(window["evicted"], self.ZERO)

    def test_wh_failed_batch_rolls_back(self):
        # oc 取消成功记账后，批内后续操作失败：整批回滚，wh 记账不落盘。
        self.assert_failure(
            self.base_ops() + [
                self.oa("c1", 0),
                self.oa("c2", 0),
                {"op": "oc", "cid": "c2"},    # 取消（已记账）
                {"op": "oc", "cid": "ghost"},  # CONNECTION：整批失败
                {"op": "wh", "from": 0, "to": 0, "now": 0},
            ],
            5, "CONNECTION",
        )

    def test_wh_unconfigured_os_is_state(self):
        self.assert_failure(
            [{"op": "add", "id": "a", "weight": 1},
             {"op": "wh", "from": 0, "to": 0, "now": 0}],
            4, "STATE",
        )

    def test_wh_premature_from_is_state(self):
        # now=3600 时当前窗为 60，from 早于下界 1 报 STATE。
        self.assert_failure(
            self.base_ops() + [{"op": "wh", "from": 0, "to": 0, "now": 3600}],
            4, "STATE",
        )

    def test_wh_input_violations(self):
        bad_ops = [
            # 键序不符（须 op,from,to,now）。
            b'{"ops":[{"op":"wh","to":0,"from":0,"now":0}]}',
            # 缺键、多键。
            b'{"ops":[{"op":"wh","from":0,"to":0}]}',
            b'{"ops":[{"op":"wh","from":0,"to":0,"now":0,"x":1}]}',
            # bool 不是合法数值。
            b'{"ops":[{"op":"wh","from":false,"to":0,"now":0}]}',
            # 范围：now 超 10^9、from 为负。
            b'{"ops":[{"op":"wh","from":0,"to":0,"now":1000000001}]}',
            b'{"ops":[{"op":"wh","from":-1,"to":0,"now":0}]}',
            # 关系：from>to、to-from>=60、to>now//60。
            b'{"ops":[{"op":"wh","from":2,"to":1,"now":180}]}',
            b'{"ops":[{"op":"wh","from":0,"to":60,"now":3600}]}',
            b'{"ops":[{"op":"wh","from":0,"to":2,"now":60}]}',
        ]
        for raw in bad_ops:
            code, stdout, stderr = run_balancer("run", raw)
            self.assertEqual(code, 2)
            self.assertEqual(stdout, b"")
            self.assertEqual(stderr, b'{"error":"INPUT"}\n')
        # 时钟倒退报 INPUT。
        self.assert_failure(
            self.base_ops() + [
                {"op": "wh", "from": 0, "to": 1, "now": 120},
                {"op": "wh", "from": 0, "to": 0, "now": 60},
            ],
            2, "INPUT",
        )

    def test_ci_cb_ca_clear_wait_history(self):
        config = config_v7(
            1, vnodes=1, overload={"cap": 1, "q": 4, "ttl": 100}
        )
        # ci 成功清空排队等待历史。
        ops = self.base_ops(ttl=100) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "oc", "cid": "c2"},
            {"op": "close", "cid": "c1", "now": 1},
            {"op": "ci", "config": config, "now": 2},
            {"op": "wh", "from": 0, "to": 0, "now": 2},
        ]
        window = self.run_ops(ops)[-1]["windows"][0]
        self.assertEqual(window["cancelled"], self.ZERO)
        # cb 回滚成功同样清空。
        ops = [
            {"op": "ci", "config": config, "now": 0},
            self.oa("c1", 1),
            self.oa("c2", 1),
            {"op": "oq", "now": 3},
            {"op": "oc", "cid": "c2"},
            {"op": "close", "cid": "c1", "now": 4},
            {"op": "cb", "rev": 1, "now": 5},
            {"op": "wh", "from": 0, "to": 0, "now": 5},
        ]
        window = self.run_ops(ops)[-1]["windows"][0]
        self.assertEqual(window["cancelled"], self.ZERO)
        # cp 预约、ca 生效成功清空。
        ops = self.base_ops(ttl=100) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "oc", "cid": "c2"},
            {"op": "close", "cid": "c1", "now": 1},
            {"op": "cp", "config": config, "at": 2, "now": 2},
            {"op": "ca", "now": 2},
            {"op": "wh", "from": 0, "to": 0, "now": 2},
        ]
        window = self.run_ops(ops)[-1]["windows"][0]
        self.assertEqual(window["cancelled"], self.ZERO)

    def test_record_replay_covers_wh(self):
        ops = self.base_ops(ttl=10) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "oq", "now": 4},
            {"op": "oc", "cid": "c2"},
            {"op": "ot", "now": 20},
            {"op": "wh", "from": 0, "to": 0, "now": 20},
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


class WaitPercentileTest(unittest.TestCase):
    """wp 排队等待分位查询：kind 映射、五桶汇总、rank/bucket/upper 与校验。"""

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

    def base_ops(self, ttl=10, cap=1, q=10):
        # 环上唯一后端 a，cap=1 便于制造入队。
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": cap, "q": q, "ttl": ttl},
        ]

    def oa(self, cid, now, key="k"):
        return {"op": "oa", "cid": cid, "flow": FLOW,
                "c": "k", "s": "k", "key": key, "now": now}

    ZERO = [0, 0, 0, 0, 0]

    def admitted_ops(self, ttl=100000):
        # c2..c6 接纳延迟 0/1/10/100/101，分别落桶 0/1/2/3/4；前三在窗 0、
        # 后二在窗 1。
        return self.base_ops(ttl=ttl) + [
            self.oa("c1", 0),
            self.oa("c2", 0), self.oa("c3", 0), self.oa("c4", 0),
            self.oa("c5", 0), self.oa("c6", 0),
            {"op": "close", "cid": "c1", "now": 0},
            {"op": "ot", "now": 0},
            {"op": "close", "cid": "c2", "now": 1},
            {"op": "ot", "now": 1},
            {"op": "close", "cid": "c3", "now": 10},
            {"op": "ot", "now": 10},
            {"op": "close", "cid": "c4", "now": 100},
            {"op": "ot", "now": 100},
            {"op": "close", "cid": "c5", "now": 101},
            {"op": "ot", "now": 101},
        ]

    def wp(self, kind, start, end, p, now):
        return {"op": "wp", "kind": kind, "from": start,
                "to": end, "p": p, "now": now}

    def test_wp_no_samples_key_order_and_nulls(self):
        ops = self.base_ops() + [self.wp("A", 0, 0, 50, 0)]
        result = self.run_ops(ops)[3]
        self.assertEqual(
            list(result),
            ["op", "kind", "from", "to", "p", "samples", "buckets",
             "rank", "bucket", "upper"],
        )
        self.assertEqual(result["op"], "wp")
        self.assertEqual(result["kind"], "A")
        self.assertEqual(result["from"], 0)
        self.assertEqual(result["to"], 0)
        self.assertEqual(result["p"], 50)
        self.assertEqual(result["samples"], 0)
        self.assertEqual(result["buckets"], self.ZERO)
        self.assertEqual(result["rank"], 0)
        self.assertIsNone(result["bucket"])
        self.assertIsNone(result["upper"])

    def test_wp_aggregates_buckets_and_percentiles(self):
        ops = self.admitted_ops()
        # 闭区间两窗汇总：五桶各 1。
        result = self.run_ops(ops + [self.wp("A", 0, 1, 100, 101)])[-1]
        self.assertEqual(result["buckets"], [1, 1, 1, 1, 1])
        self.assertEqual(result["samples"], 5)
        # rank=ceil(p*5/100) 与累计首次不小于 rank 的桶及上界。
        cases = [
            (1, 1, 0, 0),
            (40, 2, 1, 1),
            (60, 3, 2, 10),
            (80, 4, 3, 100),
            (100, 5, 4, None),
        ]
        for p, want_rank, want_bucket, want_upper in cases:
            result = self.run_ops(ops + [self.wp("A", 0, 1, p, 101)])[-1]
            self.assertEqual(result["rank"], want_rank, p)
            self.assertEqual(result["bucket"], want_bucket, p)
            self.assertEqual(result["upper"], want_upper, p)

    def test_wp_single_window_is_within_range(self):
        ops = self.admitted_ops()
        # 仅查窗 0：桶 0/1/2 各 1。
        result = self.run_ops(ops + [self.wp("A", 0, 0, 100, 101)])[-1]
        self.assertEqual(result["buckets"], [1, 1, 1, 0, 0])
        self.assertEqual(result["samples"], 3)
        # p=100：累计到桶 2 才达 rank=3，upper=10。
        self.assertEqual(result["rank"], 3)
        self.assertEqual(result["bucket"], 2)
        self.assertEqual(result["upper"], 10)

    def test_wp_kind_letters_map_to_histories(self):
        # C：取消（逻辑时钟 5，d=5 落 ≤10 桶）。
        ops = self.base_ops() + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "oq", "now": 5},
            {"op": "oc", "cid": "c2"},
            self.wp("C", 0, 0, 50, 5),
        ]
        result = self.run_ops(ops)[-1]
        self.assertEqual(result["buckets"], [0, 0, 1, 0, 0])
        self.assertEqual(result["samples"], 1)
        self.assertEqual((result["rank"], result["bucket"], result["upper"]),
                         (1, 2, 10))
        # E：过期（d=10 落 ≤10 桶）。
        ops = self.base_ops(ttl=10) + [
            self.oa("c1", 0),
            self.oa("c2", 0),
            {"op": "ot", "now": 10},
            self.wp("E", 0, 0, 1, 10),
        ]
        result = self.run_ops(ops)[-1]
        self.assertEqual(result["buckets"], [0, 0, 1, 0, 0])
        self.assertEqual((result["rank"], result["bucket"], result["upper"]),
                         (1, 2, 10))
        # V：H 头淘汰（d=5 落 ≤10 桶）。
        ops = self.base_ops(q=1) + [
            {"op": "rp", "mode": "H"},
            self.oa("c0", 0, key="k0"),
            self.oa("c1", 0, key="k1"),
            self.oa("c2", 5, key="k2"),
            self.wp("V", 0, 0, 100, 5),
        ]
        result = self.run_ops(ops)[-1]
        self.assertEqual(result["buckets"], [0, 0, 1, 0, 0])
        self.assertEqual((result["rank"], result["bucket"], result["upper"]),
                         (1, 2, 10))

    def test_wp_empty_kind_has_zero_samples(self):
        # 只有 admitted 事件，查 E 无样本。
        ops = self.admitted_ops() + [self.wp("E", 0, 1, 100, 101)]
        result = self.run_ops(ops)[-1]
        self.assertEqual(result["samples"], 0)
        self.assertEqual(result["buckets"], self.ZERO)
        self.assertEqual(result["rank"], 0)
        self.assertIsNone(result["bucket"])
        self.assertIsNone(result["upper"])

    def test_wp_advances_clock(self):
        # wp 推进共用时钟：其后更早 now 的操作报时钟倒退。
        self.assert_failure(
            self.base_ops() + [
                self.wp("A", 0, 1, 50, 120),
                self.wp("A", 0, 1, 50, 60),
            ],
            2, "INPUT",
        )

    def test_wp_unconfigured_os_is_state(self):
        self.assert_failure(
            [{"op": "add", "id": "a", "weight": 1},
             self.wp("A", 0, 0, 50, 0)],
            4, "STATE",
        )

    def test_wp_premature_from_is_state(self):
        # now=3600 时当前窗为 60，from 早于下界 1 报 STATE。
        self.assert_failure(
            self.base_ops() + [self.wp("A", 0, 0, 50, 3600)],
            4, "STATE",
        )

    def test_wp_failed_batch_rolls_back(self):
        # oc 取消成功记账后批内后续操作失败：整批回滚，wp 不落输出。
        self.assert_failure(
            self.base_ops() + [
                self.oa("c1", 0),
                self.oa("c2", 0),
                {"op": "oc", "cid": "c2"},
                self.wp("C", 0, 0, 50, 0),
                {"op": "oc", "cid": "ghost"},
            ],
            5, "CONNECTION",
        )

    def test_wp_input_violations(self):
        bad_ops = [
            # 键序不符（须 op,kind,from,to,p,now）。
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"now":0,"p":1}]}',
            # 缺键、多键。
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":1}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":1,"now":0,"x":1}]}',
            # kind 非法：全称、小写、非串。
            b'{"ops":[{"op":"wp","kind":"admitted","from":0,"to":0,"p":1,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"a","from":0,"to":0,"p":1,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":1,"from":0,"to":0,"p":1,"now":0}]}',
            # p：bool、越界 0/101、浮点、字符串。
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":true,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":0,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":101,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":1.0,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":"1","now":0}]}',
            # from/to/now：bool、负数、超 10^9。
            b'{"ops":[{"op":"wp","kind":"A","from":false,"to":0,"p":1,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":1,"now":true}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":-1,"to":0,"p":1,"now":0}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":0,"p":1,"now":1000000001}]}',
            # 关系：from>to、to-from>=60、to>now//60。
            b'{"ops":[{"op":"wp","kind":"A","from":2,"to":1,"p":1,"now":180}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":60,"p":1,"now":3600}]}',
            b'{"ops":[{"op":"wp","kind":"A","from":0,"to":2,"p":1,"now":60}]}',
        ]
        for raw in bad_ops:
            code, stdout, stderr = run_balancer("run", raw)
            self.assertEqual(code, 2, raw)
            self.assertEqual(stdout, b"")
            self.assertEqual(stderr, b'{"error":"INPUT"}\n')

    def test_record_replay_covers_wp(self):
        ops = self.admitted_ops(ttl=100000) + [
            self.wp("A", 0, 1, 90, 101),
            self.wp("C", 0, 0, 50, 101),
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


class WaitAlertTest(unittest.TestCase):
    """wa 排队等待分位告警：单窗 wp 口径、各 kind 独立 N/A 滞回状态机、
    同窗缓存、变参/跳窗/窗口与时钟拒绝、ci 清空及 record/replay。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, 0, err)
        self.assertEqual(err, b"")
        return json.loads(out.decode("utf-8"))["results"]

    def assert_failure(self, ops, exit_code, label):
        if isinstance(ops, (bytes, bytearray)):
            raw = bytes(ops)
        else:
            raw = encode_ops(ops)
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code, raw)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    @staticmethod
    def oa(cid, now, key="k"):
        return {"op": "oa", "cid": cid, "flow": FLOW,
                "c": "k", "s": "k", "key": key, "now": now}

    @staticmethod
    def close(cid, now):
        return {"op": "close", "cid": cid, "now": now}

    @staticmethod
    def wa(kind, w, now, p=100, hi=1, lo=0, n=1):
        return {"op": "wa", "kind": kind, "w": w, "p": p,
                "hi": hi, "lo": lo, "n": n, "now": now}

    def base_ops(self, cap=1, q=20, ttl=100000):
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": cap, "q": q, "ttl": ttl},
        ]

    def scenario_start(self):
        """c1 在 now=0 立即建连为活动连接；返回 (ops, active, 下一序号)。"""
        ops = self.base_ops() + [self.oa("c1", 0)]
        return ops, "c1", 2

    def admit_high(self, ops, active, index, w):
        """在窗 w 制造一个 bucket1（d=1）的 admitted 样本。
        入队于 60w，于 60w+1 接纳，返回新活动 cid。"""
        cid = "c%d" % index
        t = 60 * w
        ops += [
            self.oa(cid, t),
            self.close(active, t + 1),
            {"op": "ot", "now": t + 1},
        ]
        return cid

    def admit_low(self, ops, active, index, w):
        """在窗 w 制造一个 bucket0（d=0）的 admitted 样本。
        入队与接纳同在 60w，返回新活动 cid。"""
        cid = "c%d" % index
        t = 60 * w
        ops += [
            self.oa(cid, t),
            self.close(active, t),
            {"op": "ot", "now": t},
        ]
        return cid

    def wa_results(self, results):
        return [r for r in results if r.get("op") == "wa"]

    # ---- 键序、空窗与单窗 wp 口径 ----

    def test_empty_window_is_zero_null_null_from_N(self):
        out = self.results(
            self.base_ops() + [self.wa("A", 0, 60)]
        )[-1]
        self.assertEqual(
            list(out),
            ["op", "kind", "w", "p", "state", "samples", "bucket",
             "upper", "run", "changed"],
        )
        self.assertEqual(out["op"], "wa")
        self.assertEqual(out["kind"], "A")
        self.assertEqual(out["w"], 0)
        self.assertEqual(out["p"], 100)
        self.assertEqual(out["state"], "N")
        self.assertEqual(out["samples"], 0)
        self.assertIsNone(out["bucket"])
        self.assertIsNone(out["upper"])
        self.assertEqual(out["run"], 0)
        self.assertFalse(out["changed"])

    def test_single_window_uses_wp_buckets_and_uppers(self):
        # c1 活动、c2..c6 排队；接纳延迟 0/1/10 落窗 0，100/101 落窗 1。
        ops, active, _ = self.scenario_start()
        ops += [self.oa("c%d" % i, 0) for i in range(2, 7)]
        leaves = [("c1", 0), ("c2", 1), ("c3", 10),
                  ("c4", 100), ("c5", 101)]
        for cid, leave in leaves:
            ops += [self.close(cid, leave), {"op": "ot", "now": leave}]
        # 窗 0：桶 0/1/2 各 1；p=100 取最末非空桶 2，upper=10。
        out = self.results(ops + [self.wa("A", 0, 120)])[-1]
        self.assertEqual(out["samples"], 3)
        self.assertEqual(out["bucket"], 2)
        self.assertEqual(out["upper"], 10)
        # 窗 1：桶 3/4 各 1；p=100 取桶 4，upper=null。
        out = self.results(ops + [self.wa("A", 1, 120)])[-1]
        self.assertEqual(out["samples"], 2)
        self.assertEqual(out["bucket"], 4)
        self.assertIsNone(out["upper"])

    def test_percentile_rank_picks_bucket(self):
        # 窗 0：桶 0 两个样本（d=0）、桶 2 一个（d=10）。
        def build():
            ops, active, _ = self.scenario_start()
            ops += [self.oa("c2", 0), self.oa("c3", 0), self.oa("c4", 0)]
            ops += [
                self.close("c1", 0), {"op": "ot", "now": 0},
                self.close("c2", 0), {"op": "ot", "now": 0},
                self.close("c3", 10), {"op": "ot", "now": 10},
            ]
            return ops
        ops = build()
        # 不同 p 须独立批次（同窗异参属变参 STATE）。
        self.assertEqual(self.results(ops + [self.wa("A", 0, 60, p=1)])[-1]["bucket"], 0)
        self.assertEqual(self.results(ops + [self.wa("A", 0, 60, p=67)])[-1]["bucket"], 2)
        self.assertEqual(self.results(ops + [self.wa("A", 0, 60, p=100)])[-1]["bucket"], 2)

    def test_kind_letters_are_independent_state_machines(self):
        # 窗 0 仅一个 admitted bucket1 样本；E/C/V 无样本。
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops += [
            self.wa("A", 0, 60, hi=1, lo=0),
            self.wa("E", 0, 60, hi=1, lo=0),
            self.wa("C", 0, 60, hi=1, lo=0),
            self.wa("V", 0, 60, hi=1, lo=0),
        ]
        out = {r["kind"]: r for r in self.wa_results(self.results(ops))}
        self.assertTrue(out["A"]["changed"])
        self.assertEqual(out["A"]["state"], "A")
        self.assertEqual(out["A"]["bucket"], 1)
        for kind in ("E", "C", "V"):
            self.assertEqual(out[kind]["state"], "N")
            self.assertFalse(out[kind]["changed"])
            self.assertEqual(out[kind]["samples"], 0)
            self.assertIsNone(out[kind]["bucket"])

    def test_kind_letters_map_to_wait_histories(self):
        # A：admitted d=1 -> 桶1。
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        out = self.results(ops + [self.wa("A", 0, 60)])[-1]
        self.assertEqual((out["samples"], out["bucket"], out["upper"]),
                         (1, 1, 1))
        # C：oc 在时钟推进到 5 后取消，d=5 落桶2。
        ops = self.base_ops() + [self.oa("c1", 0), self.oa("c2", 0),
                                 {"op": "oq", "now": 5},
                                 {"op": "oc", "cid": "c2"}]
        out = self.results(ops + [self.wa("C", 0, 60)])[-1]
        self.assertEqual((out["samples"], out["bucket"], out["upper"]),
                         (1, 2, 10))
        # E：ttl=10 到期，d=10 落桶2。
        ops = self.base_ops(ttl=10) + [
            self.oa("c1", 0), self.oa("c2", 0), {"op": "ot", "now": 10},
        ]
        out = self.results(ops + [self.wa("E", 0, 60)])[-1]
        self.assertEqual((out["samples"], out["bucket"], out["upper"]),
                         (1, 2, 10))
        # V：H 模式头淘汰 d=5 落桶2。
        ops = self.base_ops(q=1) + [
            {"op": "rp", "mode": "H"},
            self.oa("c0", 0, key="k0"),
            self.oa("c1", 0, key="k1"),
            self.oa("c2", 5, key="k2"),
        ]
        out = self.results(ops + [self.wa("V", 0, 60)])[-1]
        self.assertEqual((out["samples"], out["bucket"], out["upper"]),
                         (1, 2, 10))

    # ---- N/A 滞回状态机 ----

    def test_hi_boundary_inclusive_transitions_to_A(self):
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)  # bucket1>=hi=1
        out = self.results(ops + [self.wa("A", 0, 60, hi=1, lo=0)])[-1]
        self.assertEqual(out["state"], "A")
        self.assertTrue(out["changed"])
        self.assertEqual(out["run"], 0)

    def test_n_run_accumulates_then_transitions(self):
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops.append(self.wa("A", 0, 60, hi=1, lo=0, n=2))      # run=1
        # 窗 1 空窗：方向不符清 0。
        ops.append(self.wa("A", 1, 120, hi=1, lo=0, n=2))
        active = self.admit_high(ops, active, idx + 1, 2)
        ops.append(self.wa("A", 2, 180, hi=1, lo=0, n=2))      # run=1
        active = self.admit_high(ops, active, idx + 2, 3)
        ops.append(self.wa("A", 3, 240, hi=1, lo=0, n=2))      # run=2->A
        seq = [(r["w"], r["state"], r["run"], r["changed"])
               for r in self.wa_results(self.results(ops))]
        self.assertEqual(seq, [
            (0, "N", 1, False),
            (1, "N", 0, False),
            (2, "N", 1, False),
            (3, "A", 0, True),
        ])

    def test_empty_window_clears_run_in_A(self):
        # n=1：窗 0 bucket1 转 A；窗 1 空窗 bucket=null 不满足 <=lo，留 A
        # run=0；窗 2 bucket0<=lo=0 转回 N。
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops.append(self.wa("A", 0, 60, hi=1, lo=0))            # A
        ops.append(self.wa("A", 1, 120, hi=1, lo=0))           # 空窗留 A
        active = self.admit_low(ops, active, idx + 1, 2)
        ops.append(self.wa("A", 2, 180, hi=1, lo=0))           # bucket0->N
        seq = [(r["w"], r["state"], r["run"], r["changed"], r["bucket"])
               for r in self.wa_results(self.results(ops))]
        self.assertEqual(seq, [
            (0, "A", 0, True, 1),
            (1, "A", 0, False, None),
            (2, "N", 0, True, 0),
        ])

    def test_lo_boundary_inclusive_transitions_to_N(self):
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops.append(self.wa("A", 0, 60, hi=1, lo=0))
        active = self.admit_low(ops, active, idx + 1, 1)
        ops.append(self.wa("A", 1, 120, hi=1, lo=0))
        self.assertEqual(
            [(r["state"], r["changed"])
             for r in self.wa_results(self.results(ops))],
            [("A", True), ("N", True)],
        )

    def test_other_kinds_keep_state_when_one_kind_transitions(self):
        # A 在窗0转 A；窗1 A 给低样本转回 N，期间 E 始终无样本留 N，二者
        # 状态互不影响。
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops += [self.wa("A", 0, 60), self.wa("E", 0, 60)]
        active = self.admit_low(ops, active, idx + 1, 1)
        ops += [self.wa("A", 1, 120), self.wa("E", 1, 120)]
        out = self.wa_results(self.results(ops))
        a = [r for r in out if r["kind"] == "A"]
        e = [r for r in out if r["kind"] == "E"]
        self.assertEqual([(r["state"], r["changed"]) for r in a],
                         [("A", True), ("N", True)])
        self.assertEqual([(r["state"], r["changed"]) for r in e],
                         [("N", False), ("N", False)])

    # ---- 同窗缓存 ----

    def test_same_window_same_params_returns_cached_result(self):
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops += [
            self.wa("A", 0, 60),
            self.wa("A", 0, 120),   # 同窗同参，时钟可继续走
        ]
        out = self.wa_results(self.results(ops))
        self.assertEqual(out[0], out[1])
        self.assertTrue(out[1]["changed"])

    def test_cached_does_not_advance_state_machine(self):
        # 窗0 转 A；窗0 重报返回缓存 A；窗1 低样本仍从 A 出发（n=1）转 N。
        # 缓存评估取 now=60，以便随后在 60 入队并于窗1 接纳 bucket0 样本。
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops += [self.wa("A", 0, 60), self.wa("A", 0, 60)]
        active = self.admit_low(ops, active, idx + 1, 1)
        ops.append(self.wa("A", 1, 120))
        seq = [(r["w"], r["state"], r["changed"])
               for r in self.wa_results(self.results(ops))]
        self.assertEqual(seq, [(0, "A", True), (0, "A", True),
                               (1, "N", True)])

    def test_same_window_changed_param_is_state(self):
        base = self.base_ops() + [self.wa("A", 0, 60, p=100, hi=2, lo=1, n=1)]
        for second in (
            self.wa("A", 0, 60, p=99, hi=2, lo=1, n=1),
            self.wa("A", 0, 60, p=100, hi=3, lo=1, n=1),
            self.wa("A", 0, 60, p=100, hi=2, lo=0, n=1),
            self.wa("A", 0, 60, p=100, hi=2, lo=1, n=2),
        ):
            self.assert_failure(base + [second], 4, "STATE")

    def test_param_change_next_window_is_state(self):
        base = self.base_ops() + [self.wa("A", 0, 60, p=100, hi=2, lo=1, n=1)]
        for second in (
            self.wa("A", 1, 120, p=99, hi=2, lo=1, n=1),
            self.wa("A", 1, 120, p=100, hi=3, lo=1, n=1),
            self.wa("A", 1, 120, p=100, hi=2, lo=0, n=1),
            self.wa("A", 1, 120, p=100, hi=2, lo=1, n=2),
        ):
            self.assert_failure(base + [second], 4, "STATE")

    def test_other_kind_same_window_is_independent_first_eval(self):
        # A 首评后，同窗 E 是其自身首评（非缓存、非变参）。
        ops = self.base_ops() + [
            self.wa("A", 0, 60), self.wa("E", 0, 60),
        ]
        out = self.wa_results(self.results(ops))
        self.assertEqual([r["kind"] for r in out], ["A", "E"])
        self.assertEqual([r["run"] for r in out], [0, 0])

    # ---- 跳窗与窗口关系 ----

    def test_skip_window_is_state(self):
        base = self.base_ops() + [self.wa("A", 0, 60)]
        self.assert_failure(base + [self.wa("A", 2, 180)], 4, "STATE")

    def test_window_rollback_is_state(self):
        # 先评窗1 再评窗0 属回退跳窗。
        base = self.base_ops() + [self.wa("A", 1, 120)]
        self.assert_failure(base + [self.wa("A", 0, 120)], 4, "STATE")

    def test_window_not_ended_is_state(self):
        # now=0 当前窗0，w=0 尚未结束。
        self.assert_failure(
            self.base_ops() + [self.wa("A", 0, 0)], 4, "STATE"
        )
        # w 等于当前窗同样未结束。
        self.assert_failure(
            self.base_ops() + [self.wa("A", 2, 120)], 4, "STATE"
        )

    def test_window_too_old_is_state(self):
        # now=3600 当前窗60，下界为 1；w=0 超出最近 60 窗。
        self.assert_failure(
            self.base_ops() + [self.wa("A", 0, 3600)], 4, "STATE"
        )

    def test_retention_boundary_w_accepted(self):
        # w=current-59=1 恰在保留下界上，合法。
        out = self.results(
            self.base_ops() + [self.wa("A", 1, 3600)]
        )[-1]
        self.assertEqual(out["w"], 1)

    def test_unconfigured_os_is_state(self):
        self.assert_failure(
            [{"op": "add", "id": "a", "weight": 1},
             self.wa("A", 0, 60)],
            4, "STATE",
        )

    def test_clock_regression_is_input(self):
        self.assert_failure(
            self.base_ops() + [
                self.wa("A", 0, 120),
                self.wa("A", 1, 60),
            ],
            2, "INPUT",
        )

    # ---- 输入校验 ----

    def test_input_violations(self):
        bad_ops = [
            # 键序不符（须 op,kind,w,p,hi,lo,n,now）。
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"now":60,"n":1}]}',
            # 缺键、多键。
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":1,"now":60,"x":1}]}',
            # kind：全称、小写、非串。
            b'{"ops":[{"op":"wa","kind":"admitted","w":0,"p":1,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"a","w":0,"p":1,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"B","w":0,"p":1,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":1,"w":0,"p":1,"hi":1,"lo":0,"n":1,"now":60}]}',
            # p：bool、越界 0/101、浮点、字符串。
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":true,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":0,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":101,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1.0,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":"1","hi":1,"lo":0,"n":1,"now":60}]}',
            # hi/lo：bool、越界、浮点。
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":true,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":5,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":-1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":4,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1.0,"lo":0,"n":1,"now":60}]}',
            # lo<hi 关系：lo==hi。
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":1,"n":1,"now":60}]}',
            # n：bool、越界 0/61、浮点。
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":true,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":0,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":61,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":1.0,"now":60}]}',
            # w/now：bool、负数、超 10^9。
            b'{"ops":[{"op":"wa","kind":"A","w":false,"p":1,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":-1,"p":1,"hi":1,"lo":0,"n":1,"now":60}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":1,"now":1000000001}]}',
            b'{"ops":[{"op":"wa","kind":"A","w":0,"p":1,"hi":1,"lo":0,"n":1,"now":true}]}',
        ]
        for raw in bad_ops:
            code, stdout, stderr = run_balancer("run", raw)
            self.assertEqual(code, 2, raw)
            self.assertEqual(stdout, b"")
            self.assertEqual(stderr, b'{"error":"INPUT"}\n', raw)

    # ---- 失败批回滚、ci 清空、record/replay ----

    def test_failed_batch_rolls_back(self):
        # oc 取消成功记账后批内后续操作非法：整批回滚，无 stdout。
        ops = self.base_ops() + [
            self.oa("c1", 0), self.oa("c2", 0),
            {"op": "oq", "now": 5},
            {"op": "oc", "cid": "c2"},
            self.wa("C", 0, 60),
            {"op": "oc", "cid": "ghost"},
        ]
        self.assert_failure(ops, 5, "CONNECTION")

    def test_ci_clears_wait_alerts(self):
        # 窗0 admitted bucket1，wa 首评即转 A。
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)
        ops += [self.wa("A", 0, 60), {"op": "ce"}]
        exported = self.results(ops)
        config = next(r for r in exported if r["op"] == "ce")["config"]
        # 关闭活动连接后 ci 成功；wa 状态被清空。
        ops += [
            self.close(active, 119),
            {"op": "ci", "now": 3600, "config": config},
            # 若未清空，w=59 相对旧 w=0 为跳窗报 STATE；清空后为全新首评。
            self.wa("A", 59, 3600),
            # 再评 +1 窗也合法（序列已重置）。
            self.wa("A", 60, 3660),
        ]
        out = self.wa_results(self.results(ops))
        self.assertEqual(out[0]["state"], "A")
        # ci 后两次评估均为全新首评：空窗 N、run=0、无转换。
        self.assertEqual((out[1]["w"], out[1]["state"], out[1]["changed"],
                          out[1]["samples"]), (59, "N", False, 0))
        self.assertEqual((out[2]["w"], out[2]["state"], out[2]["changed"],
                          out[2]["samples"]), (60, "N", False, 0))

    def test_record_replay_covers_wa(self):
        ops, active, idx = self.scenario_start()
        active = self.admit_high(ops, active, idx, 0)   # 窗0 bucket1
        ops.append(self.wa("A", 0, 60, hi=1, lo=0))     # -> A
        ops.append(self.wa("A", 0, 60))                 # 同窗缓存
        active = self.admit_low(ops, active, idx + 1, 1)
        ops.append(self.wa("A", 1, 120, hi=1, lo=0))    # bucket0 -> N
        ops.append(self.wa("E", 0, 120))                # 另一 kind 首评
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


class RuUnavailabilityTest(unittest.TestCase):
    """不可用时长查询 ru：四类原因 since/duration、起算与清除、输入校验。"""

    def assert_failure(self, ops, exit_code, label):
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def assert_failure_raw(self, raw, exit_code, label):
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def ru_of(self, ops, backend_id, now):
        code, stdout, stderr = run_balancer(
            "run",
            encode_ops(ops + [{"op": "ru", "id": backend_id, "now": now}]),
        )
        self.assertEqual((code, stderr), (0, b""))
        results = json.loads(stdout.decode("utf-8"))["results"]
        return results[-1]

    def test_healthy_backend_has_empty_reasons(self):
        result = self.ru_of(
            [{"op": "add", "id": "a", "weight": 1}], "a", 7
        )
        self.assertEqual(
            result,
            {"op": "ru", "id": "a", "now": 7, "reasons": []},
        )

    def test_result_byte_layout(self):
        raw = encode_ops([
            {"op": "add", "id": "a", "weight": 1},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 2},
            {"op": "ru", "id": "a", "now": 10},
        ])
        code, out, err = run_balancer("run", raw)
        self.assertEqual((code, err), (0, b""))
        self.assertIn(
            b'{"op":"ru","id":"a","now":10,"reasons":[{"reason":"health",'
            b'"since":2,"duration":8}]}',
            out,
        )
        self.assertTrue(out.endswith(b"\n"))
        self.assertNotIn(b"\n", out[:-1])

    def test_health_since_and_recovery_clear(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 10},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 25)["reasons"],
            [{"reason": "health", "since": 10, "duration": 15}],
        )
        # 转 unhealthy 之后的失败/成功未达阈值探测不改变 since。
        ops += [
            {"op": "probe", "id": "a", "ok": False, "now": 11},
            {"op": "probe", "id": "a", "ok": False, "now": 12},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 25)["reasons"],
            [{"reason": "health", "since": 10, "duration": 15}],
        )
        # 同参 probe 重报不重置 since。
        ops += [{"op": "probe", "id": "a", "ok": False, "now": 12}]
        self.assertEqual(
            self.ru_of(ops, "a", 25)["reasons"],
            [{"reason": "health", "since": 10, "duration": 15}],
        )
        # 恢复 healthy：since 清除；再次转换自新 probe 的 now 起算。
        ops += [
            {"op": "probe", "id": "a", "ok": True, "now": 20},
        ]
        self.assertEqual(self.ru_of(ops, "a", 25)["reasons"], [])
        ops += [
            {"op": "probe", "id": "a", "ok": False, "now": 30},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 35)["reasons"],
            [{"reason": "health", "since": 30, "duration": 5}],
        )

    def test_drain_since_d_x_keep_and_du_clear(self):
        # dr 时持有连接转 D；末连 close 使 D→X 不重置 since。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "ds", "id": "a", "t": 10},
            {"op": "dr", "id": "a", "now": 10},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 15)["reasons"],
            [{"reason": "drain", "since": 10, "duration": 5}],
        )
        # dr 在 D 态重报（now 相同或更新）幂等，不重置 since。
        ops += [
            {"op": "dr", "id": "a", "now": 10},
            {"op": "dr", "id": "a", "now": 14},
        ]
        ops += [{"op": "close", "cid": "x", "now": 20}]
        self.assertEqual(
            self.ru_of(ops, "a", 30)["reasons"],
            [{"reason": "drain", "since": 10, "duration": 20}],
        )
        # du 清除；新一轮 dr 自新 now 起算（无连接直接转 X）。
        ops += [{"op": "du", "id": "a", "now": 40}]
        self.assertEqual(self.ru_of(ops, "a", 40)["reasons"], [])
        ops += [{"op": "dr", "id": "a", "now": 50}]
        self.assertEqual(
            self.ru_of(ops, "a", 55)["reasons"],
            [{"reason": "drain", "since": 50, "duration": 5}],
        )

    def test_drain_since_survives_dg_deadline(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "ds", "id": "a", "t": 5},
            {"op": "dr", "id": "a", "now": 0},
            {"op": "dg", "id": "a", "now": 5},
        ]
        # dg 到期强关转 X（end=deadline=5），drain since 仍为 0。
        self.assertEqual(
            self.ru_of(ops, "a", 9)["reasons"],
            [{"reason": "drain", "since": 0, "duration": 9}],
        )

    def test_circuit_since_open_half_reopen_close(self):
        # w=10：0 转 O，10 到期后 cr 先转 H；H 失败重开 O 不重置；
        # 成功达 q=1 回 C 清除。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "cs", "id": "a", "n": 2, "m": 1, "r": 1, "w": 10,
             "q": 1},
            {"op": "cr", "id": "a", "ok": False, "now": 0},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 5)["reasons"],
            [{"reason": "circuit", "since": 0, "duration": 5}],
        )
        # now=10 到期，本条失败：先 O→H 再 H→O，since 不重置。
        ops += [{"op": "cr", "id": "a", "ok": False, "now": 10}]
        self.assertEqual(
            self.ru_of(ops, "a", 12)["reasons"],
            [{"reason": "circuit", "since": 0, "duration": 12}],
        )
        # cg 在 now=20 触发 O→H（新恢复窗 10..20），仍不重置 since。
        ops += [{"op": "cg", "id": "a", "now": 20}]
        self.assertEqual(
            self.ru_of(ops, "a", 20)["reasons"],
            [{"reason": "circuit", "since": 0, "duration": 20}],
        )
        # H 成功回 C：清除。
        ops += [{"op": "cr", "id": "a", "ok": True, "now": 20}]
        self.assertEqual(self.ru_of(ops, "a", 21)["reasons"], [])

    def test_circuit_replay_no_reset_and_cs_clear(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "cs", "id": "a", "n": 1, "m": 1, "r": 1, "w": 100,
             "q": 1},
            {"op": "cr", "id": "a", "ok": False, "now": 0},
            {"op": "cr", "id": "a", "ok": False, "now": 0},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 7)["reasons"],
            [{"reason": "circuit", "since": 0, "duration": 7}],
        )
        # cs 异参重配置 C：清除。
        ops += [
            {"op": "cs", "id": "a", "n": 2, "m": 1, "r": 1, "w": 100,
             "q": 1},
        ]
        self.assertEqual(self.ru_of(ops, "a", 8)["reasons"], [])

    def test_fault_d_segment_since_is_a(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "D", "a": 10, "z": 100, "v": 0},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 35)["reasons"],
            [{"reason": "fault", "since": 10, "duration": 25}],
        )
        # 段前与段间隙均无 fault。
        self.assertEqual(self.ru_of(ops, "a", 9)["reasons"], [])
        ops2 = ops + [
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 3, "v": 0},
        ]
        self.assertEqual(self.ru_of(ops2, "a", 50)["reasons"], [])

    def test_fault_f_flapping_phase_since(self):
        # a=10,v=5：下线相位 [10,15)、[20,25)、[30,35)，上线相位居中。
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "F", "a": 10, "z": 100, "v": 5},
        ]
        cases = [
            (10, 10, 0), (14, 10, 4),
            (15, None, None), (19, None, None),
            (20, 20, 0), (22, 20, 2), (24, 20, 4),
            (25, None, None),
            (30, 30, 0), (31, 30, 1), (34, 30, 4),
        ]
        for now, since, duration in cases:
            result = self.ru_of(ops, "a", now)
            if since is None:
                self.assertEqual(result["reasons"], [], now)
            else:
                self.assertEqual(
                    result["reasons"],
                    [{"reason": "fault", "since": since,
                      "duration": duration}],
                    now,
                )

    def test_fault_s_never_listed(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "fs", "id": "a", "k": "S", "a": 0, "z": 100, "v": 7},
        ]
        self.assertEqual(self.ru_of(ops, "a", 50)["reasons"], [])

    def test_reasons_fixed_order_drain_health_circuit_fault(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 1},
            {"op": "cs", "id": "a", "n": 1, "m": 1, "r": 1, "w": 100,
             "q": 1},
            {"op": "cr", "id": "a", "ok": False, "now": 2},
            {"op": "ds", "id": "a", "t": 100},
            {"op": "dr", "id": "a", "now": 3},
            {"op": "fs", "id": "a", "k": "D", "a": 4, "z": 100, "v": 0},
        ]
        self.assertEqual(
            self.ru_of(ops, "a", 10)["reasons"],
            [
                {"reason": "drain", "since": 3, "duration": 7},
                {"reason": "health", "since": 1, "duration": 9},
                {"reason": "circuit", "since": 2, "duration": 8},
                {"reason": "fault", "since": 4, "duration": 6},
            ],
        )

    def test_remove_readd_clears_all_since(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 1},
            {"op": "cs", "id": "a", "n": 1, "m": 1, "r": 1, "w": 100,
             "q": 1},
            {"op": "cr", "id": "a", "ok": False, "now": 2},
            {"op": "ds", "id": "a", "t": 100},
            {"op": "dr", "id": "a", "now": 3},
            {"op": "close", "cid": "x", "now": 4},
            {"op": "remove", "id": "a"},
            {"op": "add", "id": "a", "weight": 1},
        ]
        self.assertEqual(self.ru_of(ops, "a", 50)["reasons"], [])

    def test_ci_cb_clear_all_since(self):
        base = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 0},
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 5, "v": 0},
        ]
        # ci 成功：随默认运行态重建清除 health since（fault 时间线亦不载入，
        # 因为配置无 faults）。
        ops = base + [
            {"op": "ci", "config": config_v10(1), "now": 1},
        ]
        self.assertEqual(self.ru_of(ops, "a", 2)["reasons"], [])
        # cb 成功同样清除。
        ops = base + [
            {"op": "ci", "config": config_v10(1), "now": 1},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 2},
            {"op": "cb", "rev": 1, "now": 3},
        ]
        self.assertEqual(self.ru_of(ops, "a", 4)["reasons"], [])

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            [{"op": "add", "id": "a", "weight": 1},
             {"op": "ru", "id": "b", "now": 0}],
            3, "BACKEND",
        )
        self.assert_failure([{"op": "ru", "id": "a", "now": 0}],
                            3, "BACKEND")

    def test_input_errors(self):
        base = [{"op": "add", "id": "a", "weight": 1}]
        # 键序反、多键、缺键。
        self.assert_failure_raw(
            b'{"ops":[{"now":0,"id":"a","op":"ru"}]}', 2, "INPUT"
        )
        self.assert_failure(
            base + [{"op": "ru", "id": "a", "now": 0, "x": 1}],
            2, "INPUT",
        )
        self.assert_failure(base + [{"op": "ru", "id": "a"}],
                            2, "INPUT")
        # id：空串与非 UTF-8（孤立代理）。
        self.assert_failure(base + [{"op": "ru", "id": "", "now": 0}],
                            2, "INPUT")
        self.assert_failure_raw(
            b'{"ops":[{"op":"add","id":"a","weight":1},'
            b'{"op":"ru","id":"\\ud800","now":0}]}',
            2, "INPUT",
        )
        # now：bool、负数、超 10^9、字符串、浮点、null。
        for bad in (True, False, -1, 10 ** 9 + 1, "0", 1.0, None):
            self.assert_failure(
                base + [{"op": "ru", "id": "a", "now": bad}],
                2, "INPUT",
            )

    def test_clock_regression_is_input_and_rollback(self):
        # ru 时钟倒退整批回滚：前序 add 不落任何输出。
        self.assert_failure(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "ru", "id": "a", "now": 5},
                {"op": "ru", "id": "a", "now": 4},
            ],
            2, "INPUT",
        )
        # 与其他 now 操作共用同一时钟。
        self.assert_failure(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "probe", "id": "a", "ok": True, "now": 9},
                {"op": "ru", "id": "a", "now": 8},
            ],
            2, "INPUT",
        )

    def test_equal_now_does_not_regress(self):
        result = self.ru_of(
            [
                {"op": "add", "id": "a", "weight": 1},
                {"op": "probe", "id": "a", "ok": True, "now": 5},
            ],
            "a", 5,
        )
        self.assertEqual(result["now"], 5)
        self.assertEqual(result["reasons"], [])

    def test_record_replay_byte_identical(self):
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "open", "cid": "x", "flow": FLOW, "now": 0},
            {"op": "hset", "id": "a", "fail": 1, "success": 1},
            {"op": "probe", "id": "a", "ok": False, "now": 1},
            {"op": "cs", "id": "a", "n": 1, "m": 1, "r": 1, "w": 100,
             "q": 1},
            {"op": "cr", "id": "a", "ok": False, "now": 2},
            {"op": "ds", "id": "a", "t": 100},
            {"op": "dr", "id": "a", "now": 3},
            {"op": "fs", "id": "a", "k": "F", "a": 0, "z": 100, "v": 5},
            {"op": "ru", "id": "a", "now": 12},
            {"op": "ru", "id": "a", "now": 18},
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
        # now=12 落在 F 下线相位（[10,15)），now=18 落在上线相位。
        results = json.loads(run_stdout.decode("utf-8"))["results"]
        self.assertEqual(
            results[-2]["reasons"][-1],
            {"reason": "fault", "since": 10, "duration": 2},
        )
        reasons18 = results[-1]["reasons"]
        self.assertNotIn(
            "fault", [item["reason"] for item in reasons18]
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
        config = config_v11(1, **overrides)
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
        # 旧版 version=9 输入（无 capacities）：成功即清空全部覆盖。
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

    def test_ci_clears_override_and_ce_exports_it_via_capacities(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "pc", "id": "b", "cap": 4},
            {"op": "ce"},
        ]
        _, out, _ = self.run_ops(ops)
        config = json.loads(out.decode("utf-8"))["results"][-1]["config"]
        # 覆盖经 capacities 末置导出（键序 id,cap），不含运行态名。
        self.assertNotIn("cap_overrides", config)
        self.assertEqual(
            config["capacities"], [{"id": "b", "cap": 4}]
        )
        # 旧版（无 capacities）热加载成功：覆盖清空。
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


class LatencyPercentileTest(unittest.TestCase):
    """lp 后端延迟分位查询：分桶汇总、rank 定位与各类拒绝。"""

    def lp(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
        self.assertEqual(code, 0, err)
        self.assertEqual(err, b"")
        results = json.loads(out.decode("utf-8"))["results"]
        return results, out

    def assert_failure(self, raw, exit_code, label):
        code, stdout, stderr = run_balancer("run", raw)
        self.assertEqual(code, exit_code)
        self.assertEqual(stdout, b"")
        self.assertEqual(
            stderr, ('{"error":"%s"}\n' % label).encode("utf-8")
        )

    def test_bucket_boundaries_and_percentiles(self):
        # 边界 ms：1 落桶 0、10 落桶 1、100 落桶 2、1000 落桶 3、1001 桶 4。
        ops = [{"op": "add", "id": "b", "weight": 1}]
        for ms in (0, 1, 10, 100, 1000, 1001):
            ops.append(
                {"op": "mr", "id": "b", "ok": True, "ms": ms,
                 "retries": 0, "remaps": 0, "now": 0}
            )
        for p, bucket, upper in (
            (1, 0, 1), (34, 1, 10), (51, 2, 100), (67, 3, 1000),
            (84, 4, None), (100, 4, None),
        ):
            ops.append(
                {"op": "lp", "id": "b", "from": 0, "to": 0, "p": p, "now": 0}
            )
        results, _ = self.lp(ops)
        lp_results = [r for r in results if r["op"] == "lp"]
        self.assertEqual(
            [r["buckets"] for r in lp_results],
            [[2, 1, 1, 1, 1]] * 6,
        )
        for result, bucket, upper in zip(lp_results, (0, 1, 2, 3, 4, 4),
                                         (1, 10, 100, 1000, None, None)):
            self.assertEqual(result["samples"], 6)
            self.assertEqual(result["bucket"], bucket)
            self.assertEqual(result["upper"], upper)
            self.assertEqual(
                result["rank"],
                -(-result["p"] * 6 // 100),  # ceil(p*6/100)
            )

    def test_empty_window_is_nulls(self):
        results, _ = self.lp([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "lp", "id": "b", "from": 0, "to": 0, "p": 1, "now": 0},
        ])
        self.assertEqual(
            results[-1],
            {
                "op": "lp", "id": "b", "from": 0, "to": 0, "p": 1,
                "samples": 0, "buckets": [0, 0, 0, 0, 0], "rank": 0,
                "bucket": None, "upper": None,
            },
        )

    def test_multi_window_aggregation(self):
        # w0：桶 0 两个；w1：桶 4 三个；跨窗汇总后 p=50 落桶 4。
        ops = [{"op": "add", "id": "b", "weight": 1}]
        for ms, now in ((0, 0), (1, 59), (1001, 60), (2000, 90), (99999, 119)):
            ops.append(
                {"op": "mr", "id": "b", "ok": True, "ms": ms,
                 "retries": 0, "remaps": 0, "now": now}
            )
        ops.append(
            {"op": "lp", "id": "b", "from": 0, "to": 1, "p": 50, "now": 119}
        )
        results, _ = self.lp(ops)
        self.assertEqual(
            results[-1],
            {
                "op": "lp", "id": "b", "from": 0, "to": 1, "p": 50,
                "samples": 5, "buckets": [2, 0, 0, 0, 3], "rank": 3,
                "bucket": 4, "upper": None,
            },
        )

    def test_read_only_does_not_change_metrics(self):
        # lp 前后的 mh 必须逐字一致。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mr", "id": "b", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 0},
            {"op": "mh", "id": "b", "from": 0, "to": 0, "now": 0},
            {"op": "lp", "id": "b", "from": 0, "to": 0, "p": 99, "now": 0},
            {"op": "mh", "id": "b", "from": 0, "to": 0, "now": 0},
        ]
        results, _ = self.lp(ops)
        mh_results = [r for r in results if r["op"] == "mh"]
        self.assertEqual(mh_results[0], mh_results[1])

    def test_compact_key_order_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mr", "id": "b", "ok": True, "ms": 5,
             "retries": 0, "remaps": 0, "now": 0},
            {"op": "lp", "id": "b", "from": 0, "to": 0, "p": 50, "now": 0},
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        result = json.loads(out.decode("utf-8"))["results"][-1]
        self.assertEqual(
            list(result),
            ["op", "id", "from", "to", "p", "samples", "buckets",
             "rank", "bucket", "upper"],
        )

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                {"op": "lp", "id": "z", "from": 0, "to": 0,
                 "p": 1, "now": 0},
            ]),
            3, "BACKEND",
        )

    def test_unknown_id_precedes_state(self):
        self.assert_failure(
            encode_ops([
                {"op": "lp", "id": "z", "from": 0, "to": 59,
                 "p": 1, "now": 3600},
            ]),
            3, "BACKEND",
        )

    def test_premature_from_is_state(self):
        # now=3600 时当前窗为 60、下界为 1；from=0 过早。
        self.assert_failure(
            b'{"ops":[{"op":"add","id":"b","weight":1},'
            b'{"op":"lp","id":"b","from":0,"to":59,'
            b'"p":1,"now":3600}]}',
            4, "STATE",
        )

    def test_from_boundary_accepted(self):
        results, _ = self.lp([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "lp", "id": "b", "from": 1, "to": 60, "p": 1, "now": 3600},
        ])
        self.assertEqual(results[-1]["samples"], 0)

    def test_key_order_and_shape_rejections(self):
        # 键序须为 op,id,from,to,p,now：p/now 倒置非法。
        self.assert_failure(
            b'{"ops":[{"op":"lp","id":"b","from":0,"to":0,'
            b'"now":0,"p":1}]}',
            2, "INPUT",
        )
        # 缺键 / 多键。
        self.assert_failure(
            b'{"ops":[{"op":"lp","id":"b","from":0,"to":0,"now":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"lp","id":"b","from":0,"to":0,"p":1,'
            b'"now":0,"x":1}]}',
            2, "INPUT",
        )

    def test_p_range_and_type(self):
        for bad_p in (b"0", b"101", b"true", b"1.5", b'"1"', b"-1"):
            self.assert_failure(
                b'{"ops":[{"op":"add","id":"b","weight":1},'
                b'{"op":"lp","id":"b","from":0,"to":0,"p":'
                + bad_p + b',"now":0}]}',
                2, "INPUT",
            )

    def test_window_relations(self):
        # from>to、to>now//60、to-from>=60 均为 INPUT。
        self.assert_failure(
            b'{"ops":[{"op":"add","id":"b","weight":1},'
            b'{"op":"lp","id":"b","from":1,"to":0,"p":1,"now":60}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"add","id":"b","weight":1},'
            b'{"op":"lp","id":"b","from":0,"to":1,"p":1,"now":59}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"add","id":"b","weight":1},'
            b'{"op":"lp","id":"b","from":0,"to":60,"p":1,"now":3600}]}',
            2, "INPUT",
        )

    def test_clock_regression_is_input(self):
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                {"op": "mg", "id": "b", "now": 100},
                {"op": "lp", "id": "b", "from": 1, "to": 1,
                 "p": 1, "now": 60},
            ]),
            2, "INPUT",
        )

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mr", "id": "b", "ok": True, "ms": 1,
             "retries": 0, "remaps": 0, "now": 0},
            {"op": "mr", "id": "b", "ok": False, "ms": 5000,
             "retries": 2, "remaps": 1, "now": 30},
            {"op": "lp", "id": "b", "from": 0, "to": 0, "p": 90, "now": 59},
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class ErrorRateAlertTest(unittest.TestCase):
    """ea 后端错误率告警：状态机、定点 rate、缓存/跳窗/变阈值与各类清除。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(ok, now, backend="b"):
        return {"op": "mr", "id": backend, "ok": ok, "ms": 1,
                "retries": 0, "remaps": 0, "now": now}

    @staticmethod
    def ea(w, now, hi=5000, lo=1000, n=1, backend="b"):
        return {"op": "ea", "id": backend, "w": w, "hi": hi,
                "lo": lo, "n": n, "now": now}

    def test_rate_floor_and_fixed_point(self):
        # w0：3 请求 1 错误 → v=floor(10000/3)=3333 → "33.33"；空窗 "0.00"。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.mr(True, 1), self.mr(True, 2),
            self.ea(0, 60),
            self.ea(1, 120),
        ]
        ea_results = [r for r in self.results(ops) if r["op"] == "ea"]
        self.assertEqual(ea_results[0]["requests"], 3)
        self.assertEqual(ea_results[0]["errors"], 1)
        self.assertEqual(ea_results[0]["rate"], "33.33")
        self.assertEqual(ea_results[1]["requests"], 0)
        self.assertEqual(ea_results[1]["rate"], "0.00")

    def test_hi_boundary_inclusive_triggers_alarm(self):
        # v==hi 即满足 v>=hi：1/3 → v=3333，hi=3333、n=1 当窗转 A。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.mr(True, 1), self.mr(True, 2),
            self.ea(0, 60, hi=3333, lo=0, n=1),
        ]
        result = self.results(ops)[-1]
        self.assertEqual(result["state"], "A")
        self.assertTrue(result["changed"])
        self.assertEqual(result["run"], 0)

    def test_n_run_accumulates_and_resets_on_mismatch(self):
        # n=2：N 态需连续两窗 v>=hi；方向不符窗把连续数清 0。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.ea(0, 60, n=2),          # run=1
            self.mr(True, 60), self.ea(1, 120, n=2),         # v=0 复位 0
            self.mr(False, 120), self.ea(2, 180, n=2),       # run=1
            self.mr(False, 180), self.ea(3, 240, n=2),       # run=2 转 A
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "ea"]
        self.assertEqual(seq, [
            ("N", 1, False),
            ("N", 0, False),
            ("N", 1, False),
            ("A", 0, True),
        ])

    def test_A_to_N_on_lo(self):
        # n=1：w0 全错转 A；w1 无请求 v=0<=lo 转回 N，转换后 run 清 0。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.ea(0, 60, lo=0),
            self.ea(1, 120, lo=0),
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "ea"]
        self.assertEqual(seq, [("A", 0, True), ("N", 0, True)])

    def test_same_window_threshold_cache_no_advance(self):
        # 同窗同阈值重报原样返回（含 changed），不推进状态机；时钟可继续走。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            self.ea(0, 120),
            self.ea(1, 120),
        ]
        ea_results = [r for r in self.results(ops) if r["op"] == "ea"]
        self.assertEqual(ea_results[0], ea_results[1])
        self.assertTrue(ea_results[1]["changed"])
        # w1 为首评后的下一窗，正常推进（w1 空窗 v=0，A→N）。
        self.assertEqual(ea_results[2]["w"], 1)
        self.assertEqual(ea_results[2]["state"], "N")

    def test_same_window_changed_threshold_is_state(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.ea(0, 60),
            self.ea(0, 60, hi=5001),
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_skip_and_regression_window_are_state(self):
        base = [
            {"op": "add", "id": "b", "weight": 1},
            self.ea(0, 60),
        ]
        # 跳窗 w=2。
        self.assert_failure(encode_ops(base + [self.ea(2, 180)]), 4, "STATE")
        # 已逐窗到 w1 后回退评 w0。
        self.assert_failure(
            encode_ops(base + [self.ea(1, 120), self.ea(0, 120)]),
            4, "STATE",
        )

    def test_threshold_change_next_window_is_state(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.ea(0, 60, hi=5000),
            self.ea(1, 120, hi=5001),
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")

    def test_per_backend_independent(self):
        # 两个后端独立固化阈值与状态：同窗 w0 下 b 全错转 A、c 全对停留 N，
        # 且各 id 可有不同的 (hi,lo,n) 而互不触发变阈值 STATE。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            self.mr(False, 0, "b"),
            self.mr(True, 0, "c"),
            self.ea(0, 60, hi=5000, lo=1000, n=1, backend="b"),
            self.ea(0, 60, hi=100, lo=10, n=1, backend="c"),
        ]
        ea_results = [r for r in self.results(ops) if r["op"] == "ea"]
        by_id = {r["id"]: r for r in ea_results}
        self.assertEqual(by_id["b"]["state"], "A")
        self.assertTrue(by_id["b"]["changed"])
        self.assertEqual(by_id["c"]["state"], "N")
        self.assertFalse(by_id["c"]["changed"])

    def test_readd_clears_alarm(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),                       # A
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            self.ea(1, 120, hi=9999, lo=0),      # 未首评：N、changed=false
        ]
        result = self.results(ops)[-1]
        self.assertEqual(result["state"], "N")
        self.assertFalse(result["changed"])
        self.assertEqual(result["run"], 0)

    def test_ci_clears_alarm(self):
        # ce 导出当前规范化配置，ci 成功后 ea 回到未首评。
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            {"op": "ci", "config": exported, "now": 120},
            self.ea(1, 120),
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "ea"]
        self.assertEqual(seq[0], ("A", 0, True))
        self.assertEqual(seq[1], ("N", 0, False))

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.ea(0, 60, backend="z")]), 3, "BACKEND"
        )

    def test_unknown_id_precedes_window_state(self):
        # 即使 w 窗未结束（本应 STATE），未知 id 也先判 BACKEND。
        self.assert_failure(
            encode_ops([self.ea(1, 60, backend="z")]), 3, "BACKEND"
        )

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        # hi/lo 乱序。
        self.assert_failure(
            head + b'{"op":"ea","id":"b","w":0,"hi":2,"n":1,'
                    b'"lo":1,"now":60}]}',
            2, "INPUT",
        )
        # 缺 n。
        self.assert_failure(
            head + b'{"op":"ea","id":"b","w":0,"hi":2,"lo":1,"now":60}]}',
            2, "INPUT",
        )
        # 多键。
        self.assert_failure(
            head + b'{"op":"ea","id":"b","w":0,"hi":2,"lo":1,"n":1,'
                    b'"now":60,"x":1}]}',
            2, "INPUT",
        )

    def test_type_range_and_relation_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        tail = b']}'
        cases = [
            b'{"op":"ea","id":"b","w":0,"hi":0,"lo":0,"n":1,"now":60}',     # hi 下界
            b'{"op":"ea","id":"b","w":0,"hi":10001,"lo":0,"n":1,"now":60}',# hi 上界
            b'{"op":"ea","id":"b","w":0,"hi":1,"lo":-1,"n":1,"now":60}',    # lo 下界
            b'{"op":"ea","id":"b","w":0,"hi":10000,"lo":10000,"n":1,"now":60}',  # lo 上界
            b'{"op":"ea","id":"b","w":0,"hi":1,"lo":1,"n":1,"now":60}',     # lo==hi
            b'{"op":"ea","id":"b","w":0,"hi":2,"lo":1,"n":0,"now":60}',     # n 下界
            b'{"op":"ea","id":"b","w":0,"hi":2,"lo":1,"n":61,"now":60}',    # n 上界
            b'{"op":"ea","id":"b","w":0,"hi":true,"lo":0,"n":1,"now":60}',  # bool
            b'{"op":"ea","id":"b","w":0,"hi":2.0,"lo":1,"n":1,"now":60}',   # 浮点
            b'{"op":"ea","id":"b","w":0,"hi":"2","lo":1,"n":1,"now":60}',   # 字符串
            b'{"op":"ea","id":"b","w":-1,"hi":2,"lo":1,"n":1,"now":60}',    # w 下界
            b'{"op":"ea","id":"b","w":0,"hi":2,"lo":1,"n":1,"now":-1}',     # now 下界
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_window_out_of_range_is_state(self):
        # 类型/范围合法但窗口越界：w==now//60（窗未结束）或 w 超出最近 60
        # 窗保留下界，均执行期判 STATE（先于阈值/缓存判定）。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.ea(1, 60),
            ]),
            4, "STATE",
        )
        # now=3600：当前窗 60、下界 1，w=0 越界 → STATE。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.ea(0, 3600),
            ]),
            4, "STATE",
        )

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mg", "id": "b", "now": 120},
            self.ea(0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_failed_batch_is_atomic(self):
        # 合法 ea 之后跳窗触发 STATE：整批无任何 stdout（不产生部分结果）。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.ea(0, 60),
            self.ea(2, 180),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_key_order_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        result = json.loads(out.decode("utf-8"))["results"][-1]
        self.assertEqual(
            list(result),
            ["op", "id", "w", "state", "requests", "errors",
             "rate", "run", "changed"],
        )

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.mr(True, 1),
            self.ea(0, 60),
            self.ea(0, 60),
            self.mr(True, 60),
            self.ea(1, 120),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class ErrorRateAlertHistoryTest(unittest.TestCase):
    """eh 后端错误率告警转换历史：转换记账、区间查询、优先级与各类清除。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(ok, now, backend="b"):
        return {"op": "mr", "id": backend, "ok": ok, "ms": 1,
                "retries": 0, "remaps": 0, "now": now}

    @staticmethod
    def ea(w, now, hi=5000, lo=1000, n=1, backend="b"):
        return {"op": "ea", "id": backend, "w": w, "hi": hi,
                "lo": lo, "n": n, "now": now}

    @staticmethod
    def eh(start, end, now, backend="b"):
        return {"op": "eh", "id": backend, "from": start,
                "to": end, "now": now}

    def test_transition_recorded_once(self):
        # w0 全错转 A 记录一次；同窗重报不重复；w1 空窗 v=0 转回 N 再记一次。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            self.ea(0, 120),
            self.ea(1, 120),
            self.eh(0, 1, 120),
        ]
        events = self.results(ops)[-1]["events"]
        self.assertEqual(
            events,
            [
                {"window": 0, "from": "N", "to": "A", "rate": "100.00",
                 "hi": 5000, "lo": 1000, "n": 1},
                {"window": 1, "from": "A", "to": "N", "rate": "0.00",
                 "hi": 5000, "lo": 1000, "n": 1},
            ],
        )
        for event in events:
            self.assertEqual(
                list(event),
                ["window", "from", "to", "rate", "hi", "lo", "n"],
            )

    def test_rate_uses_ea_fixed_point_string(self):
        # 1/3 → 33.33；事件沿用该次 ea 的两位定点串。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.mr(True, 1), self.mr(True, 2),
            self.ea(0, 60, hi=3333, lo=0),
            self.eh(0, 0, 60),
        ]
        event = self.results(ops)[-1]["events"][0]
        self.assertEqual(event["rate"], "33.33")
        self.assertEqual((event["from"], event["to"]), ("N", "A"))

    def test_no_transition_records_nothing(self):
        # 首评停留在 N、方向不符与 n 未达均不记录。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(True, 0),
            self.ea(0, 60, n=2),                       # N，run=1
            self.mr(True, 60),
            self.ea(1, 120, n=2),                     # 复位 0
            self.eh(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_n_run_event_lands_on_trigger_window(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.ea(0, 60, n=3),
            self.mr(False, 60), self.ea(1, 120, n=3),
            self.mr(False, 120), self.ea(2, 180, n=3),
            self.eh(0, 2, 180),
        ]
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, [2])

    def test_closed_range_filter_and_window_order(self):
        # N→A 与 A→N 交替发生于 w0..w3；区间闭、按 window 升序。
        ops = [{"op": "add", "id": "b", "weight": 1}]
        for w in range(4):
            if w % 2 == 0:
                ops.append(self.mr(False, w * 60 + 1))
            ops.append(self.ea(w, w * 60 + 60, hi=1, lo=0))
        ops.append(self.eh(1, 2, 240))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, [1, 2])

    def test_empty_range_and_never_evaluated(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.eh(0, 0, 60),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_history_trimmed_to_last_60_windows(self):
        # 每窗交替转换；ea w=60 成功后删除 window<1 的事件，w0 不再可见。
        ops = [{"op": "add", "id": "b", "weight": 1}]
        for w in range(61):
            if w % 2 == 0:
                ops.append(self.mr(False, w * 60 + 1))
            ops.append(self.ea(w, w * 60 + 60, hi=1, lo=0))
        ops.append(self.eh(2, 60, 3660))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, list(range(2, 61)))

    def test_eh_is_read_only(self):
        # eh 不推进告警状态机：随后同窗 ea 仍命中首评缓存（不二次记账），
        # 且历史不被查询清理；结果事件为逐项拷贝，不被后续评估改写。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60, hi=1, lo=0),
            self.eh(0, 1, 60),
            self.mr(False, 60),
            self.ea(1, 120, hi=1, lo=0),
            self.ea(2, 180, hi=1, lo=0),
        ]
        results = self.results(ops)
        self.assertEqual(
            results[3]["events"],
            [{"window": 0, "from": "N", "to": "A", "rate": "100.00",
              "hi": 1, "lo": 0, "n": 1}],
        )
        # 同窗 ea（同 now）在 eh 之后仍原样返回首评结果。
        ops2 = ops[:4] + [self.ea(0, 60, hi=1, lo=0)]
        cached = [r for r in self.results(ops2) if r["op"] == "ea"][-1]
        self.assertTrue(cached["changed"])

    def test_per_backend_isolation(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            self.mr(False, 0, "b"),
            self.mr(True, 0, "c"),
            self.ea(0, 60, backend="b"),
            self.ea(0, 60, hi=100, lo=10, n=1, backend="c"),
            self.eh(0, 0, 60, "b"),
            self.eh(0, 0, 60, "c"),
        ]
        results = self.results(ops)
        self.assertEqual(len(results[-2]["events"]), 1)
        self.assertEqual(results[-1]["events"], [])

    def test_remove_and_readd_clears_history(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            self.eh(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_ci_clears_history(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            {"op": "ci", "config": exported, "now": 120},
            self.eh(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_cb_clears_history(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ci", "config": exported, "now": 60},
            self.mr(False, 61),
            self.ea(1, 120, hi=1, lo=0),
            {"op": "cb", "rev": 1, "now": 180},
            self.eh(0, 1, 180),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.eh(0, 0, 60, "z")]), 3, "BACKEND"
        )

    def test_unknown_id_precedes_state(self):
        # from 已低于最近 60 窗下界：未知 id 仍先判 BACKEND。
        self.assert_failure(
            encode_ops([self.eh(0, 0, 3600, "z")]), 3, "BACKEND"
        )

    def test_from_too_early_is_state(self):
        # now=3600：当前窗 60、下界 1，from=0 → STATE。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.eh(0, 0, 3600),
            ]),
            4, "STATE",
        )

    def test_lower_bound_boundary_allowed(self):
        # now=3599 → current=59、下界 0，from=0 合法。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.eh(0, 59, 3599),
        ])
        self.assertEqual(results[-1]["events"], [])

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mg", "id": "b", "now": 120},
            self.eh(0, 0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_clock_equal_allowed(self):
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mg", "id": "b", "now": 120},
            self.eh(0, 1, 120),
        ])
        self.assertEqual(results[-1]["op"], "eh")

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            # from/to 乱序。
            b'{"op":"eh","id":"b","to":0,"from":0,"now":60}',
            b'{"op":"eh","id":"b","from":0,"now":60,"to":0}',
            # 缺键。
            b'{"op":"eh","id":"b","from":0,"to":0}',
            # 多键。
            b'{"op":"eh","id":"b","from":0,"to":0,"now":60,"x":1}',
            # 非对象、id 形状。
            b'{"op":"eh","id":1,"from":0,"to":0,"now":60}',
            b'{"op":"eh","id":"","from":0,"to":0,"now":60}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_and_relation_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            b'{"op":"eh","id":"b","from":-1,"to":0,"now":60}',
            b'{"op":"eh","id":"b","from":0,"to":-1,"now":60}',
            b'{"op":"eh","id":"b","from":0,"to":0,"now":-1}',
            b'{"op":"eh","id":"b","from":0,"to":1000000001,"now":1000000001}',
            b'{"op":"eh","id":"b","from":0,"to":0,"now":true}',
            b'{"op":"eh","id":"b","from":0,"to":0.5,"now":60}',
            b'{"op":"eh","id":"b","from":"0","to":0,"now":60}',
            # from>to。
            b'{"op":"eh","id":"b","from":1,"to":0,"now":60}',
            # to>now//60。
            b'{"op":"eh","id":"b","from":0,"to":1,"now":59}',
            # to-from>=60。
            b'{"op":"eh","id":"b","from":0,"to":60,"now":3600}',
            b'{"op":"eh","id":"b","from":0,"to":61,"now":3660}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_input_precedes_backend(self):
        # 关系非法即使 id 未知也先判 INPUT。
        self.assert_failure(
            b'{"ops":[{"op":"eh","id":"z","from":1,"to":0,"now":60}]}',
            2, "INPUT",
        )

    def test_failed_batch_is_atomic(self):
        # 合法 ea 产生事件后，跳窗 ea 触发 STATE：整批无 stdout，历史不落盘。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            self.ea(2, 180),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')
        # 新批次（全新状态）确认失败未持久化任何东西（本就进程内状态）。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.eh(0, 1, 120),
        ])
        self.assertEqual(results[-1]["events"], [])

    def test_failed_eh_rolls_back_clock(self):
        # eh 因 from 过早 STATE 失败：其后的旧时刻操作不被视为时钟倒退豁免，
        # 失败批整体无输出（时钟推进随批次回滚）。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.eh(0, 0, 3600),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_key_order_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0),
            self.ea(0, 60),
            self.eh(0, 0, 60),
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        result = json.loads(out.decode("utf-8"))["results"][-1]
        self.assertEqual(list(result), ["op", "id", "events"])

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(False, 0), self.mr(True, 1),
            self.ea(0, 60),
            self.ea(0, 60),
            self.mr(True, 60),
            self.ea(1, 120),
            self.eh(0, 1, 120),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class PercentileAlertTest(unittest.TestCase):
    """pa 后端延迟分位告警：lp 窗值、N/A 滞回状态机、缓存与各类拒绝。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(ms, now, backend="b", ok=True):
        return {"op": "mr", "id": backend, "ok": ok, "ms": ms,
                "retries": 0, "remaps": 0, "now": now}

    @staticmethod
    def pa(w, now, p=100, hi=2, lo=1, n=1, backend="b"):
        return {"op": "pa", "id": backend, "w": w, "p": p,
                "hi": hi, "lo": lo, "n": n, "now": now}

    def test_window_uses_lp_buckets_and_uppers(self):
        # 各 ms 落桶：1→0(≤1)、10→1(≤10)、100→2(≤100)、1000→3、1001→4。
        # p=100 时 rank=samples，取最末非空桶；upper 依次 1/10/100/1000/null。
        cases = [
            (1, 0, 1), (10, 1, 10), (100, 2, 100),
            (1000, 3, 1000), (1001, 4, None),
        ]
        # 同窗多次 pa 仅首评推进，故用独立批次逐例验证。
        seen = []
        for ms, bucket, upper in cases:
            out = self.results([
                {"op": "add", "id": "b", "weight": 1},
                self.mr(ms, 0),
                self.pa(0, 60, p=100, hi=4, lo=0),
            ])[-1]
            seen.append((out["samples"], out["bucket"], out["upper"]))
        self.assertEqual(seen, [(1, b, u) for _, b, u in cases])

    def test_percentile_picks_rank_bucket(self):
        # w0：两个桶0样本（ms=1）与一个桶2样本（ms=100）。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1, 0), self.mr(1, 1), self.mr(100, 2),
        ]
        # p=1：rank=ceil(3/100)=1 → 桶0；p=67：rank=ceil(201/100)=3 → 桶2；
        # p=100：rank=3 → 桶2。
        out = []
        for p in (1, 67, 100):
            out.append(self.results(ops + [self.pa(0, 60, p=p, hi=4, lo=0)]
                                    )[-1]["bucket"])
        self.assertEqual(out, [0, 2, 2])

    def test_empty_window_is_zero_null_null(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.pa(0, 60),
        ])[-1]
        self.assertEqual(
            (out["samples"], out["bucket"], out["upper"]),
            (0, None, None),
        )
        self.assertEqual(out["state"], "N")
        self.assertEqual(out["run"], 0)
        self.assertFalse(out["changed"])

    def test_empty_window_clears_run_in_N(self):
        # hi=4 仅桶4满足；n=2：w0 桶4 run=1，w1 空窗清 0，w2/w3 连续桶4
        # 才转 A。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.pa(0, 60, hi=4, lo=0, n=2),      # run=1
            self.pa(1, 120, hi=4, lo=0, n=2),                       # 空窗 0
            self.mr(1001, 120), self.pa(2, 180, hi=4, lo=0, n=2),   # run=1
            self.mr(1001, 180), self.pa(3, 240, hi=4, lo=0, n=2),   # run=2→A
        ]
        seq = [(r["state"], r["run"], r["changed"], r["bucket"])
               for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(seq, [
            ("N", 1, False, 4),
            ("N", 0, False, None),
            ("N", 1, False, 4),
            ("A", 0, True, 4),
        ])

    def test_empty_window_clears_run_in_A(self):
        # n=1：w0 桶4 转 A；w1 空窗 bucket=null 不满足 <=lo，run 清 0 且
        # 停留 A；w2 桶1<=lo 才转回 N。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.pa(0, 60, hi=2, lo=1),       # A
            self.pa(1, 120, hi=2, lo=1),                        # 空窗留 A
            self.mr(10, 120), self.pa(2, 180, hi=2, lo=1),      # 桶1→N
        ]
        seq = [(r["state"], r["run"], r["changed"], r["bucket"])
               for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(seq, [
            ("A", 0, True, 4),
            ("A", 0, False, None),
            ("N", 0, True, 1),
        ])

    def test_hi_boundary_inclusive_triggers_alarm(self):
        # bucket==hi 即满足 bucket>=hi：桶2、hi=2、n=1 当窗转 A。
        result = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(100, 0),
            self.pa(0, 60, hi=2, lo=1),
        ])[-1]
        self.assertEqual(result["state"], "A")
        self.assertTrue(result["changed"])
        self.assertEqual(result["run"], 0)

    def test_n_run_accumulates_and_resets_on_mismatch(self):
        # n=2：N 态需连续两窗 bucket>=hi；方向不符窗（含空窗）清连续数。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.pa(0, 60, hi=4, lo=0, n=2),    # run=1
            self.mr(1, 60), self.pa(1, 120, hi=4, lo=0, n=2),     # 桶0 复位
            self.mr(1001, 120), self.pa(2, 180, hi=4, lo=0, n=2),  # run=1
            self.mr(1001, 180), self.pa(3, 240, hi=4, lo=0, n=2),  # run=2→A
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(seq, [
            ("N", 1, False),
            ("N", 0, False),
            ("N", 1, False),
            ("A", 0, True),
        ])

    def test_A_to_N_on_lo(self):
        # n=1：w0 桶4 转 A；w1 桶1<=lo=1 转回 N，转换后 run 清 0。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.pa(0, 60, hi=2, lo=1),
            self.mr(10, 60), self.pa(1, 120, hi=2, lo=1),
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(seq, [("A", 0, True), ("N", 0, True)])

    def test_lo_boundary_inclusive(self):
        # A 态 bucket==lo 即满足 <=lo：桶1、lo=1 当窗转 N。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.pa(0, 60, hi=2, lo=1),
            self.mr(10, 60), self.pa(1, 120, hi=2, lo=1),
        ]
        self.assertEqual(
            [r["state"] for r in self.results(ops) if r["op"] == "pa"],
            ["A", "N"],
        )

    def test_same_window_params_cache_no_advance(self):
        # 同窗同参重报原样返回（含 changed），不推进状态机；时钟可继续走。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, hi=2, lo=1),
            self.pa(0, 120, hi=2, lo=1),
            self.pa(1, 120, hi=2, lo=1),
        ]
        pa_results = [r for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(pa_results[0], pa_results[1])
        self.assertTrue(pa_results[1]["changed"])
        # w1 为首评后的下一窗，空窗 bucket=null：A 态方向不符，留 A run=0。
        self.assertEqual(pa_results[2]["w"], 1)
        self.assertEqual(pa_results[2]["state"], "A")
        self.assertIsNone(pa_results[2]["bucket"])

    def test_same_window_changed_param_is_state(self):
        base = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, p=100, hi=2, lo=1, n=1),
        ]
        # 同窗改 p/hi/lo/n 任一皆 STATE。
        for second in (
            self.pa(0, 60, p=99, hi=2, lo=1, n=1),
            self.pa(0, 60, p=100, hi=3, lo=1, n=1),
            self.pa(0, 60, p=100, hi=2, lo=0, n=1),
            self.pa(0, 60, p=100, hi=2, lo=1, n=2),
        ):
            self.assert_failure(encode_ops(base + [second]), 4, "STATE")

    def test_param_change_next_window_is_state(self):
        base = [
            {"op": "add", "id": "b", "weight": 1},
            self.pa(0, 60, p=100, hi=2, lo=1, n=1),
        ]
        for second in (
            self.pa(1, 120, p=99, hi=2, lo=1, n=1),
            self.pa(1, 120, p=100, hi=3, lo=1, n=1),
            self.pa(1, 120, p=100, hi=2, lo=0, n=1),
            self.pa(1, 120, p=100, hi=2, lo=1, n=2),
        ):
            self.assert_failure(encode_ops(base + [second]), 4, "STATE")

    def test_skip_and_regression_window_are_state(self):
        base = [
            {"op": "add", "id": "b", "weight": 1},
            self.pa(0, 60),
        ]
        # 跳窗 w=2。
        self.assert_failure(
            encode_ops(base + [self.pa(2, 180)]), 4, "STATE"
        )
        # 已逐窗到 w1 后回退评 w0。
        self.assert_failure(
            encode_ops(base + [self.pa(1, 120), self.pa(0, 120)]),
            4, "STATE",
        )

    def test_per_backend_independent(self):
        # b/c 独立固化参数与状态：同窗 w0 下 b 桶4转 A、c 桶0停留 N。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            self.mr(1001, 0, "b"),
            self.mr(1, 0, "c"),
            self.pa(0, 60, p=100, hi=2, lo=1, n=1, backend="b"),
            self.pa(0, 60, p=50, hi=4, lo=0, n=2, backend="c"),
        ]
        by_id = {
            r["id"]: r
            for r in self.results(ops) if r["op"] == "pa"
        }
        self.assertEqual(by_id["b"]["state"], "A")
        self.assertTrue(by_id["b"]["changed"])
        self.assertEqual(by_id["c"]["state"], "N")
        self.assertFalse(by_id["c"]["changed"])

    def test_readd_clears_alarm(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, hi=2, lo=1),               # A
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            # 新实例首评参数可任意，状态自 N 起、changed=false。
            self.pa(1, 120, p=80, hi=4, lo=0, n=2),
        ]
        result = self.results(ops)[-1]
        self.assertEqual(result["state"], "N")
        self.assertFalse(result["changed"])
        self.assertEqual(result["run"], 0)

    def test_ci_clears_alarm(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, hi=2, lo=1),                       # A
            {"op": "ci", "config": exported, "now": 120},
            self.pa(1, 120, hi=2, lo=1),                      # 未首评：N
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(seq[0], ("A", 0, True))
        self.assertEqual(seq[1], ("N", 0, False))

    def test_cb_clears_alarm(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, hi=2, lo=1),                       # A（rev 前）
            # ci 成功产生 rev 1 并清告警。
            {"op": "ci", "config": exported, "now": 120},
            self.pa(1, 120, hi=2, lo=1),                      # 未首评：N
            # cb 回滚到 rev 1 的快照，再次清告警。
            {"op": "cb", "rev": 1, "now": 180},
            self.pa(2, 180, hi=2, lo=1),                      # 未首评：N
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.results(ops) if r["op"] == "pa"]
        self.assertEqual(seq, [
            ("A", 0, True),
            ("N", 0, False),
            ("N", 0, False),
        ])

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.pa(0, 60, backend="z")]), 3, "BACKEND"
        )

    def test_unknown_id_precedes_window_state(self):
        # 即使 w 窗未结束（本应 STATE），未知 id 也先判 BACKEND。
        self.assert_failure(
            encode_ops([self.pa(1, 60, backend="z")]), 3, "BACKEND"
        )

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        # p 与 w 乱序。
        self.assert_failure(
            head + b'{"op":"pa","id":"b","p":100,"w":0,"hi":2,'
                    b'"lo":1,"n":1,"now":60}]}',
            2, "INPUT",
        )
        # hi/lo 乱序。
        self.assert_failure(
            head + b'{"op":"pa","id":"b","w":0,"p":100,"lo":1,'
                    b'"hi":2,"n":1,"now":60}]}',
            2, "INPUT",
        )
        # 缺 n。
        self.assert_failure(
            head + b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,'
                    b'"lo":1,"now":60}]}',
            2, "INPUT",
        )
        # 多键。
        self.assert_failure(
            head + b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,'
                    b'"lo":1,"n":1,"now":60,"x":1}]}',
            2, "INPUT",
        )

    def test_type_range_and_relation_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        tail = b"]}"
        cases = [
            b'{"op":"pa","id":"b","w":0,"p":0,"hi":2,"lo":1,"n":1,"now":60}',    # p 下界
            b'{"op":"pa","id":"b","w":0,"p":101,"hi":2,"lo":1,"n":1,"now":60}',  # p 上界
            b'{"op":"pa","id":"b","w":0,"p":true,"hi":2,"lo":1,"n":1,"now":60}', # p bool
            b'{"op":"pa","id":"b","w":0,"p":50.0,"hi":2,"lo":1,"n":1,"now":60}', # p 浮点
            b'{"op":"pa","id":"b","w":0,"p":"50","hi":2,"lo":1,"n":1,"now":60}', # p 字符串
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":-1,"lo":0,"n":1,"now":60}', # hi 下界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":5,"lo":0,"n":1,"now":60}',  # hi 上界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":-1,"n":1,"now":60}', # lo 下界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":5,"n":1,"now":60}',  # lo 上界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":2,"n":1,"now":60}',  # lo==hi
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":0,"lo":0,"n":1,"now":60}',  # hi=0 且 lo==hi
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":1,"n":0,"now":60}',  # n 下界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":1,"n":61,"now":60}', # n 上界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":true,"lo":1,"n":1,"now":60}',  # hi bool
            b'{"op":"pa","id":"b","w":-1,"p":100,"hi":2,"lo":1,"n":1,"now":60}', # w 下界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":1,"n":1,"now":-1}',  # now 下界
            b'{"op":"pa","id":"b","w":0,"p":100,"hi":2,"lo":1,"n":1,"now":1000000001}',  # now 上界
            b'{"op":"pa","id":123,"w":0,"p":100,"hi":2,"lo":1,"n":1,"now":60}',  # id 非字符串
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_window_out_of_range_is_state(self):
        # w==now//60（窗未结束）报 STATE。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.pa(1, 60),
            ]),
            4, "STATE",
        )
        # w 为未来窗（now=120 当前窗 2，w=2 未结束）。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.pa(2, 120),
            ]),
            4, "STATE",
        )
        # now=3600：当前窗 60、下界 1，w=0 过旧 → STATE。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.pa(0, 3600),
            ]),
            4, "STATE",
        )

    def test_window_lower_bound_is_inclusive(self):
        # current=60 时下界为 1，w=1 合法；current=59 时下界为 0，w=0 合法。
        for now, w in ((3600, 1), (3540, 0)):
            out = self.results([
                {"op": "add", "id": "b", "weight": 1},
                self.pa(w, now),
            ])[-1]
            self.assertEqual(out["w"], w)

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mg", "id": "b", "now": 120},
            self.pa(0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_failed_batch_is_atomic(self):
        # 合法 pa 之后跳窗触发 STATE：整批无任何 stdout。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.pa(0, 60),
            self.pa(2, 180),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_key_order_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, hi=2, lo=1),
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        result = json.loads(out.decode("utf-8"))["results"][-1]
        self.assertEqual(
            list(result),
            ["op", "id", "w", "p", "state", "samples", "bucket",
             "upper", "run", "changed"],
        )

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.mr(1001, 1),
            self.pa(0, 60, hi=2, lo=1, n=2),
            self.pa(0, 60, hi=2, lo=1, n=2),
            self.pa(1, 120, hi=2, lo=1, n=2),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class PercentileAlertHistoryTest(unittest.TestCase):
    """ph 后端延迟分位告警转换历史：pa 转换记账、闭区间查询、优先级与清除。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(ms, now, backend="b"):
        return {"op": "mr", "id": backend, "ok": True, "ms": ms,
                "retries": 0, "remaps": 0, "now": now}

    @staticmethod
    def pa(w, now, p=100, hi=2, lo=1, n=1, backend="b"):
        return {"op": "pa", "id": backend, "w": w, "p": p,
                "hi": hi, "lo": lo, "n": n, "now": now}

    @staticmethod
    def ph(start, end, now, backend="b"):
        return {"op": "ph", "id": backend, "from": start,
                "to": end, "now": now}

    def test_transition_recorded_once(self):
        # w0 桶4（ms=1001）转 A 记录一次；同窗重报不重复；w1 桶1 转回 N。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60),
            self.mr(10, 61),
            self.pa(0, 120),
            self.pa(1, 120),
            self.ph(0, 1, 120),
        ]
        events = self.results(ops)[-1]["events"]
        self.assertEqual(
            events,
            [
                {"window": 0, "from": "N", "to": "A", "p": 100,
                 "samples": 1, "bucket": 4, "upper": None,
                 "hi": 2, "lo": 1, "n": 1},
                {"window": 1, "from": "A", "to": "N", "p": 100,
                 "samples": 1, "bucket": 1, "upper": 10,
                 "hi": 2, "lo": 1, "n": 1},
            ],
        )
        for event in events:
            self.assertEqual(
                list(event),
                ["window", "from", "to", "p", "samples", "bucket",
                 "upper", "hi", "lo", "n"],
            )

    def test_event_values_taken_from_triggering_evaluation(self):
        # 桶0一个、桶2一个：p=67 rank=2 取桶2；upper 沿用该次 pa 的 100。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1, 0), self.mr(100, 1),
            self.pa(0, 60, p=67, hi=2, lo=0),
            self.ph(0, 0, 60),
        ]
        event = self.results(ops)[-1]["events"][0]
        self.assertEqual(
            event,
            {"window": 0, "from": "N", "to": "A", "p": 67,
             "samples": 2, "bucket": 2, "upper": 100,
             "hi": 2, "lo": 0, "n": 1},
        )

    def test_no_transition_records_nothing(self):
        # 首评停留 N、方向不符与 n 未达均不记录（含无样本窗）。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60, hi=4, lo=0, n=2),          # N，run=1
            self.pa(1, 120, hi=4, lo=0, n=2),         # 空窗清 0
            self.ph(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_n_run_event_lands_on_trigger_window(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.pa(0, 60, hi=4, lo=0, n=3),
            self.mr(1001, 61), self.pa(1, 120, hi=4, lo=0, n=3),
            self.mr(1001, 121), self.pa(2, 180, hi=4, lo=0, n=3),
            self.ph(0, 2, 180),
        ]
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, [2])

    def test_closed_range_filter_and_window_order(self):
        # N→A 与 A→N 交替发生于 w0..w3；区间闭、按 window 升序。
        ops = [{"op": "add", "id": "b", "weight": 1}]
        for w in range(4):
            ops.append(self.mr(1001 if w % 2 == 0 else 10, w * 60 + 1))
            ops.append(self.pa(w, w * 60 + 60))
        ops.append(self.ph(1, 2, 240))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, [1, 2])

    def test_empty_range_and_never_evaluated(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.ph(0, 0, 60),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_history_trimmed_to_last_60_windows(self):
        # 每窗交替转换；pa w=60 成功后删除 window<1 的事件，w0 不再可见。
        ops = [{"op": "add", "id": "b", "weight": 1}]
        for w in range(61):
            ops.append(self.mr(1001 if w % 2 == 0 else 10, w * 60 + 1))
            ops.append(self.pa(w, w * 60 + 60))
        ops.append(self.ph(2, 60, 3660))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, list(range(2, 61)))

    def test_ph_is_read_only(self):
        # ph 不推进告警状态机：随后同窗 pa 仍命中首评缓存，历史不被查询
        # 清理；结果事件为逐项拷贝，不被后续评估改写。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60),
            self.ph(0, 1, 60),
            self.pa(0, 60),
            self.mr(10, 61),
            self.pa(1, 120),
            self.pa(2, 180),
        ]
        results = self.results(ops)
        self.assertEqual(
            results[3]["events"],
            [{"window": 0, "from": "N", "to": "A", "p": 100,
              "samples": 1, "bucket": 4, "upper": None,
              "hi": 2, "lo": 1, "n": 1}],
        )
        # 同窗 pa 在 ph 之后仍原样返回首评结果（changed 仍为 true）。
        self.assertTrue(results[4]["changed"])
        self.assertEqual(results[4]["w"], 0)

    def test_per_backend_isolation(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            self.mr(1001, 0, "b"),
            self.mr(1, 0, "c"),
            self.pa(0, 60, backend="b"),
            self.pa(0, 60, hi=4, lo=0, n=2, backend="c"),
            self.ph(0, 0, 60, "b"),
            self.ph(0, 0, 60, "c"),
        ]
        results = self.results(ops)
        self.assertEqual(len(results[-2]["events"]), 1)
        self.assertEqual(results[-1]["events"], [])

    def test_remove_and_readd_clears_history(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60),
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            self.ph(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_ci_clears_history(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60),
            {"op": "ci", "config": exported, "now": 120},
            self.ph(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_cb_clears_history(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ci", "config": exported, "now": 60},
            self.mr(1001, 61),
            self.pa(1, 120),
            {"op": "cb", "rev": 1, "now": 180},
            self.ph(0, 1, 180),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.ph(0, 0, 60, "z")]), 3, "BACKEND"
        )

    def test_unknown_id_precedes_state(self):
        # from 已低于最近 60 窗下界：未知 id 仍先判 BACKEND。
        self.assert_failure(
            encode_ops([self.ph(0, 0, 3600, "z")]), 3, "BACKEND"
        )

    def test_from_too_early_is_state(self):
        # now=3600：当前窗 60、下界 1，from=0 → STATE。
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.ph(0, 0, 3600),
            ]),
            4, "STATE",
        )

    def test_lower_bound_boundary_allowed(self):
        # now=3599 → current=59、下界 0，from=0 合法。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.ph(0, 59, 3599),
        ])
        self.assertEqual(results[-1]["events"], [])

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mg", "id": "b", "now": 120},
            self.ph(0, 0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_clock_equal_allowed(self):
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "mg", "id": "b", "now": 120},
            self.ph(0, 1, 120),
        ])
        self.assertEqual(results[-1]["op"], "ph")

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            # from/to 乱序。
            b'{"op":"ph","id":"b","to":0,"from":0,"now":60}',
            b'{"op":"ph","id":"b","from":0,"now":60,"to":0}',
            # 缺键。
            b'{"op":"ph","id":"b","from":0,"to":0}',
            # 多键。
            b'{"op":"ph","id":"b","from":0,"to":0,"now":60,"x":1}',
            # 非对象、id 形状。
            b'{"op":"ph","id":1,"from":0,"to":0,"now":60}',
            b'{"op":"ph","id":"","from":0,"to":0,"now":60}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_and_relation_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            b'{"op":"ph","id":"b","from":-1,"to":0,"now":60}',
            b'{"op":"ph","id":"b","from":0,"to":-1,"now":60}',
            b'{"op":"ph","id":"b","from":0,"to":0,"now":-1}',
            b'{"op":"ph","id":"b","from":0,"to":1000000001,"now":1000000001}',
            b'{"op":"ph","id":"b","from":0,"to":0,"now":true}',
            b'{"op":"ph","id":"b","from":0,"to":0.5,"now":60}',
            b'{"op":"ph","id":"b","from":"0","to":0,"now":60}',
            # from>to。
            b'{"op":"ph","id":"b","from":1,"to":0,"now":60}',
            # to>now//60。
            b'{"op":"ph","id":"b","from":0,"to":1,"now":59}',
            # to-from>=60。
            b'{"op":"ph","id":"b","from":0,"to":60,"now":3600}',
            b'{"op":"ph","id":"b","from":0,"to":61,"now":3660}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_input_precedes_backend(self):
        # 关系非法即使 id 未知也先判 INPUT。
        self.assert_failure(
            b'{"ops":[{"op":"ph","id":"z","from":1,"to":0,"now":60}]}',
            2, "INPUT",
        )

    def test_failed_batch_is_atomic(self):
        # 合法 pa 产生事件后，跳窗 pa 触发 STATE：整批无 stdout，历史不落盘。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60),
            self.pa(2, 180),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')
        # 新批次（全新状态）确认失败未持久化任何东西（本就进程内状态）。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.ph(0, 1, 120),
        ])
        self.assertEqual(results[-1]["events"], [])

    def test_failed_ph_rolls_back_clock(self):
        # ph 因 from 过早 STATE 失败：失败批整体无输出（时钟推进随批回滚）。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.ph(0, 0, 3600),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_key_order_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0),
            self.pa(0, 60),
            self.ph(0, 0, 60),
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        result = json.loads(out.decode("utf-8"))["results"][-1]
        self.assertEqual(list(result), ["op", "id", "events"])

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(1001, 0), self.mr(10, 1),
            self.pa(0, 60),
            self.pa(0, 60),
            self.mr(10, 61),
            self.pa(1, 120),
            self.ph(0, 1, 120),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class RetryRemapAlertTest(unittest.TestCase):
    """xa 每后端重试/重映射告警：mh 窗值、R/M 独立、N/A 滞回、缓存与拒绝。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(retries=0, remaps=0, now=0, backend="b", ok=True):
        return {"op": "mr", "id": backend, "ok": ok, "ms": 1,
                "retries": retries, "remaps": remaps, "now": now}

    @staticmethod
    def xa(w, now, k="R", hi=5, lo=1, n=1, backend="b"):
        return {"op": "xa", "id": backend, "k": k, "w": w,
                "hi": hi, "lo": lo, "n": n, "now": now}

    def xa_results(self, ops):
        return [r for r in self.results(ops) if r["op"] == "xa"]

    def test_result_key_order_and_value_source(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
        ])[-1]
        self.assertEqual(
            list(out),
            ["op", "id", "k", "w", "state", "value", "run", "changed"],
        )
        self.assertEqual(out["state"], "A")
        self.assertEqual(out["value"], 5)
        self.assertEqual(out["run"], 0)
        self.assertIs(out["changed"], True)
        # M 取同窗 remaps 累计。
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="M", hi=3, lo=1),
        ])[-1]
        self.assertEqual(out["value"], 2)
        self.assertEqual(out["state"], "N")
        self.assertFalse(out["changed"])

    def test_value_sums_window_reports(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=3, remaps=4, now=0),
            self.mr(retries=2, remaps=1, now=1),
            self.xa(0, 60, k="R"),
            self.xa(0, 61, k="M"),
        ])
        rs = [r for r in out if r["op"] == "xa"]
        self.assertEqual((rs[0]["value"], rs[1]["value"]), (5, 5))

    def test_missing_window_value_zero_starts_N(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60, hi=1, lo=0),
        ])[-1]
        self.assertEqual(
            (out["state"], out["value"], out["run"], out["changed"]),
            ("N", 0, 0, False),
        )

    def test_r_and_m_are_independent(self):
        seq = self.xa_results([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, remaps=5, now=0),
            self.xa(0, 60, k="R", hi=5, lo=1),
            self.xa(0, 61, k="M", hi=6, lo=1),
        ])
        self.assertEqual([r["state"] for r in seq], ["A", "N"])

    def test_per_backend_independent(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            self.mr(retries=5, now=0, backend="b"),
            self.xa(0, 60, backend="b"),
            self.xa(0, 61, backend="c"),
        ]
        seq = self.xa_results(ops)
        self.assertEqual([r["state"] for r in seq], ["A", "N"])
        self.assertEqual(seq[1]["value"], 0)

    def test_n_accumulates_then_transitions_and_returns(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, now=0),
            self.xa(0, 60, n=2),                          # run=1
            self.mr(retries=6, now=60),
            self.xa(1, 120, n=2),                         # N→A
            # w2 缺窗 value=0<=lo：A 态 run=1。
            self.xa(2, 180, n=2),
            # w3 仍缺窗：连续 2 窗 value<=lo → A→N。
            self.xa(3, 240, n=2),
        ]
        seq = [(r["state"], r["run"], r["changed"], r["value"])
               for r in self.xa_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False, 5),
            ("A", 0, True, 6),
            ("A", 1, False, 0),
            ("N", 0, True, 0),
        ])

    def test_direction_mismatch_clears_run_in_N(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, now=0),
            self.xa(0, 60, n=2),                          # run=1
            self.mr(retries=1, now=60),
            self.xa(1, 120, n=2),                         # <hi 清 0
            self.mr(retries=5, now=120),
            self.xa(2, 180, n=2),                         # run=1
            self.mr(retries=5, now=180),
            self.xa(3, 240, n=2),                         # run=2→A
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.xa_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False), ("N", 0, False),
            ("N", 1, False), ("A", 0, True),
        ])

    def test_direction_mismatch_clears_run_in_A(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, now=0),
            self.xa(0, 60),                               # A
            self.mr(retries=4, now=60),
            self.xa(1, 120),                              # 4>lo 清 0 留 A
            self.mr(retries=1, now=120),
            self.xa(2, 180),                              # 1<=lo → N
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.xa_results(ops)]
        self.assertEqual(seq, [
            ("A", 0, True), ("A", 0, False), ("N", 0, True),
        ])

    def test_hi_lo_boundaries_inclusive(self):
        # value==hi 满足 >=hi；value==lo 满足 <=lo。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, now=0),
            self.xa(0, 60, hi=5, lo=3),                  # 5==hi → N→A
            self.mr(retries=3, now=60),
            self.xa(1, 120, hi=5, lo=3),                 # 3==lo → A→N
        ]
        seq = [(r["state"], r["changed"])
               for r in self.xa_results(ops)]
        self.assertEqual(seq, [("A", True), ("N", True)])

    def test_same_window_same_params_returns_cached(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60, n=2),
            self.xa(0, 61, n=2),
            self.xa(0, 62, n=2),
        ]
        seq = self.xa_results(ops)
        self.assertEqual(seq[0], seq[1])
        self.assertEqual(seq[1], seq[2])
        self.assertEqual([r["run"] for r in seq], [0, 0, 0])

    def test_same_window_changed_param_is_state(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60, n=2),
            self.xa(0, 61, n=3),
        ]), 4, "STATE")

    def test_param_change_next_window_is_state(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60, hi=5),
            self.xa(1, 120, hi=4),
        ]), 4, "STATE")

    def test_skipped_window_is_state(self):
        # 前跳一窗 w=2（最近已评 w=0）→ STATE。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60),
            self.xa(2, 180),
        ]), 4, "STATE")
        # 回退：w=1 后再评 w=0（即使 w=0 仍在保留窗范围内）→ STATE。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(1, 120),
            self.xa(0, 180),
        ]), 4, "STATE")

    def test_window_not_ended_is_state(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(1, 119),
        ]), 4, "STATE")

    def test_window_too_old_is_state(self):
        # now=3600 当前窗 60、下界 1，w=0 过旧。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 3600),
        ]), 4, "STATE")

    def test_window_lower_bound_is_inclusive(self):
        for now, w in ((3600, 1), (3599, 0)):
            out = self.results([
                {"op": "add", "id": "b", "weight": 1},
                self.xa(w, now),
            ])[-1]
            self.assertEqual(out["w"], w)

    def test_clock_regression_is_input(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=1, now=120),
            self.xa(0, 60),
        ]), 2, "INPUT")

    def test_unknown_id_is_backend(self):
        self.assert_failure(encode_ops([self.xa(0, 60, backend="z")]),
                            3, "BACKEND")

    def test_unknown_id_precedes_window_state(self):
        self.assert_failure(encode_ops([self.xa(5, 60, backend="z")]),
                            3, "BACKEND")

    def test_key_order_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        tail = b"]}"
        cases = [
            # k 与 w 乱序。
            b'{"op":"xa","id":"b","w":0,"k":"R","hi":1,"lo":0,"n":1,"now":60}',
            # hi/lo 乱序。
            b'{"op":"xa","id":"b","k":"R","w":0,"lo":0,"hi":1,"n":1,"now":60}',
            # 缺 w。
            b'{"op":"xa","id":"b","k":"R","hi":1,"lo":0,"n":1,"now":60}',
            # 多键。
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":1,"now":60,"x":1}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_relation_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        tail = b"]}"
        cases = [
            b'{"op":"xa","id":"b","k":"X","w":0,"hi":1,"lo":0,"n":1,"now":60}',   # k 非法
            b'{"op":"xa","id":"b","k":"r","w":0,"hi":1,"lo":0,"n":1,"now":60}',
            b'{"op":"xa","id":"b","k":1,"w":0,"hi":1,"lo":0,"n":1,"now":60}',
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":0,"lo":0,"n":1,"now":60}',   # hi 下界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1000000000000000001,"lo":0,"n":1,"now":60}',  # hi 超界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":-1,"n":1,"now":60}',  # lo 下界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":1000000000000000000,"n":1,"now":60}',  # lo 上界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":1,"n":1,"now":60}',   # lo==hi
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":2,"n":1,"now":60}',   # lo>hi
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":0,"now":60}',   # n 下界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":61,"now":60}',  # n 上界
            b'{"op":"xa","id":"b","k":"R","w":-1,"hi":1,"lo":0,"n":1,"now":60}',  # w 下界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":1,"now":-1}',   # now 下界
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":1,"now":1000000001}',  # now 上界
            b'{"op":"xa","id":"b","k":"R","w":true,"hi":1,"lo":0,"n":1,"now":60}',   # w bool
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":true,"lo":0,"n":1,"now":60}',  # hi bool
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":false,"n":1,"now":60}', # lo bool
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":true,"now":60}',  # n bool
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1,"lo":0,"n":1,"now":true}',   # now bool
            b'{"op":"xa","id":"b","k":"R","w":0,"hi":1.0,"lo":0,"n":1,"now":60}',   # hi 浮点
            b'{"op":"xa","id":"b","k":"R","w":"0","hi":1,"lo":0,"n":1,"now":60}',   # w 字符串
            b'{"op":"xa","id":123,"k":"R","w":0,"hi":1,"lo":0,"n":1,"now":60}',    # id 非字符串
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_threshold_1e18_accepted(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60, hi=10 ** 18, lo=10 ** 18 - 1),
        ])[-1]
        self.assertEqual(
            (out["state"], out["value"], out["run"]), ("N", 0, 0)
        )

    def test_remove_and_readd_clears_each_kind(self):
        # remove 后直接 xa → BACKEND。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.xa(0, 60),
            {"op": "remove", "id": "b"},
            self.xa(1, 120),
        ]), 3, "BACKEND")
        # 重加后 R/M 均回到未首评。
        for k in ("R", "M"):
            seq = self.xa_results([
                {"op": "add", "id": "b", "weight": 1},
                self.mr(retries=9, remaps=9, now=0),
                self.xa(0, 60, k=k, hi=1, lo=0),
                {"op": "remove", "id": "b"},
                {"op": "add", "id": "b", "weight": 1},
                self.xa(1, 120, k=k, hi=1, lo=0),
            ])
            self.assertEqual(
                [(r["state"], r["run"], r["changed"]) for r in seq],
                [("A", 0, True), ("N", 0, False)],
            )

    def test_ci_cb_ca_clear_alarm(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        # ci 成功清告警。
        seq = self.xa_results([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, now=0),
            self.xa(0, 60),                               # A
            {"op": "ci", "config": exported, "now": 120},
            self.xa(1, 120),                              # 未首评：N
        ])
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cb 回滚成功同样清告警（先 ci 产生 rev 1）。
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),                              # A
            {"op": "cb", "rev": 1, "now": 180},
            self.xa(2, 180),                              # 未首评：N
        ]
        seq = self.xa_results(ops)
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cp 预约、ca 生效成功清告警。
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),                              # A
            {"op": "cp", "config": exported, "at": 180, "now": 121},
            {"op": "ca", "now": 180},
            self.xa(2, 180),                              # 未首评：N
        ]
        seq = self.xa_results(ops)
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])

    def test_failed_batch_is_atomic(self):
        # 合法 xa 之后跳窗触发 STATE：整批无 stdout。
        code, stdout, stderr = run_balancer("run", encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xa(9, 600),
        ]))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_single_newline(self):
        _, out, _ = run_balancer("run", encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
            self.xa(0, 61, k="M"),
        ]))
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr(retries=5, remaps=2, now=0),
            self.mr(retries=6, remaps=0, now=60),
            self.xa(0, 60, k="R", n=2),
            self.xa(0, 61, k="R", n=2),
            self.xa(1, 120, k="R", n=2),
            self.xa(1, 121, k="M", hi=3),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")

    def test_record_replay_covers_failure(self):
        raw = encode_ops([self.xa(0, 60, backend="z")])
        _, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, 3)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)
        self.assertEqual(rep_stderr, b'{"error":"BACKEND"}\n')


class ConcurrencyAlertTest(unittest.TestCase):
    """na 每后端并发告警：mx 窗 samples/peak、N/A 滞回、缓存与拒绝。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def na(w, now, hi=1, lo=0, n=1, backend="b"):
        return {"op": "na", "id": backend, "w": w,
                "hi": hi, "lo": lo, "n": n, "now": now}

    @staticmethod
    def open(cid, now=0):
        return {"op": "open", "cid": cid, "flow": FLOW, "now": now}

    @staticmethod
    def close(cid, now=0):
        return {"op": "close", "cid": cid, "now": now}

    @staticmethod
    def ms(now, backend="b"):
        return {"op": "ms", "id": backend, "now": now}

    def na_results(self, ops):
        return [r for r in self.results(ops) if r["op"] == "na"]

    def test_result_key_order_and_value_source(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.open("y", 0),
            self.ms(1),                      # 并发 2
            self.close("y", 2),
            self.ms(3),                      # 并发 1
            self.na(0, 60, hi=2),
        ])[-1]
        self.assertEqual(
            list(out),
            ["op", "id", "w", "state", "samples", "peak", "run", "changed"],
        )
        # samples 为窗内采样点数，peak 为窗内并发峰值。
        self.assertEqual(out["samples"], 2)
        self.assertEqual(out["peak"], 2)
        self.assertEqual(out["state"], "A")
        self.assertEqual(out["run"], 0)
        self.assertIs(out["changed"], True)

    def test_empty_window_zero_starts_N(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 60),
        ])[-1]
        self.assertEqual(
            (out["state"], out["samples"], out["peak"],
             out["run"], out["changed"]),
            ("N", 0, 0, 0, False),
        )

    def test_empty_window_does_not_count_in_A(self):
        # 空窗 samples=0：即使 peak=0<=lo 也不计入 A→N 连续数。
        seq = self.na_results([
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60),                  # A
            self.na(1, 120),                 # 空窗：留 A，run=0
            self.na(2, 180),                 # 空窗：仍留 A
        ])
        self.assertEqual(
            [(r["state"], r["samples"], r["run"], r["changed"]) for r in seq],
            [("A", 1, 0, True), ("A", 0, 0, False), ("A", 0, 0, False)],
        )

    def test_n_accumulates_then_transitions_and_returns(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60, n=2),             # run=1
            self.ms(60),
            self.na(1, 120, n=2),            # N→A
            self.close("x", 120),
            self.ms(121),                    # 并发 0：samples>0 且 peak<=lo
            self.na(2, 180, n=2),            # A 态 run=1
            self.ms(180),
            self.na(3, 240, n=2),            # 连续 2 窗 → A→N
        ]
        seq = [(r["state"], r["run"], r["changed"], r["peak"])
               for r in self.na_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False, 1),
            ("A", 0, True, 1),
            ("A", 1, False, 0),
            ("N", 0, True, 0),
        ])

    def test_direction_mismatch_clears_run_in_N(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60, n=2),             # run=1
            self.close("x", 60),
            self.ms(61),
            self.na(1, 120, n=2),            # peak<hi 清 0
            self.open("y", 120),
            self.ms(121),
            self.na(2, 180, n=2),            # run=1
            self.ms(180),
            self.na(3, 240, n=2),            # run=2→A
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.na_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False), ("N", 0, False),
            ("N", 1, False), ("A", 0, True),
        ])

    def test_direction_mismatch_clears_run_in_A(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60, n=2),             # run=1
            self.ms(60),
            self.na(1, 120, n=2),            # N→A
            self.ms(120),
            self.na(2, 180, n=2),            # peak>lo 清 0 留 A
            self.close("x", 180),
            self.ms(181),
            self.na(3, 240, n=2),            # peak<=lo：A 态 run=1
        ]
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.na_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False), ("A", 0, True),
            ("A", 0, False), ("A", 1, False),
        ])

    def test_hi_lo_boundaries_inclusive(self):
        # peak==hi 满足 >=hi；peak==lo 满足 <=lo。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.open("y", 0),
            self.ms(1),                      # peak 2
            self.na(0, 60, hi=2, lo=1),      # 2==hi → N→A
            self.close("y", 60),
            self.ms(61),                     # peak 1
            self.na(1, 120, hi=2, lo=1),     # 1==lo → A→N
        ]
        seq = [(r["state"], r["changed"]) for r in self.na_results(ops)]
        self.assertEqual(seq, [("A", True), ("N", True)])

    def test_per_backend_independent(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            self.open("x", 0),               # 等权首选取最早加入的 b
            self.ms(1, backend="b"),
            self.ms(1, backend="c"),
            self.na(0, 60, backend="b"),     # b 并发 1 → A
            self.na(0, 61, backend="c"),     # c 并发 0 → 留 N
        ]
        seq = self.na_results(ops)
        self.assertEqual([r["state"] for r in seq], ["A", "N"])
        self.assertEqual(seq[1]["peak"], 0)

    def test_same_window_same_params_returns_cached(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60, n=2),
            self.na(0, 61, n=2),
            self.na(0, 62, n=2),
        ]
        seq = self.na_results(ops)
        self.assertEqual(seq[0], seq[1])
        self.assertEqual(seq[1], seq[2])
        self.assertEqual([r["run"] for r in seq], [1, 1, 1])

    def test_cross_window_uses_maintained_aggregate(self):
        # 填满一个已结束窗（59 个不同 now 采样），跨窗 na 仍须按 ms 同步
        # 维护的 samples/peak 聚合 O(1) 评估，合法跨窗不得失败。
        ops = [{"op": "add", "id": "b", "weight": 1}, self.open("x", 0)]
        ops += [self.ms(t) for t in range(1, 60)]   # 59 样本，峰值并发 1
        ops.append(self.na(0, 60, hi=1))            # N→A
        ops += [self.ms(60), self.na(1, 120, hi=1)]  # 下一连续窗正常评估
        seq = self.na_results(ops)
        self.assertEqual(
            [(r["w"], r["samples"], r["peak"], r["state"], r["changed"])
             for r in seq],
            [(0, 59, 1, "A", True), (1, 1, 1, "A", False)],
        )

    def test_same_now_rereport_not_double_counted(self):
        # 同 (id,now) 同值重报幂等：samples/peak 不重复计数，mx 与 na 一致。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.open("y", 0),
            self.ms(1),
            self.ms(1),                       # 同值重报，不重复计数
            self.na(0, 60, hi=2),
        ]
        na_out = self.na_results(ops)[0]
        self.assertEqual(
            (na_out["samples"], na_out["peak"]), (1, 2)
        )
        mx_out = self.results(ops + [
            {"op": "mx", "id": "b", "from": 0, "to": 0, "now": 60}
        ])[-1]["windows"][0]
        self.assertEqual(
            (mx_out["samples"], mx_out["peak"], mx_out["last"]), (1, 2, 2)
        )

    def test_same_window_changed_param_is_state(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 60, n=2),
            self.na(0, 61, n=3),
        ]), 4, "STATE")

    def test_param_change_next_window_is_state(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 60, hi=2),
            self.na(1, 120, hi=3),
        ]), 4, "STATE")

    def test_skipped_window_is_state(self):
        # 前跳一窗 w=2（最近已评 w=0）→ STATE。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 60),
            self.na(2, 180),
        ]), 4, "STATE")
        # 回退：w=1 后再评 w=0（即使 w=0 仍在保留窗范围内）→ STATE。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(1, 120),
            self.na(0, 180),
        ]), 4, "STATE")

    def test_window_not_ended_is_state(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(1, 119),
        ]), 4, "STATE")
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 59),
        ]), 4, "STATE")

    def test_window_too_old_is_state(self):
        # now=3600 当前窗 60、下界 1，w=0 过旧。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 3600),
        ]), 4, "STATE")

    def test_window_lower_bound_is_inclusive(self):
        for now, w in ((3600, 1), (3599, 0)):
            out = self.results([
                {"op": "add", "id": "b", "weight": 1},
                self.na(w, now),
            ])[-1]
            self.assertEqual(out["w"], w)

    def test_clock_regression_is_input(self):
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.ms(120),
            self.na(0, 60),
        ]), 2, "INPUT")

    def test_unknown_id_is_backend(self):
        self.assert_failure(encode_ops([self.na(0, 60, backend="z")]),
                            3, "BACKEND")

    def test_unknown_id_precedes_window_state(self):
        self.assert_failure(encode_ops([self.na(5, 60, backend="z")]),
                            3, "BACKEND")

    def test_key_order_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        tail = b"]}"
        cases = [
            # hi/lo 乱序。
            b'{"op":"na","id":"b","w":0,"lo":0,"hi":1,"n":1,"now":60}',
            # w 与 hi 乱序。
            b'{"op":"na","id":"b","hi":1,"w":0,"lo":0,"n":1,"now":60}',
            # 缺 w。
            b'{"op":"na","id":"b","hi":1,"lo":0,"n":1,"now":60}',
            # 多键。
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":1,"now":60,"x":1}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_relation_rejections(self):
        head = b'{"ops":[{"op":"add","id":"b","weight":1},'
        tail = b"]}"
        cases = [
            b'{"op":"na","id":"b","w":0,"hi":0,"lo":0,"n":1,"now":60}',   # hi 下界
            b'{"op":"na","id":"b","w":0,"hi":1000000001,"lo":0,"n":1,"now":60}',  # hi 超界
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":-1,"n":1,"now":60}',  # lo 下界
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":1000000000,"n":1,"now":60}',  # lo 上界
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":1,"n":1,"now":60}',   # lo==hi
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":2,"n":1,"now":60}',   # lo>hi
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":0,"now":60}',   # n 下界
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":61,"now":60}',  # n 上界
            b'{"op":"na","id":"b","w":-1,"hi":1,"lo":0,"n":1,"now":60}',  # w 下界
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":1,"now":-1}',   # now 下界
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":1,"now":1000000001}',  # now 上界
            b'{"op":"na","id":"b","w":true,"hi":1,"lo":0,"n":1,"now":60}',   # w bool
            b'{"op":"na","id":"b","w":0,"hi":true,"lo":0,"n":1,"now":60}',  # hi bool
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":false,"n":1,"now":60}', # lo bool
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":true,"now":60}',  # n bool
            b'{"op":"na","id":"b","w":0,"hi":1,"lo":0,"n":1,"now":true}',   # now bool
            b'{"op":"na","id":"b","w":0,"hi":1.0,"lo":0,"n":1,"now":60}',   # hi 浮点
            b'{"op":"na","id":"b","w":"0","hi":1,"lo":0,"n":1,"now":60}',   # w 字符串
            b'{"op":"na","id":123,"w":0,"hi":1,"lo":0,"n":1,"now":60}',    # id 非字符串
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_threshold_bounds_accepted(self):
        out = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 60, hi=10 ** 9, lo=10 ** 9 - 1),
        ])[-1]
        self.assertEqual(
            (out["state"], out["peak"], out["run"]), ("N", 0, 0)
        )

    def test_remove_and_readd_clears(self):
        # remove 后直接 na → BACKEND。
        self.assert_failure(encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.na(0, 60),
            {"op": "remove", "id": "b"},
            self.na(1, 120),
        ]), 3, "BACKEND")
        # 重加后回到未首评。
        seq = self.na_results([
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60),                  # A
            self.close("x", 60),
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            self.na(1, 120),                 # 未首评：N
        ])
        self.assertEqual(
            [(r["state"], r["run"], r["changed"]) for r in seq],
            [("A", 0, True), ("N", 0, False)],
        )

    def test_ci_cb_ca_cu_clear_alarm(self):
        exported = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ce"},
        ])[-1]["config"]
        digest = self.results([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ct"},
        ])[-1]["digest"]
        raise_ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60),                  # A
            self.close("x", 61),
        ]
        # ci 成功清告警。
        seq = self.na_results(
            raise_ops
            + [{"op": "ci", "config": exported, "now": 120},
               self.na(1, 120)]              # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cb 回滚成功同样清告警（先 ci 产生 rev 1）。
        seq = self.na_results(
            [{"op": "ci", "config": exported, "now": 0}]
            + [self.open("x", 0), self.ms(1), self.na(0, 60),
               self.close("x", 61)]
            + [{"op": "cb", "rev": 1, "now": 120},
               self.na(1, 120)]              # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cp 预约、ca 生效成功清告警。
        seq = self.na_results(
            [{"op": "ci", "config": exported, "now": 0}]
            + [self.open("x", 0), self.ms(1), self.na(0, 60),
               self.close("x", 61)]
            + [{"op": "cp", "config": exported, "at": 180, "now": 62},
               {"op": "ca", "now": 180},
               self.na(1, 240)]              # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cu 热加载成功同样清告警。
        seq = self.na_results(
            raise_ops
            + [{"op": "cu", "base": digest, "section": "vnodes",
                "value": None, "now": 62},
               self.na(0, 120)]              # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])

    def test_failed_batch_is_atomic(self):
        # 合法 na 之后跳窗触发 STATE：整批无 stdout。
        code, stdout, stderr = run_balancer("run", encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60),
            self.na(9, 600),
        ]))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_single_newline(self):
        _, out, _ = run_balancer("run", encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60),
        ]))
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.open("x", 0),
            self.ms(1),
            self.na(0, 60, n=2),
            self.ms(60),
            self.na(0, 61, n=2),
            self.na(1, 120, n=2),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")

    def test_record_replay_covers_failure(self):
        raw = encode_ops([self.na(0, 60, backend="z")])
        _, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, 3)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)
        self.assertEqual(rep_stderr, b'{"error":"BACKEND"}\n')


class LimitAlertTest(unittest.TestCase):
    """le 限流告警：lh 窗 token/quota、N/A 滞回、缓存与拒绝。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def le(w, now, hi=1, lo=0, n=1, scope="C", ident="c1"):
        return {"op": "le", "scope": scope, "id": ident, "w": w,
                "hi": hi, "lo": lo, "n": n, "now": now}

    @staticmethod
    def ls(now, scope="C", ident="c1", r=1, b=1):
        return {"op": "ls", "scope": scope, "id": ident,
                "r": r, "b": b, "now": now}

    @staticmethod
    def qs(now, scope="C", ident="c1", limit=1, span=60):
        return {"op": "qs", "scope": scope, "id": ident,
                "limit": limit, "span": span, "now": now}

    @staticmethod
    def oa(cid, now, c="c1", s="s1"):
        return {"op": "oa", "cid": cid, "flow": FLOW, "c": c, "s": s,
                "key": "k", "now": now}

    @staticmethod
    def base_ops():
        # 环上唯一后端 a，供 oa 路由；cap/队列宽裕不构成本测试的阻塞。
        return [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "chash", "vnodes": 4},
            {"op": "os", "cap": 100, "q": 100, "ttl": 1000},
        ]

    def le_results(self, ops):
        return [r for r in self.results(ops) if r["op"] == "le"]

    def test_result_key_order_and_value_source(self):
        out = self.results(
            self.base_ops()
            + [self.ls(0), self.qs(0),
               self.oa("x", 0),          # 接纳：token 1→0、quota used 0→1
               self.oa("y", 0),          # 令牌与配额均不足：token+1、quota+1
               self.le(0, 60, hi=2)]
        )[-1]
        self.assertEqual(
            list(out),
            ["op", "scope", "id", "w", "state", "token", "quota",
             "value", "run", "changed"],
        )
        # value 为 token+quota（封顶 10^18）。
        self.assertEqual((out["token"], out["quota"], out["value"]), (1, 1, 2))
        self.assertEqual(
            (out["state"], out["run"], out["changed"]), ("A", 0, True)
        )

    def test_empty_window_zero_starts_N(self):
        out = self.results(self.base_ops() + [self.ls(0), self.le(0, 60)])[-1]
        self.assertEqual(
            (out["state"], out["token"], out["quota"], out["value"],
             out["run"], out["changed"]),
            ("N", 0, 0, 0, 0, False),
        )

    def test_empty_window_counts_toward_recovery(self):
        # 空窗 value=0<=lo：计入 A→N 连续数（无 na 的 samples>0 门槛）。
        seq = self.le_results(
            self.base_ops()
            + [self.ls(0),
               self.oa("x", 0), self.oa("y", 0),   # w0 token=1
               self.le(0, 60),                     # N→A
               self.le(1, 120)]                    # 空窗：A→N
        )
        self.assertEqual(
            [(r["state"], r["value"], r["run"], r["changed"]) for r in seq],
            [("A", 1, 0, True), ("N", 0, 0, True)],
        )

    def test_n_accumulates_then_transitions_and_returns(self):
        ops = self.base_ops() + [self.ls(0)]
        ops += [self.oa("x", 0), self.oa("y", 0)]      # w0 token=1
        ops += [self.le(0, 60, n=2)]                   # N run=1
        ops += [self.oa("z", 60), self.oa("w", 60)]    # w1 token=1
        ops += [self.le(1, 120, n=2)]                  # N→A
        ops += [self.le(2, 180, n=2)]                  # 空窗：A run=1
        ops += [self.le(3, 240, n=2)]                  # 空窗：A→N
        seq = [(r["state"], r["run"], r["changed"], r["value"])
               for r in self.le_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False, 1),
            ("A", 0, True, 1),
            ("A", 1, False, 0),
            ("N", 0, True, 0),
        ])

    def test_direction_mismatch_clears_run_in_N(self):
        ops = self.base_ops() + [self.ls(0)]
        ops += [self.oa("x", 0), self.oa("y", 0)]      # w0 token=1
        ops += [self.le(0, 60, n=2)]                   # run=1
        ops += [self.le(1, 120, n=2)]                  # 空窗 value<hi 清 0
        ops += [self.oa("z", 120), self.oa("w", 120)]  # w2 token=1
        ops += [self.le(2, 180, n=2)]                  # run=1
        ops += [self.oa("p", 180), self.oa("q", 180)]  # w3 token=1
        ops += [self.le(3, 240, n=2)]                  # run=2→A
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.le_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False), ("N", 0, False),
            ("N", 1, False), ("A", 0, True),
        ])

    def test_direction_mismatch_clears_run_in_A(self):
        ops = self.base_ops() + [self.ls(0)]
        ops += [self.oa("x", 0), self.oa("y", 0)]      # w0 token=1
        ops += [self.le(0, 60, n=2)]                   # run=1
        ops += [self.oa("z", 60), self.oa("w", 60)]    # w1 token=1
        ops += [self.le(1, 120, n=2)]                  # N→A
        ops += [self.oa("p", 120), self.oa("q", 120)]  # w2 token=1
        ops += [self.le(2, 180, n=2)]                  # value>lo 清 0 留 A
        ops += [self.le(3, 240, n=2)]                  # 空窗：A run=1
        seq = [(r["state"], r["run"], r["changed"])
               for r in self.le_results(ops)]
        self.assertEqual(seq, [
            ("N", 1, False), ("A", 0, True),
            ("A", 0, False), ("A", 1, False),
        ])

    def test_hi_lo_boundaries_inclusive(self):
        # value==hi 满足 >=hi；value==lo 满足 <=lo。
        ops = self.base_ops() + [self.ls(0), self.qs(0)]
        ops += [self.oa("x", 0), self.oa("y", 0)]   # w0 token=1 quota=1
        ops += [self.le(0, 60, hi=2, lo=1)]         # value 2==hi → N→A
        ops += [self.qs(60, limit=10)]              # 重配放宽配额（状态保留）
        ops += [self.oa("z", 60), self.oa("w", 60)]  # w1 token=1 quota=0
        ops += [self.le(1, 120, hi=2, lo=1)]        # value 1==lo → A→N
        seq = [(r["state"], r["changed"], r["value"])
               for r in self.le_results(ops)]
        self.assertEqual(seq, [("A", True, 2), ("N", True, 1)])

    def test_per_identifier_independent(self):
        ops = self.base_ops() + [self.ls(0), self.ls(0, ident="c2")]
        ops += [self.oa("x", 0), self.oa("y", 0)]   # c1 w0 token=1
        ops += [self.le(0, 60, ident="c1")]         # A
        ops += [self.le(0, 61, ident="c2")]         # 空窗：N
        seq = self.le_results(ops)
        self.assertEqual([r["state"] for r in seq], ["A", "N"])
        self.assertEqual(seq[1]["value"], 0)

    def test_b_scope_token_and_s_scope_quota(self):
        ops = self.base_ops() + [
            self.ls(0, scope="B", ident="a"),
            self.qs(0, scope="S", ident="s1", limit=1),
        ]
        ops += [self.oa("x", 0), self.oa("y", 0)]   # B token=1、S quota=1
        ops += [self.le(0, 60, scope="B", ident="a"),
                self.le(0, 61, scope="S", ident="s1")]
        seq = self.le_results(ops)
        self.assertEqual(
            [(r["state"], r["token"], r["quota"], r["changed"]) for r in seq],
            [("A", 1, 0, True), ("A", 0, 1, True)],
        )

    def test_same_window_same_params_returns_cached(self):
        ops = self.base_ops() + [self.ls(0)]
        ops += [self.oa("x", 0), self.oa("y", 0)]
        ops += [self.le(0, 60, n=2), self.le(0, 61, n=2), self.le(0, 62, n=2)]
        seq = self.le_results(ops)
        self.assertEqual(seq[0], seq[1])
        self.assertEqual(seq[1], seq[2])
        self.assertEqual([r["run"] for r in seq], [1, 1, 1])

    def test_same_window_changed_param_is_state(self):
        self.assert_failure(encode_ops(
            self.base_ops()
            + [self.ls(0), self.le(0, 60, n=2), self.le(0, 61, n=3)]
        ), 4, "STATE")

    def test_param_change_next_window_is_state(self):
        self.assert_failure(encode_ops(
            self.base_ops()
            + [self.ls(0), self.le(0, 60, hi=2), self.le(1, 120, hi=3)]
        ), 4, "STATE")

    def test_skipped_window_is_state(self):
        # 前跳一窗 w=2（最近已评 w=0）→ STATE。
        self.assert_failure(encode_ops(
            self.base_ops() + [self.ls(0), self.le(0, 60), self.le(2, 180)]
        ), 4, "STATE")
        # 回退：w=1 后再评 w=0（即使 w=0 仍在保留窗范围内）→ STATE。
        self.assert_failure(encode_ops(
            self.base_ops() + [self.ls(0), self.le(1, 120), self.le(0, 180)]
        ), 4, "STATE")

    def test_window_not_ended_is_state(self):
        self.assert_failure(encode_ops(
            self.base_ops() + [self.ls(0), self.le(1, 119)]
        ), 4, "STATE")
        self.assert_failure(encode_ops(
            self.base_ops() + [self.ls(0), self.le(0, 59)]
        ), 4, "STATE")

    def test_window_too_old_is_state(self):
        # now=3600 当前窗 60、下界 1，w=0 过旧。
        self.assert_failure(encode_ops(
            self.base_ops() + [self.ls(0), self.le(0, 3600)]
        ), 4, "STATE")

    def test_window_lower_bound_is_inclusive(self):
        for now, w in ((3600, 1), (3599, 0)):
            out = self.results(
                self.base_ops() + [self.ls(0), self.le(w, now)]
            )[-1]
            self.assertEqual(out["w"], w)

    def test_clock_regression_is_input(self):
        self.assert_failure(encode_ops(
            self.base_ops()
            + [self.ls(0), self.ls(120, r=2, b=2), self.le(0, 60)]
        ), 2, "INPUT")

    def test_unknown_b_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.le(0, 60, scope="B", ident="z")]), 3, "BACKEND"
        )

    def test_unknown_b_id_precedes_no_bucket_state(self):
        # B 未知 id 报 BACKEND，先于无桶无配额与窗口判定。
        self.assert_failure(
            encode_ops([self.le(5, 60, scope="B", ident="z")]), 3, "BACKEND"
        )

    def test_no_bucket_no_quota_is_state(self):
        # C 标识既无在配桶也无在配配额。
        self.assert_failure(encode_ops([self.le(0, 60)]), 4, "STATE")
        # B 后端存在但同样无桶无配额。
        self.assert_failure(encode_ops(
            [{"op": "add", "id": "a", "weight": 1},
             self.le(0, 60, scope="B", ident="a")]
        ), 4, "STATE")

    def test_key_order_rejections(self):
        head = b'{"ops":[{"op":"add","id":"a","weight":1},'
        tail = b"]}"
        cases = [
            # hi/lo 乱序。
            b'{"op":"le","scope":"C","id":"c1","w":0,"lo":0,"hi":1,"n":1,"now":60}',
            # w 与 hi 乱序。
            b'{"op":"le","scope":"C","id":"c1","hi":1,"w":0,"lo":0,"n":1,"now":60}',
            # scope 与 id 乱序。
            b'{"op":"le","id":"c1","scope":"C","w":0,"hi":1,"lo":0,"n":1,"now":60}',
            # 缺 w。
            b'{"op":"le","scope":"C","id":"c1","hi":1,"lo":0,"n":1,"now":60}',
            # 多键。
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":60,"x":1}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_relation_rejections(self):
        head = b'{"ops":[{"op":"add","id":"a","weight":1},'
        tail = b"]}"
        cases = [
            b'{"op":"le","scope":"D","id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":60}',  # scope 非法
            b'{"op":"le","scope":"c","id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":60}',  # scope 小写
            b'{"op":"le","scope":1,"id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":60}',    # scope 非字符串
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":0,"lo":0,"n":1,"now":60}',   # hi 下界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1000000000000000001,"lo":0,"n":1,"now":60}',  # hi 超界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":-1,"n":1,"now":60}',  # lo 下界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":1000000000000000000,"n":1,"now":60}',  # lo 上界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":1,"n":1,"now":60}',   # lo==hi
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":2,"n":1,"now":60}',   # lo>hi
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":0,"now":60}',   # n 下界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":61,"now":60}',  # n 上界
            b'{"op":"le","scope":"C","id":"c1","w":-1,"hi":1,"lo":0,"n":1,"now":60}',  # w 下界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":-1}',   # now 下界
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":1000000001}',  # now 上界
            b'{"op":"le","scope":"C","id":"c1","w":true,"hi":1,"lo":0,"n":1,"now":60}',   # w bool
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":true,"lo":0,"n":1,"now":60}',  # hi bool
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":false,"n":1,"now":60}', # lo bool
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":true,"now":60}',  # n bool
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1,"lo":0,"n":1,"now":true}',   # now bool
            b'{"op":"le","scope":"C","id":"c1","w":0,"hi":1.0,"lo":0,"n":1,"now":60}',   # hi 浮点
            b'{"op":"le","scope":"C","id":"c1","w":"0","hi":1,"lo":0,"n":1,"now":60}',   # w 字符串
            b'{"op":"le","scope":"C","id":123,"w":0,"hi":1,"lo":0,"n":1,"now":60}',    # id 非字符串
            b'{"op":"le","scope":"C","id":"","w":0,"hi":1,"lo":0,"n":1,"now":60}',     # id 空串
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_threshold_bounds_accepted(self):
        out = self.results(
            self.base_ops()
            + [self.ls(0), self.le(0, 60, hi=10 ** 18, lo=10 ** 18 - 1)]
        )[-1]
        self.assertEqual(
            (out["state"], out["value"], out["run"]), ("N", 0, 0)
        )

    def test_ls_qs_reconfig_preserves_alert(self):
        ops = self.base_ops() + [self.ls(0)]
        ops += [self.oa("x", 0), self.oa("y", 0)]   # w0 token=1
        ops += [self.le(0, 60)]                     # N→A
        ops += [self.ls(60, r=2, b=5), self.qs(60)]  # 重配桶并新配配额
        ops += [self.le(1, 120)]                    # 状态保留：空窗 A→N
        seq = [(r["state"], r["changed"]) for r in self.le_results(ops)]
        self.assertEqual(seq, [("A", True), ("N", True)])

    def test_remove_and_readd_clears_b_alert(self):
        # remove 后直接 le（B）→ BACKEND。
        self.assert_failure(encode_ops(
            self.base_ops()
            + [self.ls(0, scope="B", ident="a"),
               self.le(0, 60, scope="B", ident="a"),
               {"op": "remove", "id": "a"},
               self.le(1, 120, scope="B", ident="a")]
        ), 3, "BACKEND")
        # 重加后回到未首评。
        seq = self.le_results(
            self.base_ops()
            + [self.ls(0, scope="B", ident="a"),
               self.oa("x", 0), self.oa("y", 0),        # B 桶 w0 token=1
               self.le(0, 60, scope="B", ident="a"),    # A
               {"op": "close", "cid": "x", "now": 61},
               {"op": "remove", "id": "a"},
               {"op": "add", "id": "a", "weight": 1},
               self.ls(62, scope="B", ident="a"),
               self.le(1, 120, scope="B", ident="a")]   # 未首评：N
        )
        self.assertEqual(
            [(r["state"], r["changed"]) for r in seq],
            [("A", True), ("N", False)],
        )

    def test_ci_cb_ca_cu_clear_alarm(self):
        exported = self.results(
            self.base_ops() + [self.ls(0), {"op": "ce"}]
        )[-1]["config"]
        digest = self.results(
            self.base_ops() + [self.ls(0), {"op": "ct"}]
        )[-1]["digest"]
        raise_ops = self.base_ops() + [
            self.ls(0),
            self.oa("x", 0), self.oa("y", 0),       # w0 token=1（y 排队）
            self.le(0, 60),                         # A
            {"op": "close", "cid": "x", "now": 61},
            {"op": "oc", "cid": "y"},
        ]
        # ci 成功清告警。
        seq = self.le_results(
            raise_ops
            + [{"op": "ci", "config": exported, "now": 120},
               self.le(1, 120)]                     # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cb 回滚成功同样清告警（先 ci 产生 rev 1）。
        seq = self.le_results(
            [{"op": "ci", "config": exported, "now": 0}]
            + [self.oa("x", 0), self.oa("y", 0), self.le(0, 60),
               {"op": "close", "cid": "x", "now": 61},
               {"op": "oc", "cid": "y"}]
            + [{"op": "cb", "rev": 1, "now": 120},
               self.le(1, 120)]                     # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cp 预约、ca 生效成功清告警。
        seq = self.le_results(
            [{"op": "ci", "config": exported, "now": 0}]
            + [self.oa("x", 0), self.oa("y", 0), self.le(0, 60),
               {"op": "close", "cid": "x", "now": 61},
               {"op": "oc", "cid": "y"}]
            + [{"op": "cp", "config": exported, "at": 180, "now": 62},
               {"op": "ca", "now": 180},
               self.le(1, 240)]                     # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])
        # cu 热加载成功同样清告警。
        seq = self.le_results(
            raise_ops
            + [{"op": "cu", "base": digest, "section": "vnodes",
                "value": None, "now": 62},
               self.le(1, 120)]                     # 未首评：N
        )
        self.assertEqual([(r["state"], r["changed"]) for r in seq],
                         [("A", True), ("N", False)])

    def test_failed_batch_is_atomic(self):
        # 合法 le 之后跳窗触发 STATE：整批无 stdout。
        code, stdout, stderr = run_balancer("run", encode_ops(
            self.base_ops()
            + [self.ls(0), self.oa("x", 0), self.oa("y", 0),
               self.le(0, 60), self.le(9, 600)]
        ))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_single_newline(self):
        _, out, _ = run_balancer("run", encode_ops(
            self.base_ops()
            + [self.ls(0), self.oa("x", 0), self.oa("y", 0), self.le(0, 60)]
        ))
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)

    def test_record_replay_byte_identical(self):
        raw = encode_ops(
            self.base_ops()
            + [self.ls(0),
               self.oa("x", 0), self.oa("y", 0),
               self.le(0, 60, n=2),
               self.oa("z", 60), self.oa("w", 60),
               self.le(0, 61, n=2),
               self.le(1, 120, n=2)]
        )
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")

    def test_record_replay_covers_failure(self):
        raw = encode_ops([self.le(0, 60, scope="B", ident="z")])
        _, run_stdout, run_stderr = run_balancer("run", raw)
        _, rec_stdout, _ = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, 3)
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stderr, run_stderr)
        self.assertEqual(rep_stderr, b'{"error":"BACKEND"}\n')


class RetryRemapAlertHistoryTest(unittest.TestCase):
    """xh 重试/重映射告警转换历史：事件追加、区间、只读、裁剪与清除。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(retries=0, remaps=0, now=0, backend="b", ok=True):
        return {"op": "mr", "id": backend, "ok": ok, "ms": 1,
                "retries": retries, "remaps": remaps, "now": now}

    @staticmethod
    def xa(w, now, k="R", hi=5, lo=1, n=1, backend="b"):
        return {"op": "xa", "id": backend, "k": k, "w": w,
                "hi": hi, "lo": lo, "n": n, "now": now}

    @staticmethod
    def xh(start, end, now, k="R", backend="b"):
        return {"op": "xh", "id": backend, "k": k,
                "from": start, "to": end, "now": now}

    def add(self):
        return {"op": "add", "id": "b", "weight": 1}

    def test_transition_recorded_once(self):
        # w0 retries=5 转 A 记录一次；同窗重报不重复；w1 retries=1 转回 N。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.mr(retries=1, now=60),
            self.xa(0, 120),                          # 同窗重报
            self.xa(1, 120),
            self.xh(0, 1, 120),
        ]
        events = self.results(ops)[-1]["events"]
        self.assertEqual(
            events,
            [
                {"window": 0, "from": "N", "to": "A", "value": 5,
                 "hi": 5, "lo": 1, "n": 1},
                {"window": 1, "from": "A", "to": "N", "value": 1,
                 "hi": 5, "lo": 1, "n": 1},
            ],
        )
        for event in events:
            self.assertEqual(
                list(event),
                ["window", "from", "to", "value", "hi", "lo", "n"],
            )

    def test_event_value_taken_from_triggering_evaluation(self):
        # n=2：触发转换的 w1 窗累计 retries=7，事件 value 取该次评估值 7。
        ops = [
            self.add(),
            self.mr(retries=5, now=0), self.xa(0, 60, n=2),
            self.mr(retries=7, now=60), self.xa(1, 120, n=2),
            self.xh(0, 1, 120),
        ]
        events = self.results(ops)[-1]["events"]
        self.assertEqual(
            events,
            [{"window": 1, "from": "N", "to": "A", "value": 7,
              "hi": 5, "lo": 1, "n": 2}],
        )
        for field in ("window", "value", "hi", "lo", "n"):
            self.assertIsInstance(events[0][field], int)
            self.assertNotIsInstance(events[0][field], bool)

    def test_no_transition_records_nothing(self):
        # 首评停留 N、方向不符与 n 未达（含缺窗 value=0 反向）均不记录。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60, n=2),                        # N，run=1
            self.xa(1, 120, n=2),                       # 缺窗清 0
            self.xh(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_n_run_event_lands_on_trigger_window(self):
        ops = [self.add()]
        for w, r in ((0, 5), (1, 6), (2, 7)):
            ops.append(self.mr(retries=r, now=w * 60 + 1))
            ops.append(self.xa(w, w * 60 + 60, n=3))
        ops.append(self.xh(0, 2, 180))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, [2])

    def test_r_and_m_have_independent_history(self):
        # R 转换、M 不转换：两键历史独立。
        ops = [
            self.add(),
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
            self.xa(0, 61, k="M", hi=3, lo=1),
            self.xh(0, 0, 61, k="R"),
            self.xh(0, 0, 62, k="M"),
        ]
        results = self.results(ops)
        self.assertEqual(len(results[-2]["events"]), 1)
        self.assertEqual(results[-2]["events"][0]["value"], 5)
        self.assertEqual(results[-1]["events"], [])

    def test_per_backend_isolation(self):
        ops = [
            self.add(),
            {"op": "add", "id": "c", "weight": 1},
            self.mr(retries=5, now=0, backend="b"),
            self.xa(0, 60, backend="b"),
            self.xa(0, 61, backend="c"),
            self.xh(0, 0, 61, backend="b"),
            self.xh(0, 0, 62, backend="c"),
        ]
        results = self.results(ops)
        self.assertEqual(len(results[-2]["events"]), 1)
        self.assertEqual(results[-1]["events"], [])

    def test_closed_range_filter_and_window_order(self):
        # N→A 与 A→N 交替发生于 w0..w3；区间闭、按 window 升序。
        ops = [self.add()]
        for w in range(4):
            ops.append(self.mr(
                retries=5 if w % 2 == 0 else 1, now=w * 60 + 1
            ))
            ops.append(self.xa(w, w * 60 + 60))
        ops.append(self.xh(1, 2, 240))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, [1, 2])

    def test_empty_range_and_never_evaluated(self):
        ops = [self.add(), self.xh(0, 0, 60)]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_history_trimmed_to_last_60_windows(self):
        # 每窗交替转换；xa w=60 成功后删除 window<1 的事件，w0 不再可见。
        ops = [self.add()]
        for w in range(61):
            ops.append(self.mr(
                retries=5 if w % 2 == 0 else 1, now=w * 60 + 1
            ))
            ops.append(self.xa(w, w * 60 + 60))
        ops.append(self.xh(2, 60, 3660))
        windows = [
            event["window"] for event in self.results(ops)[-1]["events"]
        ]
        self.assertEqual(windows, list(range(2, 61)))

    def test_xh_is_read_only(self):
        # xh 不推进告警状态机：随后同窗 xa 仍命中首评缓存，历史不被查询
        # 清理；结果事件为逐项拷贝，不被后续评估改写。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xh(0, 1, 60),
            self.xa(0, 60),
            self.mr(retries=1, now=60),
            self.xa(1, 120),
        ]
        results = self.results(ops)
        self.assertEqual(
            results[3]["events"],
            [{"window": 0, "from": "N", "to": "A", "value": 5,
              "hi": 5, "lo": 1, "n": 1}],
        )
        # 同窗 xa 在 xh 之后仍原样返回首评结果（changed 仍为 true）。
        self.assertTrue(results[4]["changed"])
        self.assertEqual(results[4]["w"], 0)

    def test_remove_and_readd_clears_history(self):
        for k in ("R", "M"):
            ops = [
                self.add(),
                self.mr(retries=9, remaps=9, now=0),
                self.xa(0, 60, k=k, hi=1, lo=0),
                {"op": "remove", "id": "b"},
                self.add(),
                self.xh(0, 1, 120, k=k),
            ]
            self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_ci_clears_history(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            {"op": "ci", "config": exported, "now": 120},
            self.xh(0, 1, 120),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_cb_clears_history(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),
            {"op": "cb", "rev": 1, "now": 180},
            self.xh(0, 1, 180),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_ca_clears_history(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),
            {"op": "cp", "config": exported, "at": 180, "now": 121},
            {"op": "ca", "now": 180},
            self.xh(0, 1, 180),
        ]
        self.assertEqual(self.results(ops)[-1]["events"], [])

    def test_unknown_id_is_backend(self):
        self.assert_failure(encode_ops([self.xh(0, 0, 60, "R", "z")]),
                            3, "BACKEND")

    def test_unknown_id_precedes_state(self):
        # from 已低于最近 60 窗下界：未知 id 仍先判 BACKEND。
        self.assert_failure(
            encode_ops([self.xh(0, 0, 3600, "R", "z")]), 3, "BACKEND"
        )

    def test_from_too_early_is_state(self):
        # now=3600：当前窗 60、下界 1，from=0 → STATE。
        self.assert_failure(
            encode_ops([self.add(), self.xh(0, 0, 3600)]),
            4, "STATE",
        )

    def test_lower_bound_boundary_allowed(self):
        # now=3599 → current=59、下界 0，from=0 合法。
        results = self.results([self.add(), self.xh(0, 59, 3599)])
        self.assertEqual(results[-1]["events"], [])

    def test_clock_regression_is_input(self):
        ops = [
            self.add(),
            {"op": "mg", "id": "b", "now": 120},
            self.xh(0, 0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_clock_equal_allowed(self):
        results = self.results([
            self.add(),
            {"op": "mg", "id": "b", "now": 120},
            self.xh(0, 1, 120),
        ])
        self.assertEqual(results[-1]["op"], "xh")

    def test_result_key_order(self):
        out = self.results([
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xh(0, 0, 60),
        ])[-1]
        self.assertEqual(list(out), ["op", "id", "k", "events"])
        self.assertEqual(out["k"], "R")

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            # k 与 from 乱序。
            b'{"op":"xh","id":"b","from":0,"k":"R","to":0,"now":60}',
            b'{"op":"xh","id":"b","k":"R","from":0,"now":60,"to":0}',
            # 缺键。
            b'{"op":"xh","id":"b","k":"R","to":0,"now":60}',
            # 多键。
            b'{"op":"xh","id":"b","k":"R","from":0,"to":0,"now":60,"x":1}',
            # 非对象、id 形状。
            b'{"op":"xh","id":1,"k":"R","from":0,"to":0,"now":60}',
            b'{"op":"xh","id":"","k":"R","from":0,"to":0,"now":60}',
            # k 非法。
            b'{"op":"xh","id":"b","k":"X","from":0,"to":0,"now":60}',
            b'{"op":"xh","id":"b","k":1,"from":0,"to":0,"now":60}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_and_relation_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            b'{"op":"xh","id":"b","k":"R","from":-1,"to":0,"now":60}',
            b'{"op":"xh","id":"b","k":"R","from":0,"to":-1,"now":60}',
            b'{"op":"xh","id":"b","k":"R","from":0,"to":0,"now":-1}',
            b'{"op":"xh","id":"b","k":"R","from":0,"to":1000000001,"now":1000000001}',
            b'{"op":"xh","id":"b","k":"R","from":0,"to":0,"now":true}',
            b'{"op":"xh","id":"b","k":"R","from":0,"to":0.5,"now":60}',
            b'{"op":"xh","id":"b","k":"R","from":"0","to":0,"now":60}',
            # from>to。
            b'{"op":"xh","id":"b","k":"R","from":1,"to":0,"now":60}',
            # to>now//60。
            b'{"op":"xh","id":"b","k":"R","from":0,"to":1,"now":59}',
            # to-from>=60。
            b'{"op":"xh","id":"b","k":"R","from":0,"to":60,"now":3600}',
            b'{"op":"xh","id":"b","k":"R","from":0,"to":61,"now":3660}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_input_precedes_backend(self):
        # 关系非法即使 id 未知也先判 INPUT。
        self.assert_failure(
            b'{"ops":[{"op":"xh","id":"z","k":"R","from":1,"to":0,"now":60}]}',
            2, "INPUT",
        )

    def test_failed_batch_is_atomic(self):
        # 合法 xa 产生事件后，跳窗 xa 触发 STATE：整批无 stdout，历史不落盘。
        code, stdout, stderr = run_balancer("run", encode_ops([
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xa(9, 600),
        ]))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_single_newline(self):
        _, out, _ = run_balancer("run", encode_ops([
            self.add(),
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
            self.xh(0, 0, 60, k="R"),
        ]))
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        self.assertIn(
            b'"op":"xh","id":"b","k":"R","events":[{"window":0,'
            b'"from":"N","to":"A","value":5,"hi":5,"lo":1,"n":1}]',
            out,
        )

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            self.add(),
            self.mr(retries=5, remaps=9, now=0),
            self.mr(retries=1, now=60),
            self.xa(0, 60, k="R", n=2),
            self.xa(0, 61, k="R", n=2),
            self.xa(1, 120, k="R", n=2),
            self.xa(1, 121, k="M", hi=3),
            self.xh(0, 1, 121, k="R"),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class RetryRemapAlertSummaryTest(unittest.TestCase):
    """xg 重试/重映射告警汇总：state/last/raised/cleared/total 与错误前置。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(retries=0, remaps=0, now=0, backend="b", ok=True):
        return {"op": "mr", "id": backend, "ok": ok, "ms": 1,
                "retries": retries, "remaps": remaps, "now": now}

    @staticmethod
    def xa(w, now, k="R", hi=5, lo=1, n=1, backend="b"):
        return {"op": "xa", "id": backend, "k": k, "w": w,
                "hi": hi, "lo": lo, "n": n, "now": now}

    @staticmethod
    def xg(start, end, now, k="R", backend="b"):
        return {"op": "xg", "id": backend, "k": k,
                "from": start, "to": end, "now": now}

    @staticmethod
    def xh(start, end, now, k="R", backend="b"):
        return {"op": "xh", "id": backend, "k": k,
                "from": start, "to": end, "now": now}

    def add(self, backend="b"):
        return {"op": "add", "id": backend, "weight": 1}

    def alternating(self, count, k="R"):
        # w0..count-1 每窗交替：偶窗 N→A（retries=5），奇窗 A→N（retries=1）。
        ops = [self.add()]
        for w in range(count):
            ops.append(self.mr(
                retries=5 if w % 2 == 0 else 1, now=w * 60 + 1
            ))
            ops.append(self.xa(w, w * 60 + 60, k=k))
        return ops

    def test_never_evaluated_is_quiet(self):
        # 从未评估：state=N、last=null、三项计数全 0。
        out = self.results([self.add(), self.xg(0, 0, 60)])[-1]
        self.assertEqual(
            out,
            {"op": "xg", "id": "b", "k": "R", "state": "N", "last": None,
             "total": 0, "raised": 0, "cleared": 0},
        )

    def test_evaluated_without_transition(self):
        # 已首评但未转换（n=2 仅 run=1）：state=N、last=null、计数 0。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60, n=2),
            self.xg(0, 0, 60),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(out["state"], "N")
        self.assertIsNone(out["last"])
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (0, 0, 0))

    def test_summary_counts_and_current_state(self):
        # w0 N→A、w1 A→N、w2 N→A：区间 [0,2] raised=2、cleared=1，当前 A。
        ops = self.alternating(3)
        ops.append(self.xg(0, 2, 180))
        out = self.results(ops)[-1]
        self.assertEqual(out["state"], "A")
        self.assertEqual(out["last"], 2)
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (2, 1, 3))

    def test_state_n_after_clearing_transition(self):
        # 末窗 A→N 后 state=N，last 仍为最近转换窗。
        ops = self.alternating(2)
        ops.append(self.xg(0, 1, 120))
        out = self.results(ops)[-1]
        self.assertEqual(out["state"], "N")
        self.assertEqual(out["last"], 1)
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (1, 1, 2))

    def test_last_ignores_query_range(self):
        # last 取现存历史中最近转换窗，不限闭区间 [from,to]。
        ops = self.alternating(3)
        ops.append(self.xg(1, 1, 180))
        out = self.results(ops)[-1]
        # 区间内仅 w1 的 A→N；last 仍为区间外的 w2。
        self.assertEqual(out["last"], 2)
        self.assertEqual(out["state"], "A")
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (0, 1, 1))

    def test_closed_range_boundaries(self):
        ops = self.alternating(4)
        ops.append(self.xg(1, 2, 240))
        out = self.results(ops)[-1]
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (1, 1, 2))

    def test_range_without_events(self):
        # w0 N→A 后查询缺转换的 w1：计数全 0，last 仍取区间外 w0。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xg(1, 1, 120),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(out["last"], 0)
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (0, 0, 0))

    def test_r_and_m_independent(self):
        ops = [
            self.add(),
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
            self.xa(0, 61, k="M", hi=3, lo=1),
            self.xg(0, 0, 61, k="R"),
            self.xg(0, 0, 62, k="M"),
        ]
        results = self.results(ops)
        self.assertEqual(
            (results[-2]["state"], results[-2]["raised"],
             results[-2]["cleared"], results[-2]["total"],
             results[-2]["last"]),
            ("A", 1, 0, 1, 0),
        )
        # M 值 2 未达 hi=3，停留 N。
        self.assertEqual(
            (results[-1]["state"], results[-1]["last"],
             results[-1]["total"]),
            ("N", None, 0),
        )

    def test_per_backend_isolation(self):
        ops = [
            self.add(),
            self.add("c"),
            self.mr(retries=5, now=0, backend="b"),
            self.xa(0, 60, backend="b"),
            self.xa(0, 61, backend="c"),
            self.xg(0, 0, 61, backend="b"),
            self.xg(0, 0, 62, backend="c"),
        ]
        results = self.results(ops)
        self.assertEqual(results[-2]["state"], "A")
        self.assertEqual(results[-2]["raised"], 1)
        self.assertEqual(results[-1]["state"], "N")
        self.assertEqual(results[-1]["total"], 0)
        self.assertIsNone(results[-1]["last"])

    def test_last_sees_only_retained_60_windows(self):
        # 61 窗交替；xa w=60 后 w0 事件已裁剪。查询 [2,2]：raised=1，
        # last 为现存历史最近窗 60（区间外）。
        ops = self.alternating(61)
        ops.append(self.xg(2, 2, 3660))
        out = self.results(ops)[-1]
        self.assertEqual(out["last"], 60)
        self.assertEqual(out["state"], "A")
        self.assertEqual((out["raised"], out["cleared"], out["total"]),
                         (1, 0, 1))

    def test_xg_is_read_only(self):
        # xg 不推进状态机：随后同窗 xa 命中首评缓存（changed 原样）；
        # 不裁剪历史，xh 仍见全部事件。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xg(0, 1, 60),
            self.xa(0, 60),
            self.xh(0, 1, 60),
        ]
        results = self.results(ops)
        self.assertTrue(results[4]["changed"])
        self.assertEqual(results[4]["w"], 0)
        self.assertEqual(
            [e["window"] for e in results[-1]["events"]], [0]
        )

    def test_remove_and_readd_clears(self):
        for k in ("R", "M"):
            ops = [
                self.add(),
                self.mr(retries=9, remaps=9, now=0),
                self.xa(0, 60, k=k, hi=1, lo=0),
                {"op": "remove", "id": "b"},
                self.add(),
                self.xg(0, 1, 120, k=k),
            ]
            out = self.results(ops)[-1]
            self.assertEqual(out["state"], "N")
            self.assertIsNone(out["last"])
            self.assertEqual(out["total"], 0)

    def test_ci_clears(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            {"op": "ci", "config": exported, "now": 120},
            self.xg(0, 1, 120),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(out["state"], "N")
        self.assertIsNone(out["last"])
        self.assertEqual(out["total"], 0)

    def test_cb_clears(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),
            {"op": "cb", "rev": 1, "now": 180},
            self.xg(0, 1, 180),
        ]
        out = self.results(ops)[-1]
        self.assertEqual((out["state"], out["last"], out["total"]),
                         ("N", None, 0))

    def test_ca_clears(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),
            {"op": "cp", "config": exported, "at": 180, "now": 121},
            {"op": "ca", "now": 180},
            self.xg(0, 1, 180),
        ]
        out = self.results(ops)[-1]
        self.assertEqual((out["state"], out["last"], out["total"]),
                         ("N", None, 0))

    def test_unknown_id_is_backend(self):
        self.assert_failure(encode_ops([self.xg(0, 0, 60, "R", "z")]),
                            3, "BACKEND")

    def test_unknown_id_precedes_state(self):
        # from 已低于最近 60 窗下界：未知 id 仍先判 BACKEND。
        self.assert_failure(
            encode_ops([self.xg(0, 0, 3600, "R", "z")]), 3, "BACKEND"
        )

    def test_from_too_early_is_state(self):
        # now=3600：当前窗 60、下界 1，from=0 → STATE。
        self.assert_failure(
            encode_ops([self.add(), self.xg(0, 0, 3600)]),
            4, "STATE",
        )

    def test_lower_bound_boundary_allowed(self):
        # now=3599 → current=59、下界 0，from=0 合法。
        results = self.results([self.add(), self.xg(0, 59, 3599)])
        out = results[-1]
        self.assertEqual(out["state"], "N")
        self.assertEqual(out["total"], 0)

    def test_clock_regression_is_input(self):
        ops = [
            self.add(),
            {"op": "mg", "id": "b", "now": 120},
            self.xg(0, 0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_clock_equal_allowed(self):
        results = self.results([
            self.add(),
            {"op": "mg", "id": "b", "now": 120},
            self.xg(0, 1, 120),
        ])
        self.assertEqual(results[-1]["op"], "xg")

    def test_result_key_order(self):
        out = self.results([
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xg(0, 0, 60),
        ])[-1]
        self.assertEqual(
            list(out),
            ["op", "id", "k", "state", "last", "total", "raised",
             "cleared"],
        )
        self.assertEqual(out["k"], "R")

    def test_counter_types(self):
        ops = self.alternating(2)
        ops.append(self.xg(0, 1, 120))
        out = self.results(ops)[-1]
        for field in ("total", "raised", "cleared"):
            self.assertIsInstance(out[field], int)
            self.assertNotIsInstance(out[field], bool)
            self.assertGreaterEqual(out[field], 0)
        self.assertIsInstance(out["last"], int)

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            # k 与 from 乱序。
            b'{"op":"xg","id":"b","from":0,"k":"R","to":0,"now":60}',
            b'{"op":"xg","id":"b","k":"R","from":0,"now":60,"to":0}',
            # 缺键。
            b'{"op":"xg","id":"b","k":"R","to":0,"now":60}',
            # 多键。
            b'{"op":"xg","id":"b","k":"R","from":0,"to":0,"now":60,"x":1}',
            # 非对象、id 形状。
            b'{"op":"xg","id":1,"k":"R","from":0,"to":0,"now":60}',
            b'{"op":"xg","id":"","k":"R","from":0,"to":0,"now":60}',
            # k 非法。
            b'{"op":"xg","id":"b","k":"X","from":0,"to":0,"now":60}',
            b'{"op":"xg","id":"b","k":1,"from":0,"to":0,"now":60}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_type_range_and_relation_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            b'{"op":"xg","id":"b","k":"R","from":-1,"to":0,"now":60}',
            b'{"op":"xg","id":"b","k":"R","from":0,"to":-1,"now":60}',
            b'{"op":"xg","id":"b","k":"R","from":0,"to":0,"now":-1}',
            b'{"op":"xg","id":"b","k":"R","from":0,"to":1000000001,"now":1000000001}',
            b'{"op":"xg","id":"b","k":"R","from":0,"to":0,"now":true}',
            b'{"op":"xg","id":"b","k":"R","from":0,"to":0.5,"now":60}',
            b'{"op":"xg","id":"b","k":"R","from":"0","to":0,"now":60}',
            # from>to。
            b'{"op":"xg","id":"b","k":"R","from":1,"to":0,"now":60}',
            # to>now//60。
            b'{"op":"xg","id":"b","k":"R","from":0,"to":1,"now":59}',
            # to-from>=60。
            b'{"op":"xg","id":"b","k":"R","from":0,"to":60,"now":3600}',
            b'{"op":"xg","id":"b","k":"R","from":0,"to":61,"now":3660}',
        ]
        for body in cases:
            self.assert_failure(head + body + tail, 2, "INPUT")

    def test_input_precedes_backend(self):
        # 关系非法即使 id 未知也先判 INPUT。
        self.assert_failure(
            b'{"ops":[{"op":"xg","id":"z","k":"R","from":1,"to":0,"now":60}]}',
            2, "INPUT",
        )

    def test_failed_batch_is_atomic(self):
        # 合法 xa 产生事件与 A 态后，跳窗 xa 触发 STATE：整批无 stdout，
        # xg 不落任何输出，状态不改变。
        code, stdout, stderr = run_balancer("run", encode_ops([
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xg(0, 0, 60),
            self.xa(9, 600),
        ]))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_single_newline(self):
        _, out, _ = run_balancer("run", encode_ops([
            self.add(),
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
            self.xg(0, 0, 60, k="R"),
        ]))
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        self.assertIn(
            b'"op":"xg","id":"b","k":"R","state":"A","last":0,'
            b'"total":1,"raised":1,"cleared":0',
            out,
        )

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            self.add(),
            self.mr(retries=5, remaps=9, now=0),
            self.mr(retries=1, now=60),
            self.xa(0, 60, k="R", n=2),
            self.xa(0, 61, k="R", n=2),
            self.xa(1, 120, k="R", n=2),
            self.xa(1, 121, k="M", hi=3),
            self.xg(0, 1, 121, k="R"),
            self.xg(0, 0, 121, k="M"),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, 0)
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class RetryRemapPoolOverviewTest(unittest.TestCase):
    """xp 池级 R/M 告警概览：现存后端汇总、UTF-8 字节序与错误前置。"""

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def mr(retries=0, remaps=0, now=0, backend="b"):
        return {"op": "mr", "id": backend, "ok": True, "ms": 1,
                "retries": retries, "remaps": remaps, "now": now}

    @staticmethod
    def xa(w, now, k="R", hi=5, lo=1, n=1, backend="b"):
        return {"op": "xa", "id": backend, "k": k, "w": w,
                "hi": hi, "lo": lo, "n": n, "now": now}

    @staticmethod
    def xp(start, end, now):
        return {"op": "xp", "from": start, "to": end, "now": now}

    @staticmethod
    def add(backend="b"):
        return {"op": "add", "id": backend, "weight": 1}

    def test_empty_pool(self):
        out = self.results([self.xp(0, 0, 60)])[-1]
        self.assertEqual(
            out,
            {"op": "xp", "backends": [],
             "total": {
                 "R": {"alerting": 0, "raised": 0, "cleared": 0},
                 "M": {"alerting": 0, "raised": 0, "cleared": 0},
             }},
        )

    def test_result_key_order(self):
        out = self.results([
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xp(0, 0, 60),
        ])[-1]
        self.assertEqual(list(out), ["op", "backends", "total"])
        self.assertEqual(list(out["total"]), ["R", "M"])
        self.assertEqual(list(out["total"]["R"]),
                         ["alerting", "raised", "cleared"])
        self.assertEqual(list(out["backends"][0]), ["id", "R", "M"])
        self.assertEqual(list(out["backends"][0]["R"]),
                         ["state", "raised", "cleared"])

    def test_backends_sorted_by_utf8_bytes(self):
        # "b"=0x62 < "c"=0x63 < "é"=0xc3a9（Python 码位序为 b<c<é，
        # 但加入序故意打乱；再以多字节字符验证按字节而非加入序）。
        ops = [
            self.add("é"), self.add("c"), self.add("b"), self.add("中"),
            self.xp(0, 0, 60),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(
            [item["id"] for item in out["backends"]],
            ["b", "c", "é", "中"],
        )

    def test_state_counts_and_totals(self):
        # b：R 在 w0 N→A；c：R/M 均未评估；d：M 在 w1 N→A。
        ops = [
            self.add("b"), self.add("c"), self.add("d"),
            self.mr(retries=5, now=0, backend="b"),
            self.xa(0, 60, k="R", backend="b"),
            self.mr(remaps=5, now=60, backend="d"),
            self.xa(1, 120, k="M", hi=5, lo=1, backend="d"),
            self.xp(0, 1, 120),
        ]
        out = self.results(ops)[-1]
        by_id = {item["id"]: item for item in out["backends"]}
        self.assertEqual(
            by_id["b"]["R"],
            {"state": "A", "raised": 1, "cleared": 0},
        )
        self.assertEqual(
            by_id["b"]["M"],
            {"state": "N", "raised": 0, "cleared": 0},
        )
        self.assertEqual(by_id["c"]["R"]["state"], "N")
        self.assertEqual(
            by_id["d"]["M"],
            {"state": "A", "raised": 1, "cleared": 0},
        )
        self.assertEqual(
            out["total"]["R"],
            {"alerting": 1, "raised": 1, "cleared": 0},
        )
        self.assertEqual(
            out["total"]["M"],
            {"alerting": 1, "raised": 1, "cleared": 0},
        )

    def test_raised_and_cleared_in_range(self):
        # w0 N→A、w1 A→N、w2 N→A；查询 [1,1] 只计 cleared=1。
        ops = [self.add()]
        for w in (0, 1, 2):
            ops.append(self.mr(
                retries=5 if w % 2 == 0 else 1, now=w * 60 + 1
            ))
            ops.append(self.xa(w, w * 60 + 60))
        ops.append(self.xp(1, 1, 180))
        out = self.results(ops)[-1]
        self.assertEqual(
            out["backends"][0]["R"],
            {"state": "A", "raised": 0, "cleared": 1},
        )
        # alerting 取当前 A 态，与区间无关。
        self.assertEqual(
            out["total"]["R"],
            {"alerting": 1, "raised": 0, "cleared": 1},
        )

    def test_alerting_zero_when_cleared(self):
        ops = [self.add()]
        for w in (0, 1):
            ops.append(self.mr(
                retries=5 if w == 0 else 1, now=w * 60 + 1
            ))
            ops.append(self.xa(w, w * 60 + 60))
        ops.append(self.xp(0, 1, 120))
        out = self.results(ops)[-1]
        self.assertEqual(out["backends"][0]["R"]["state"], "N")
        self.assertEqual(
            out["total"]["R"],
            {"alerting": 0, "raised": 1, "cleared": 1},
        )

    def test_xp_is_read_only(self):
        # xp 不推进状态机：随后同窗 xa 命中缓存（changed 原样）；不裁剪
        # 历史，xh 仍见事件。
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xp(0, 0, 60),
            self.xa(0, 60),
        ]
        results = self.results(ops)
        self.assertTrue(results[-1]["changed"])
        self.assertEqual(results[-1]["w"], 0)

    def test_xp_does_not_trim_history(self):
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xp(0, 0, 60),
            {"op": "xh", "id": "b", "k": "R", "from": 0,
             "to": 0, "now": 60},
        ]
        out = self.results(ops)[-1]
        self.assertEqual([e["window"] for e in out["events"]], [0])

    def test_remove_and_readd_clears(self):
        ops = [
            self.add(),
            self.mr(retries=9, remaps=9, now=0),
            self.xa(0, 60, k="R"),
            self.xa(0, 60, k="M"),
            {"op": "remove", "id": "b"},
            self.add(),
            self.xp(0, 1, 120),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(len(out["backends"]), 1)
        for kind in ("R", "M"):
            self.assertEqual(
                out["backends"][0][kind],
                {"state": "N", "raised": 0, "cleared": 0},
            )
        self.assertEqual(
            out["total"],
            {
                "R": {"alerting": 0, "raised": 0, "cleared": 0},
                "M": {"alerting": 0, "raised": 0, "cleared": 0},
            },
        )

    def test_ci_clears(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            {"op": "ci", "config": exported, "now": 120},
            self.xp(0, 1, 120),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(out["backends"][0]["R"]["state"], "N")
        self.assertEqual(out["total"]["R"]["alerting"], 0)

    def test_cb_clears(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            {"op": "ci", "config": exported, "now": 0},
            self.mr(retries=5, now=60),
            self.xa(1, 120),
            {"op": "cb", "rev": 1, "now": 180},
            self.xp(0, 1, 180),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(out["backends"][0]["R"]["state"], "N")
        self.assertEqual(out["total"]["R"]["raised"], 0)

    def test_ca_clears(self):
        exported = self.results([
            self.add(), {"op": "ce"},
        ])[-1]["config"]
        ops = [
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            {"op": "cp", "config": exported, "at": 120, "now": 61},
            {"op": "ca", "now": 120},
            self.xp(0, 1, 120),
        ]
        out = self.results(ops)[-1]
        self.assertEqual(out["backends"][0]["R"]["state"], "N")
        self.assertEqual(out["total"]["R"]["alerting"], 0)

    def test_from_too_early_is_state(self):
        # now=3600：当前窗 60、下界 1，from=0 → STATE。
        self.assert_failure(
            encode_ops([self.add(), self.xp(0, 0, 3600)]),
            4, "STATE",
        )

    def test_lower_bound_boundary_allowed(self):
        # now=3599 → current=59、下界 0，from=0 合法；空池亦然。
        out = self.results([self.xp(0, 59, 3599)])[-1]
        self.assertEqual(out["backends"], [])

    def test_clock_regression_is_input(self):
        ops = [
            self.add(),
            {"op": "mg", "id": "b", "now": 120},
            self.xp(0, 0, 60),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_clock_equal_allowed(self):
        out = self.results([
            self.add(),
            {"op": "mg", "id": "b", "now": 120},
            self.xp(0, 1, 120),
        ])[-1]
        self.assertEqual(out["op"], "xp")

    def test_key_order_and_shape_rejections(self):
        head = b'{"ops":['
        tail = b']}'
        cases = [
            # 乱序键。
            b'{"op":"xp","now":60,"from":0,"to":0}',
            # 缺键。
            b'{"op":"xp","from":0,"to":0}',
            b'{"op":"xp","from":0,"now":60}',
            b'{"op":"xp","to":0,"now":60}',
            # 多键。
            b'{"op":"xp","from":0,"to":0,"now":60,"x":1}',
            # 类型错误。
            b'{"op":"xp","from":"0","to":0,"now":60}',
            b'{"op":"xp","from":0,"to":null,"now":60}',
            b'{"op":"xp","from":0,"to":0,"now":1.5}',
            # bool 不接受。
            b'{"op":"xp","from":false,"to":0,"now":60}',
            # 越界。
            b'{"op":"xp","from":0,"to":0,"now":1000000001}',
            b'{"op":"xp","from":-1,"to":0,"now":60}',
            # 关系错误：from>to、to>now//60、to-from>=60。
            b'{"op":"xp","from":2,"to":1,"now":180}',
            b'{"op":"xp","from":0,"to":2,"now":60}',
            b'{"op":"xp","from":0,"to":60,"now":3600}',
            # 非对象。
            b'"xp"',
            b'42',
        ]
        for case in cases:
            self.assert_failure(head + case + tail, 2, "INPUT")

    def test_unknown_op_is_input(self):
        self.assert_failure(
            b'{"ops":[{"op":"xp"}]}', 2, "INPUT",
        )

    def test_failed_batch_is_atomic(self):
        # 合法 xa 后 xp 的 from 过早触发 STATE：整批无 stdout。
        code, stdout, stderr = run_balancer("run", encode_ops([
            self.add(),
            self.mr(retries=5, now=0),
            self.xa(0, 60),
            self.xp(0, 0, 3600),
        ]))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_output_single_newline_and_bytes(self):
        _, out, _ = run_balancer("run", encode_ops([self.xp(0, 0, 60)]))
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        self.assertEqual(
            out,
            b'{"results":[{"op":"xp","backends":[],"total":{'
            b'"R":{"alerting":0,"raised":0,"cleared":0},'
            b'"M":{"alerting":0,"raised":0,"cleared":0}}}]'
            b',"backends":[]}\n',
        )

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            self.add(), self.add("c"),
            self.mr(retries=5, remaps=2, now=0),
            self.xa(0, 60, k="R"),
            self.xa(0, 60, k="M", hi=3, lo=1),
            self.xp(0, 1, 120),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")


class ReasonTimelineTest(unittest.TestCase):
    """rt 原因事件时刻查询：count/first/last 记账、区间窗序、错误与清除。"""

    FLOW = ["s", 1, "t", 2, "tcp"]

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def rt(backend, start, end, now):
        return {"op": "rt", "id": backend, "from": start, "to": end, "now": now}

    @staticmethod
    def triple(count, first, last):
        return {"count": count, "first": first, "last": last}

    def health_flip(self, backend, now):
        # fail=success=1 后一次失败探测即产生一次 health 事件。
        return [
            {"op": "hset", "id": backend, "fail": 1, "success": 1},
            {"op": "probe", "id": backend, "ok": False, "now": now},
        ]

    def test_first_last_all_four_reasons(self):
        ops = [
            {"op": "add", "id": "h", "weight": 1},
            {"op": "add", "id": "d", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            {"op": "add", "id": "o", "weight": 1},
        ]
        ops += self.health_flip("h", 0)
        ops += [
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            {"op": "ds", "id": "d", "t": 10},
            {"op": "dr", "id": "d", "now": 0},
            {"op": "cs", "id": "c", "n": 1, "m": 1, "r": 1, "w": 1, "q": 1},
            {"op": "cr", "id": "c", "ok": False, "now": 0},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 1, "ttl": 10},
            {"op": "open", "cid": "y", "flow": self.FLOW, "now": 0},
            {"op": "oa", "cid": "z", "flow": self.FLOW, "c": "k",
             "s": "k", "key": "k", "now": 0},
        ]
        for backend in ("h", "d", "c", "o"):
            ops.append(self.rt(backend, 0, 0, 0))
        windows = {r["id"]: r["windows"][0]
                   for r in self.results(ops) if r["op"] == "rt"}
        expected = {
            "h": ("health", 0), "d": ("drain", 0),
            "c": ("circuit", 0), "o": ("overload", 0),
        }
        zero = self.triple(0, None, None)
        for backend, (reason, moment) in expected.items():
            window = windows[backend]
            self.assertEqual(window["window"], 0)
            for name in ("health", "drain", "circuit", "overload"):
                if name == reason:
                    self.assertEqual(window[name], self.triple(1, moment, moment))
                else:
                    self.assertEqual(window[name], zero)

    def test_multiple_same_window_events_update_count_and_last(self):
        # 同窗两次 h→u（中间恢复）：count=2、first 留首次、last 随最近事件。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "hset", "id": "b", "fail": 1, "success": 1},
            {"op": "probe", "id": "b", "ok": False, "now": 10},
            {"op": "probe", "id": "b", "ok": True, "now": 20},
            {"op": "probe", "id": "b", "ok": False, "now": 50},
            self.rt("b", 0, 0, 59),
        ]
        entry = self.results(ops)[-1]["windows"][0]["health"]
        self.assertEqual(entry, self.triple(2, 10, 50))

    def test_circuit_reopen_records_each_transition(self):
        # C→O（10），cg 转 H（11），H 中失败重开 O（12）：两次 circuit。
        ops = [
            {"op": "add", "id": "c", "weight": 1},
            {"op": "cs", "id": "c", "n": 1, "m": 1, "r": 1, "w": 1, "q": 1},
            {"op": "cr", "id": "c", "ok": False, "now": 10},
            {"op": "cg", "id": "c", "now": 11},
            {"op": "cr", "id": "c", "ok": False, "now": 12},
            self.rt("c", 0, 0, 12),
        ]
        entry = self.results(ops)[-1]["windows"][0]["circuit"]
        self.assertEqual(entry, self.triple(2, 10, 12))

    def test_drain_transitions_only(self):
        # dr 记一次；D 态再 dr 无转换不记；du 后再 dr 记第二次。
        ops = [
            {"op": "add", "id": "d", "weight": 1},
            {"op": "open", "cid": "x", "flow": self.FLOW, "now": 0},
            {"op": "ds", "id": "d", "t": 100},
            {"op": "dr", "id": "d", "now": 10},
            {"op": "dr", "id": "d", "now": 11},
            {"op": "du", "id": "d", "now": 12},
            {"op": "dr", "id": "d", "now": 20},
            self.rt("d", 0, 0, 59),
        ]
        entry = self.results(ops)[-1]["windows"][0]["drain"]
        self.assertEqual(entry, self.triple(2, 10, 20))

    def test_overload_each_queue_event(self):
        # cap=1 被占，两次入队 Q 各记一次 overload。
        ops = [
            {"op": "add", "id": "o", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "os", "cap": 1, "q": 2, "ttl": 10},
            {"op": "open", "cid": "y", "flow": self.FLOW, "now": 0},
            {"op": "oa", "cid": "z", "flow": self.FLOW, "c": "k",
             "s": "k", "key": "k", "now": 1},
            {"op": "oa", "cid": "w", "flow": self.FLOW, "c": "j",
             "s": "j", "key": "j", "now": 5},
            self.rt("o", 0, 0, 5),
        ]
        entry = self.results(ops)[-1]["windows"][0]["overload"]
        self.assertEqual(entry, self.triple(2, 1, 5))

    def test_cross_window_closed_range_ascending(self):
        # w0 与 w1 各一次 health；区间闭、按 window 升序，空窗补零三元组。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "hset", "id": "b", "fail": 1, "success": 1},
            {"op": "probe", "id": "b", "ok": False, "now": 10},
            {"op": "probe", "id": "b", "ok": True, "now": 60},
            {"op": "probe", "id": "b", "ok": False, "now": 70},
            self.rt("b", 0, 2, 179),
        ]
        windows = self.results(ops)[-1]["windows"]
        self.assertEqual([w["window"] for w in windows], [0, 1, 2])
        zero = self.triple(0, None, None)
        self.assertEqual(windows[0]["health"], self.triple(1, 10, 10))
        self.assertEqual(windows[1]["health"], self.triple(1, 70, 70))
        self.assertEqual(windows[2]["health"], zero)
        for window in windows:
            self.assertEqual(
                list(window),
                ["window", "health", "drain", "circuit", "overload"],
            )
            for name in ("drain", "circuit", "overload"):
                self.assertEqual(window[name], zero)

    def test_never_evented_backend_is_zero_null_null(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 0),
        ]
        window = self.results(ops)[-1]["windows"][0]
        zero = self.triple(0, None, None)
        for name in ("health", "drain", "circuit", "overload"):
            self.assertEqual(window[name], zero)
            self.assertEqual(list(window[name]), ["count", "first", "last"])

    def test_result_key_order(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 0),
        ]
        results = self.results(ops)
        result = results[-1]
        self.assertEqual(list(result), ["op", "id", "windows"])

    def test_output_byte_layout_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 0),
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        fragment = (
            b'{"op":"rt","id":"b","windows":[{"window":0,'
            b'"health":{"count":0,"first":null,"last":null},'
            b'"drain":{"count":0,"first":null,"last":null},'
            b'"circuit":{"count":0,"first":null,"last":null},'
            b'"overload":{"count":0,"first":null,"last":null}}]}'
        )
        self.assertIn(fragment, out)

    def test_rt_count_matches_rh(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "hset", "id": "b", "fail": 1, "success": 1},
            {"op": "probe", "id": "b", "ok": False, "now": 10},
            {"op": "probe", "id": "b", "ok": True, "now": 20},
            {"op": "probe", "id": "b", "ok": False, "now": 50},
            {"op": "rh", "id": "b", "from": 0, "to": 0, "now": 59},
            self.rt("b", 0, 0, 59),
        ]
        results = self.results(ops)
        rh = next(r for r in results if r["op"] == "rh")
        rt = next(r for r in results if r["op"] == "rt")
        for name in ("health", "drain", "circuit", "overload"):
            self.assertEqual(
                rt["windows"][0][name]["count"], rh["windows"][0][name]
            )

    def test_key_order_and_key_set_is_input(self):
        self.assert_failure(
            b'{"ops":[{"op":"rt","id":"b","to":0,"from":0,"now":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rt","id":"b","from":0,"to":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rt","id":"b","from":0,"to":0,"now":0,"x":1}]}',
            2, "INPUT",
        )

    def test_bad_numbers_are_input(self):
        # 各字段按固定位置插入非法值（bool、负数、超界、小数、串、null）。
        bad_values = ("true", "-1", "1000000001", "1.5", '"0"', "null")
        templates = {
            "from": '{"op":"rt","id":"b","from":%s,"to":0,"now":0}',
            "to": '{"op":"rt","id":"b","from":0,"to":%s,"now":0}',
            "now": '{"op":"rt","id":"b","from":0,"to":0,"now":%s}',
        }
        for field, template in templates.items():
            for value in bad_values:
                raw = ('{"ops":[%s]}' % (template % value)).encode("utf-8")
                self.assert_failure(raw, 2, "INPUT")

    def test_window_relations_are_input(self):
        # from>to、to>now//60、to-from=60 均 INPUT。
        self.assert_failure(
            encode_ops([{"op": "add", "id": "b", "weight": 1},
                        self.rt("b", 1, 0, 0)]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "add", "id": "b", "weight": 1},
                        self.rt("b", 0, 2, 119)]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "add", "id": "b", "weight": 1},
                        self.rt("b", 0, 60, 3600)]),
            2, "INPUT",
        )

    def test_window_relation_boundaries_ok(self):
        # to-from=59 合法；from=to=now//60 合法。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 1, 60, 3600),
            self.rt("b", 16666666, 16666666, 10 ** 9),
        ]
        results = self.results(ops)
        rts = [r for r in results if r["op"] == "rt"]
        self.assertEqual(len(rts[0]["windows"]), 60)
        self.assertEqual(len(rts[1]["windows"]), 1)

    def test_bad_id_is_input(self):
        self.assert_failure(
            b'{"ops":[{"op":"rt","id":"","from":0,"to":0,"now":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rt","id":7,"from":0,"to":0,"now":0}]}',
            2, "INPUT",
        )

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.rt("x", 0, 0, 0)]), 3, "BACKEND"
        )

    def test_unknown_id_with_stale_from_is_backend(self):
        # 未知 id 优先于 from 过旧：BACKEND。
        self.assert_failure(
            encode_ops([self.rt("x", 0, 0, 3600)]), 3, "BACKEND"
        )

    def test_unknown_id_with_bad_relation_is_input(self):
        # 数值关系在解析期判定，优先于未知 id：INPUT。
        self.assert_failure(
            encode_ops([self.rt("x", 1, 0, 0)]), 2, "INPUT"
        )

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "probe", "id": "b", "ok": True, "now": 5},
            self.rt("b", 0, 0, 0),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_stale_window_is_state(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 3600),
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        # 下界 max(0, now//60-59)=1 合法。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 1, 1, 3600),
        ])
        self.assertEqual(results[-1]["windows"][0]["window"], 1)

    def test_retains_last_60_windows(self):
        # w1 事件在 now=3600（w60）仍可查，w0 已不可查。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
        ] + self.health_flip("b", 60) + [
            {"op": "probe", "id": "b", "ok": True, "now": 119},
            self.rt("b", 1, 60, 3600),
        ]
        windows = self.results(ops)[-1]["windows"]
        self.assertEqual([w["window"] for w in windows], list(range(1, 61)))
        self.assertEqual(windows[0]["health"], self.triple(1, 60, 60))
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.rt("b", 0, 0, 3600),
            ]),
            4, "STATE",
        )

    def test_remove_readd_clears_timeline(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
        ] + self.health_flip("b", 0) + [
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 0),
        ]
        window = self.results(ops)[-1]["windows"][0]
        self.assertEqual(window["health"], self.triple(0, None, None))

    @staticmethod
    def v1_config():
        return {
            "version": 1,
            "backends": [
                {"id": "b", "weight": 1, "d": 0, "fail": 3, "success": 2,
                 "circuit": None, "drain": None}
            ],
            "vnodes": None,
            "limits": [],
            "overload": None,
        }

    def test_ci_success_clears_timeline(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
        ] + self.health_flip("b", 0) + [
            {"op": "ci", "config": self.v1_config(), "now": 60},
            self.rt("b", 1, 1, 60),
        ]
        window = self.results(ops)[-1]["windows"][0]
        self.assertEqual(window["health"], self.triple(0, None, None))

    def test_cb_success_clears_timeline(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
        ] + self.health_flip("b", 0) + [
            {"op": "ci", "config": self.v1_config(), "now": 60},
        ] + self.health_flip("b", 120) + [
            {"op": "cb", "rev": 1, "now": 180},
            self.rt("b", 3, 3, 180),
        ]
        window = self.results(ops)[-1]["windows"][0]
        self.assertEqual(window["health"], self.triple(0, None, None))

    def test_rt_is_read_only(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
        ] + self.health_flip("b", 10) + [
            self.rt("b", 0, 0, 59),
            self.rt("b", 0, 0, 59),
            {"op": "rh", "id": "b", "from": 0, "to": 0, "now": 59},
        ]
        results = self.results(ops)
        rts = [r for r in results if r["op"] == "rt"]
        self.assertEqual(rts[0], rts[1])
        rh = next(r for r in results if r["op"] == "rh")
        self.assertEqual(rts[0]["windows"][0]["health"]["count"],
                         rh["windows"][0]["health"])

    def test_failed_rt_has_no_output(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 3600),
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (4, b""))
        self.assertEqual(stderr, b'{"error":"STATE"}\n')

    def test_failure_after_rt_rolls_back_batch(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 5),
            {"op": "probe", "id": "b", "ok": "notabool", "now": 6},
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (2, b""))
        self.assertEqual(stderr, b'{"error":"INPUT"}\n')

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            {"op": "hset", "id": "b", "fail": 1, "success": 1},
            {"op": "probe", "id": "b", "ok": False, "now": 3},
            {"op": "probe", "id": "b", "ok": True, "now": 4},
            {"op": "probe", "id": "b", "ok": False, "now": 66},
            self.rt("b", 0, 1, 66),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")

    def test_record_replay_covers_failing_rt(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.rt("b", 0, 0, 3600),
        ])
        _, rec_stdout, _ = run_balancer("record", raw)
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual(record["exit"], 4)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, 4)
        self.assertEqual(rep_stdout, b"")
        self.assertEqual(rep_stderr, b'{"error":"STATE"}\n')


class RetryTimelineTest(unittest.TestCase):
    """rr 重试/重映射时刻历史：mr 与 fx/fr 自动度量记账、区间窗序、错误与
    清除。"""

    FLOW = ["s", 1, "t", 2, "tcp"]

    def results(self, ops):
        code, out, err = run_balancer("run", encode_ops(ops))
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

    @staticmethod
    def rr(backend, start, end, now):
        return {"op": "rr", "id": backend, "from": start, "to": end,
                "now": now}

    @staticmethod
    def mr(backend, ok, ms, retries, remaps, now):
        return {"op": "mr", "id": backend, "ok": ok, "ms": ms,
                "retries": retries, "remaps": remaps, "now": now}

    @staticmethod
    def triple(count, first, last):
        return {"count": count, "first": first, "last": last}

    def hash_key_for(self, target, ids):
        """vnodes=1 下取首个哈希落点为 target 的 key（镜像环令牌规则）。"""
        tokens = sorted(
            (
                int.from_bytes(
                    hashlib.sha256(
                        i.encode("utf-8") + b"\x00" + b"0"
                    ).digest(),
                    "big",
                ),
                i,
            )
            for i in ids
        )
        digests = [token[0] for token in tokens]
        for n in range(10000):
            key = "k%d" % n
            key_hash = int.from_bytes(
                hashlib.sha256(key.encode("utf-8")).digest(), "big"
            )
            idx = bisect.bisect_left(digests, key_hash) % len(tokens)
            if tokens[idx][1] == target:
                return key
        self.fail("no hash key for %r" % target)

    def test_mr_positive_records_count_first_last(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 5, 2, 3, 10),
            self.rr("b", 0, 0, 59),
        ]
        window = self.results(ops)[-1]["windows"][0]
        self.assertEqual(window["retries"], self.triple(2, 10, 10))
        self.assertEqual(window["remaps"], self.triple(3, 10, 10))

    def test_zero_values_not_recorded(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 5, 0, 0, 10),
            self.rr("b", 0, 0, 59),
        ]
        window = self.results(ops)[-1]["windows"][0]
        zero = self.triple(0, None, None)
        self.assertEqual(window["retries"], zero)
        self.assertEqual(window["remaps"], zero)

    def test_last_updates_first_stable_same_window(self):
        # 同窗三次写入：count 累加，first 留首次 now，last 随最近 now。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 1, 0, 10),
            self.mr("b", True, 1, 2, 0, 20),
            self.mr("b", True, 1, 3, 0, 50),
            self.rr("b", 0, 0, 59),
        ]
        window = self.results(ops)[-1]["windows"][0]
        self.assertEqual(window["retries"], self.triple(6, 10, 50))
        # remaps 始终零：不记。
        self.assertEqual(window["remaps"], self.triple(0, None, None))

    def test_retries_and_remaps_tracked_independently(self):
        # 仅其中一项为正时另一项不建 first/last。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 0, 4, 10),
            self.mr("b", False, 1, 5, 0, 20),
            self.rr("b", 0, 0, 59),
        ]
        window = self.results(ops)[-1]["windows"][0]
        self.assertEqual(window["retries"], self.triple(5, 20, 20))
        self.assertEqual(window["remaps"], self.triple(4, 10, 10))

    def test_count_matches_mh(self):
        # rr 同窗 count 恒等于 mh 同窗 retries/remaps 累计。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 4, 6, 0),
            self.mr("b", False, 1, 0, 2, 30),
            self.mr("b", True, 1, 7, 0, 65),
            {"op": "mh", "id": "b", "from": 0, "to": 1, "now": 119},
            self.rr("b", 0, 1, 119),
        ]
        results = self.results(ops)
        mh = next(r for r in results if r["op"] == "mh")
        rr = next(r for r in results if r["op"] == "rr")
        for window in range(2):
            self.assertEqual(
                rr["windows"][window]["retries"]["count"],
                mh["windows"][window]["retries"],
            )
            self.assertEqual(
                rr["windows"][window]["remaps"]["count"],
                mh["windows"][window]["remaps"],
            )
        self.assertEqual(rr["windows"][1]["retries"],
                         self.triple(7, 65, 65))
        self.assertEqual(rr["windows"][1]["remaps"],
                         self.triple(0, None, None))

    def test_cross_window_closed_range_ascending(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 1, 0, 10),
            self.mr("b", True, 1, 0, 4, 50),
            self.mr("b", True, 1, 2, 0, 65),
            self.mr("b", True, 1, 3, 1, 125),
            self.rr("b", 0, 2, 179),
        ]
        windows = self.results(ops)[-1]["windows"]
        self.assertEqual([w["window"] for w in windows], [0, 1, 2])
        zero = self.triple(0, None, None)
        self.assertEqual(windows[0]["retries"], self.triple(1, 10, 10))
        self.assertEqual(windows[0]["remaps"], self.triple(4, 50, 50))
        self.assertEqual(windows[1]["retries"], self.triple(2, 65, 65))
        self.assertEqual(windows[1]["remaps"], zero)
        self.assertEqual(windows[2]["retries"], self.triple(3, 125, 125))
        self.assertEqual(windows[2]["remaps"], self.triple(1, 125, 125))
        for window in windows:
            self.assertEqual(list(window), ["window", "retries", "remaps"])
            self.assertEqual(
                list(window["retries"]), ["count", "first", "last"]
            )

    def test_never_written_is_zero_null_null(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 0),
        ]
        window = self.results(ops)[-1]["windows"][0]
        zero = self.triple(0, None, None)
        self.assertEqual(window["retries"], zero)
        self.assertEqual(window["remaps"], zero)

    def test_result_key_order(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 0),
        ]
        result = self.results(ops)[-1]
        self.assertEqual(list(result), ["op", "id", "windows"])

    def test_output_byte_layout_and_single_newline(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 0),
        ])
        _, out, _ = run_balancer("run", raw)
        self.assertTrue(out.endswith(b"}\n") and out.count(b"\n") == 1)
        fragment = (
            b'{"op":"rr","id":"b","windows":[{"window":0,'
            b'"retries":{"count":0,"first":null,"last":null},'
            b'"remaps":{"count":0,"first":null,"last":null}}]}'
        )
        self.assertIn(fragment, out)

    def test_fx_auto_metric_records_timeline(self):
        # a 在 D 段，fx 自 a 重映射至 b：自动度量只归属结果 b，retries=
        # remaps=1；a 无度量。
        key = self.hash_key_for("a", ["a", "b"])
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "add", "id": "b", "weight": 1},
            {"op": "chash", "vnodes": 1},
            {"op": "fs", "id": "a", "k": "D", "a": 0, "z": 100, "v": 0},
            {"op": "fx", "cid": "c", "flow": self.FLOW, "key": key,
             "timeout": 10, "now": 5},
            self.rr("b", 0, 0, 59),
            self.rr("a", 0, 0, 59),
        ]
        rrs = {r["id"]: r["windows"][0]
               for r in self.results(ops) if r["op"] == "rr"}
        self.assertEqual(rrs["b"]["retries"], self.triple(1, 5, 5))
        self.assertEqual(rrs["b"]["remaps"], self.triple(1, 5, 5))
        zero = self.triple(0, None, None)
        self.assertEqual(rrs["a"]["retries"], zero)
        self.assertEqual(rrs["a"]["remaps"], zero)

    def test_fr_auto_metrics_record_timeline(self):
        # 三后端皆 D，fr 耗尽 3 次拒绝：首项 a 记总 retries=remaps=2，
        # 余项 b 记 0。
        key = self.hash_key_for("a", ["a", "b", "c"])
        ops = [
            {"op": "add", "id": "a", "weight": 1},
            {"op": "add", "id": "b", "weight": 1},
            {"op": "add", "id": "c", "weight": 1},
            {"op": "chash", "vnodes": 1},
        ]
        for backend in ("a", "b", "c"):
            ops.append(
                {"op": "fs", "id": backend, "k": "D", "a": 0, "z": 100,
                 "v": 0}
            )
        ops += [
            {"op": "fr", "cid": "c", "flow": self.FLOW, "key": key,
             "timeout": 10, "max": 3, "now": 5},
            self.rr("a", 0, 0, 59),
            self.rr("b", 0, 0, 59),
        ]
        rrs = {r["id"]: r["windows"][0]
               for r in self.results(ops) if r["op"] == "rr"}
        self.assertEqual(rrs["a"]["retries"], self.triple(2, 5, 5))
        self.assertEqual(rrs["a"]["remaps"], self.triple(2, 5, 5))
        zero = self.triple(0, None, None)
        self.assertEqual(rrs["b"]["retries"], zero)
        self.assertEqual(rrs["b"]["remaps"], zero)

    def test_key_order_and_key_set_is_input(self):
        self.assert_failure(
            b'{"ops":[{"op":"rr","id":"b","to":0,"from":0,"now":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rr","id":"b","from":0,"to":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rr","id":"b","from":0,"to":0,"now":0,"x":1}]}',
            2, "INPUT",
        )

    def test_bad_numbers_are_input(self):
        bad_values = ("true", "-1", "1000000001", "1.5", '"0"', "null")
        templates = {
            "from": '{"op":"rr","id":"b","from":%s,"to":0,"now":0}',
            "to": '{"op":"rr","id":"b","from":0,"to":%s,"now":0}',
            "now": '{"op":"rr","id":"b","from":0,"to":0,"now":%s}',
        }
        for field, template in templates.items():
            for value in bad_values:
                raw = ('{"ops":[%s]}' % (template % value)).encode("utf-8")
                self.assert_failure(raw, 2, "INPUT")

    def test_window_relations_are_input(self):
        self.assert_failure(
            encode_ops([{"op": "add", "id": "b", "weight": 1},
                        self.rr("b", 1, 0, 0)]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "add", "id": "b", "weight": 1},
                        self.rr("b", 0, 2, 119)]),
            2, "INPUT",
        )
        self.assert_failure(
            encode_ops([{"op": "add", "id": "b", "weight": 1},
                        self.rr("b", 0, 60, 3600)]),
            2, "INPUT",
        )

    def test_window_relation_boundaries_ok(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 1, 60, 3600),
            self.rr("b", 16666666, 16666666, 10 ** 9),
        ]
        rrs = [r for r in self.results(ops) if r["op"] == "rr"]
        self.assertEqual(len(rrs[0]["windows"]), 60)
        self.assertEqual(len(rrs[1]["windows"]), 1)

    def test_bad_id_is_input(self):
        self.assert_failure(
            b'{"ops":[{"op":"rr","id":"","from":0,"to":0,"now":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rr","id":7,"from":0,"to":0,"now":0}]}',
            2, "INPUT",
        )
        self.assert_failure(
            b'{"ops":[{"op":"rr","id":"\\ud800","from":0,"to":0,"now":0}]}',
            2, "INPUT",
        )

    def test_unknown_id_is_backend(self):
        self.assert_failure(
            encode_ops([self.rr("x", 0, 0, 0)]), 3, "BACKEND"
        )

    def test_unknown_id_with_stale_from_is_backend(self):
        self.assert_failure(
            encode_ops([self.rr("x", 0, 0, 3600)]), 3, "BACKEND"
        )

    def test_unknown_id_with_bad_relation_is_input(self):
        self.assert_failure(
            encode_ops([self.rr("x", 1, 0, 0)]), 2, "INPUT"
        )

    def test_clock_regression_is_input(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 1, 1, 100),
            self.rr("b", 1, 1, 59),
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")

    def test_rr_advances_clock(self):
        # rr 推进时钟：rr(now=100) 后更早时刻的 mg(now=99) 报时钟倒退。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 1, 100),
            {"op": "mg", "id": "b", "now": 99},
        ]
        self.assert_failure(encode_ops(ops), 2, "INPUT")
        # 同刻或更晚仍可。
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 1, 100),
            {"op": "mg", "id": "b", "now": 100},
        ])
        self.assertEqual(results[-1]["op"], "mg")

    def test_stale_window_is_state(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 3600),
        ]
        self.assert_failure(encode_ops(ops), 4, "STATE")
        results = self.results([
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 1, 1, 3600),
        ])
        self.assertEqual(results[-1]["windows"][0]["window"], 1)

    def test_retains_last_60_windows(self):
        # w0 写入在 now=3600（w60）仍保留 w1..w60，w0 已淘汰不可查。
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 1, 1, 60),
            self.rr("b", 1, 60, 3600),
        ]
        windows = self.results(ops)[-1]["windows"]
        self.assertEqual([w["window"] for w in windows], list(range(1, 61)))
        self.assertEqual(windows[0]["retries"], self.triple(1, 60, 60))
        self.assert_failure(
            encode_ops([
                {"op": "add", "id": "b", "weight": 1},
                self.rr("b", 0, 0, 3600),
            ]),
            4, "STATE",
        )

    def test_remove_readd_clears_timeline(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 3, 2, 0),
            {"op": "remove", "id": "b"},
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 0),
        ]
        window = self.results(ops)[-1]["windows"][0]
        zero = self.triple(0, None, None)
        self.assertEqual(window["retries"], zero)
        self.assertEqual(window["remaps"], zero)

    @staticmethod
    def v1_config():
        return {
            "version": 1,
            "backends": [
                {"id": "b", "weight": 1, "d": 0, "fail": 3, "success": 2,
                 "circuit": None, "drain": None}
            ],
            "vnodes": None,
            "limits": [],
            "overload": None,
        }

    def test_ci_success_clears_timeline(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 3, 2, 0),
            {"op": "ci", "config": self.v1_config(), "now": 60},
            self.rr("b", 1, 1, 60),
        ]
        window = self.results(ops)[-1]["windows"][0]
        zero = self.triple(0, None, None)
        self.assertEqual(window["retries"], zero)
        self.assertEqual(window["remaps"], zero)

    def test_cb_success_clears_timeline(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            {"op": "ci", "config": self.v1_config(), "now": 60},
            self.mr("b", True, 1, 3, 2, 120),
            {"op": "cb", "rev": 1, "now": 180},
            self.rr("b", 3, 3, 180),
        ]
        window = self.results(ops)[-1]["windows"][0]
        zero = self.triple(0, None, None)
        self.assertEqual(window["retries"], zero)
        self.assertEqual(window["remaps"], zero)

    def test_rr_is_read_only(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 2, 3, 10),
            self.rr("b", 0, 0, 59),
            self.rr("b", 0, 0, 59),
        ]
        results = self.results(ops)
        rrs = [r for r in results if r["op"] == "rr"]
        self.assertEqual(rrs[0], rrs[1])

    def test_failure_after_rr_rolls_back_batch(self):
        ops = [
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 5),
            {"op": "probe", "id": "b", "ok": "notabool", "now": 6},
        ]
        code, stdout, stderr = run_balancer("run", encode_ops(ops))
        self.assertEqual((code, stdout), (2, b""))
        self.assertEqual(stderr, b'{"error":"INPUT"}\n')

    def test_record_replay_byte_identical(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.mr("b", True, 1, 2, 3, 3),
            self.mr("b", False, 1, 1, 0, 66),
            self.rr("b", 0, 1, 66),
        ])
        run_code, run_stdout, _ = run_balancer("run", raw)
        rec_code, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual((rec_code, rec_stderr), (0, b""))
        self.assertEqual(rep_code, run_code)
        self.assertEqual(rep_code, record["exit"])
        self.assertEqual(rep_stdout, run_stdout)
        self.assertEqual(rep_stdout, base64.b64decode(record["stdout"]))
        self.assertEqual(rep_stderr, b"")

    def test_record_replay_covers_failing_rr(self):
        raw = encode_ops([
            {"op": "add", "id": "b", "weight": 1},
            self.rr("b", 0, 0, 3600),
        ])
        _, rec_stdout, _ = run_balancer("record", raw)
        record = json.loads(rec_stdout.decode("utf-8"))
        self.assertEqual(record["exit"], 4)
        rep_code, rep_stdout, rep_stderr = run_balancer(
            "replay", rec_stdout
        )
        self.assertEqual(rep_code, 4)
        self.assertEqual(rep_stdout, b"")
        self.assertEqual(rep_stderr, b'{"error":"STATE"}\n')


class AuditCursorTest(unittest.TestCase):
    """审计游标查询 ai：分页、truncated、STATE/INPUT 与只读契约。"""

    def ai(self, after, limit):
        return {"op": "ai", "after": after, "limit": limit}

    def commits(self, count, start=1):
        # 每次 ci 成功分配一个新 rev 并追加一条 ci 审计事件。
        return [
            {"op": "ci", "config": config_v6(i), "now": i}
            for i in range(start, start + count)
        ]

    def run_ops(self, ops):
        return run_balancer("run", encode_ops(ops))

    def result(self, ops):
        code, stdout, stderr = self.run_ops(ops)
        self.assertEqual((code, stderr), (0, b""))
        return json.loads(stdout.decode("utf-8"))["results"][-1]

    def test_empty_history_after_zero(self):
        # 空历史的 after=0 查询返回 0,false,false,[]，键序固定。
        r = self.result([self.ai(0, 64)])
        self.assertEqual(
            list(r),
            ["op", "after", "next", "truncated", "more", "events"],
        )
        self.assertEqual(
            r,
            {
                "op": "ai",
                "after": 0,
                "next": 0,
                "truncated": False,
                "more": False,
                "events": [],
            },
        )

    def test_empty_history_after_positive_is_state(self):
        # latest 初始为 0；任何 after>0 都指向尚未分配的 rev。
        code, stdout, stderr = self.run_ops([self.ai(1, 1)])
        self.assertEqual((code, stdout, stderr), (4, b"", b'{"error":"STATE"}\n'))

    def test_pages_events_after_cursor_with_limit(self):
        ops = self.commits(5)
        r = self.result(ops + [self.ai(0, 2)])
        self.assertEqual([e["rev"] for e in r["events"]], [1, 2])
        self.assertEqual((r["after"], r["next"], r["more"], r["truncated"]),
                         (0, 2, True, False))
        # 事件项键序与值义沿用 al。
        event = r["events"][0]
        self.assertEqual(
            list(event),
            ["rev", "now", "kind", "section", "before", "after"],
        )
        self.assertEqual(
            (event["now"], event["kind"], event["section"]),
            (1, "ci", None),
        )
        # 以 next 续页，末页不足 limit 时 more=false；after=latest 返回空。
        r = self.result(ops + [self.ai(2, 2)])
        self.assertEqual([e["rev"] for e in r["events"]], [3, 4])
        self.assertEqual((r["next"], r["more"]), (4, True))
        r = self.result(ops + [self.ai(4, 2)])
        self.assertEqual([e["rev"] for e in r["events"]], [5])
        self.assertEqual((r["next"], r["more"]), (5, False))
        r = self.result(ops + [self.ai(5, 2)])
        self.assertEqual((r["events"], r["next"], r["more"]), ([], 5, False))

    def test_after_equal_latest_ok_beyond_is_state(self):
        ops = self.commits(5)
        self.assertEqual(self.result(ops + [self.ai(5, 1)])["events"], [])
        code, stdout, stderr = self.run_ops(ops + [self.ai(6, 1)])
        self.assertEqual((code, stdout, stderr), (4, b"", b'{"error":"STATE"}\n'))

    def test_truncated_window_starts_at_oldest_retained(self):
        # 70 次变更后仅保留 rev 7..70（maxlen 64）。after=0 < 7-1：截断。
        ops = self.commits(70)
        r = self.result(ops + [self.ai(0, 64)])
        self.assertTrue(r["truncated"])
        self.assertEqual([e["rev"] for e in r["events"]], list(range(7, 71)))
        self.assertEqual((r["next"], r["more"]), (70, False))
        # after=最旧 rev-1=6 时缺口恰为淘汰区，不截断；after=5 才截断。
        r = self.result(ops + [self.ai(6, 64)])
        self.assertFalse(r["truncated"])
        self.assertEqual([e["rev"] for e in r["events"]], list(range(7, 71)))
        r = self.result(ops + [self.ai(5, 64)])
        self.assertTrue(r["truncated"])
        self.assertEqual([e["rev"] for e in r["events"]][0], 7)
        # 截断窗口同样分页：truncated 恒为 true，more 随页后是否有事件。
        r = self.result(ops + [self.ai(0, 10)])
        self.assertEqual([e["rev"] for e in r["events"]], list(range(7, 17)))
        self.assertEqual((r["next"], r["more"], r["truncated"]),
                         (16, True, True))

    def test_untruncated_paging_inside_window(self):
        ops = self.commits(70)
        r = self.result(ops + [self.ai(30, 5)])
        self.assertFalse(r["truncated"])
        self.assertEqual([e["rev"] for e in r["events"]], [31, 32, 33, 34, 35])
        self.assertEqual((r["next"], r["more"]), (35, True))
        # 恰好取满末页：more=false。
        r = self.result(ops + [self.ai(65, 5)])
        self.assertEqual([e["rev"] for e in r["events"]], [66, 67, 68, 69, 70])
        self.assertFalse(r["more"])

    def test_cb_and_cu_events_share_cursor(self):
        ops = self.commits(2)
        ops.append({"op": "cb", "rev": 1, "now": 2})
        r = self.result(ops + [self.ai(0, 64)])
        self.assertEqual(
            [(e["rev"], e["kind"], e["section"]) for e in r["events"]],
            [(1, "ci", None), (2, "ci", None), (3, "cb", None)],
        )

    def test_al_unchanged_and_matches_ai_events(self):
        ops = self.commits(3)
        code, stdout, stderr = self.run_ops(ops + [{"op": "al"}])
        self.assertEqual((code, stderr), (0, b""))
        al = json.loads(stdout.decode("utf-8"))["results"][-1]
        self.assertEqual(list(al), ["op", "events"])
        ai_events = self.result(ops + [self.ai(0, 64)])["events"]
        self.assertEqual(al["events"], ai_events)

    def test_read_only_does_not_observe_or_move_clock(self):
        # 同一批中 ai 只反映此前已提交的 rev；重复查询逐字节相同。
        ops = self.commits(2)
        code, stdout, stderr = self.run_ops(
            ops + [self.ai(0, 64), self.ai(0, 64)]
        )
        self.assertEqual((code, stderr), (0, b""))
        results = json.loads(stdout.decode("utf-8"))["results"]
        self.assertEqual(results[-2], results[-1])
        self.assertEqual([e["rev"] for e in results[-1]["events"]], [1, 2])

    def test_invalid_shapes_are_input(self):
        bad_raw = [
            b'{"ops":[{"op":"ai"}]}',
            b'{"ops":[{"op":"ai","after":0}]}',
            b'{"ops":[{"op":"ai","limit":1}]}',
            b'{"ops":[{"op":"ai","after":0,"limit":1,"x":1}]}',
            b'{"ops":[{"op":"ai","limit":1,"after":0}]}',
            b'{"ops":[{"op":"ai","after":-1,"limit":1}]}',
            b'{"ops":[{"op":"ai","after":1000000000000000001,"limit":1}]}',
            b'{"ops":[{"op":"ai","after":true,"limit":1}]}',
            b'{"ops":[{"op":"ai","after":0,"limit":0}]}',
            b'{"ops":[{"op":"ai","after":0,"limit":65}]}',
            b'{"ops":[{"op":"ai","after":0,"limit":false}]}',
            b'{"ops":[{"op":"ai","after":0.0,"limit":1}]}',
            b'{"ops":[{"op":"ai","after":"0","limit":1}]}',
            b'{"ops":[{"op":"ai","after":0,"after":0,"limit":1}]}',
        ]
        for raw in bad_raw:
            code, stdout, stderr = run_balancer("run", raw)
            self.assertEqual(
                (code, stdout, stderr),
                (2, b"", b'{"error":"INPUT"}\n'),
                raw,
            )

    def test_boundary_after_values(self):
        # after=10^18 在空历史上越 latest 报 STATE；after=0 边界合法。
        code, _, _ = self.run_ops([self.ai(10 ** 18, 1)])
        self.assertEqual(code, 4)
        code, _, _ = self.run_ops([self.ai(0, 1)])
        self.assertEqual(code, 0)

    def test_failure_rolls_back_whole_batch(self):
        ops = self.commits(2)
        code, stdout, stderr = self.run_ops(ops + [self.ai(100, 1)])
        self.assertEqual((code, stdout, stderr), (4, b"", b'{"error":"STATE"}\n'))
        code, stdout, stderr = self.run_ops(
            ops + [{"op": "ai", "after": 0, "limit": 0}]
        )
        self.assertEqual((code, stdout, stderr), (2, b"", b'{"error":"INPUT"}\n'))

    def test_record_replay_byte_identical(self):
        raw = encode_ops(self.commits(70) + [self.ai(0, 10)])
        run_code, run_stdout, _ = run_balancer("run", raw)
        _, rec_stdout, rec_stderr = run_balancer("record", raw)
        rep_code, rep_stdout, rep_stderr = run_balancer("replay", rec_stdout)
        self.assertEqual(rep_stderr, b"")
        self.assertEqual((run_code, rec_stderr), (0, b""))
        self.assertEqual((rep_code, rep_stdout), (run_code, run_stdout))


if __name__ == "__main__":
    unittest.main()
