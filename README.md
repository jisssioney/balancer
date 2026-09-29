# balancer

从零实现的四层负载均衡与后端池管理框架，仅用 Python 标准库、不联网。

- 入口：`python balancer.py <子命令>`
- 所有超时与健康探测必须由显式时钟驱动；相同请求序列必须产生逐字节相同的调度与连接决策。
- 调度、连接与统计结果统一写成 JSON，浮点数按固定小数位格式化。

## 配置变更审计：al / ai

配置变更（`ci`/`cb`/`cu`/`ca`）成功并分配新 rev 时追加一条审计事件；`rev` 从 1 起递增，事件按 rev 升序仅保留最近 64 条，超额淘汰最旧项。两查询均只读、不推进时钟，失败批次回滚。

- `al`：精确键序仅 `op`，返回键序 `op,events`，原样列出当前保留的全部事件。
- `ai`：精确键序 `op,after,limit`。`after` 为 0..10^18、`limit` 为 1..64 的非 bool 整数；键序、类型或范围非法报 INPUT/2。令 `latest` 为已分配最大 rev（初始 0），`after>latest` 报 STATE/4。
  - 历史非空且 `after` 小于最旧事件 rev 减 1 时，游标之前已有事件被淘汰：`truncated=true` 并自最旧事件读取；否则 `truncated=false`，仅取 `rev>after` 的事件。
  - 至多返回 `limit` 条；`next` 为末条事件 rev，无条目时等于 `after`；`more` 表示该页之后是否仍有可读事件。
  - 返回键序 `op,after,next,truncated,more,events`；空历史的 `after=0` 查询返回 `next=0,truncated=false,more=false,events=[]`。
- 事件项键序 `rev,now,kind,section,before,after`，两查询值义相同：`now` 为该变更操作的显式时钟，`kind` ∈ ci/cb/cu/ca，`section` 仅 `cu` 取被替换的顶层字段、其余为 null，`before`/`after` 为变更前后规范化 version=11 配置的 ct 摘要。
- `ai` 时间 O(A)、额外空间 O(limit)，A≤64，仅用标准库；紧凑 UTF-8 固定键序 JSON、单换行及 record/replay 逐字节契约不变。

## 测试

    python -m unittest discover
