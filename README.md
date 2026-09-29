# balancer

从零实现的四层负载均衡与后端池管理框架，仅用 Python 标准库、不联网。

- 入口：`python balancer.py <子命令>`
- 所有超时与健康探测必须由显式时钟驱动；相同请求序列必须产生逐字节相同的调度与连接决策。
- 调度、连接与统计结果统一写成 JSON，浮点数按固定小数位格式化。

## 配置变更审计：al / ai

成功的配置变更（ci/cb/cu/ca）每分配一个新 rev 即追加一条审计事件，事件
只按 rev 升序保留最近 64 条；rev 从 1 起递增、只增不复用，初始无事件。
事件项固定键序 `rev,now,kind,section,before,after`：rev 为本次新 rev，
now 为该操作的显式时钟，kind ∈ ci/cb/cu/ca，section 仅 cu 取被替换的顶层
字段名、其余为 null，before/after 为操作前后规范化 version=11 配置的 ct
摘要（小写 64 位十六进制；值未变两摘要相同仍记录）。

- `al`：精确键序仅 `op`，只读、不推进时钟。返回键序 `op,events`；events
  为当前保留事件（至多 64 条，按 rev 升序）的逐项全新快照，重复查询逐字节
  相同。时间 O(A)，A≤64。
- `ai`：精确键序 `op,after,limit`（键须按此序出现）。after 为
  0..10^18、limit 为 1..64 的非 bool 整数；只读、不推进时钟。令 latest
  为已分配最大 rev（初始 0），`after>latest` 报 STATE/4。复用 al 保留的
  最近 64 条事件：历史非空且 after 小于最旧 rev 减 1 时，游标之前的事件
  已被淘汰、不可再读，置 truncated=true 并自最旧事件读取；否则取
  rev>after 的事件；至多返回 limit 条。结果键序
  `op,after,next,truncated,more,events`：next 为末条事件 rev，无条目时
  等于 after；truncated、more 为 bool，more 表示保留事件中本页之后是否
  尚有可读事件；events 项键序与值义沿用 al。空历史的 after=0 查询返回
  next=0、truncated=false、more=false、events=[]。键序、类型或范围非法
  报 INPUT/2。时间 O(A)、额外空间 O(limit)，A≤64。

两者均为只读：不推进共用非递减时钟、不修改任何状态，失败批次回滚；输出
沿用紧凑 UTF-8 固定键序 JSON 与单末尾换行，record/replay 逐字节契约不变，
仅用标准库。

## 测试

    python -m unittest discover
