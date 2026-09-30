# balancer

从零实现的四层负载均衡与后端池管理框架，仅用 Python 标准库、不联网。

- 入口：`python balancer.py <子命令>`
- 所有超时与健康探测必须由显式时钟驱动；相同请求序列必须产生逐字节相同的调度与连接决策。
- 调度、连接与统计结果统一写成 JSON，浮点数按固定小数位格式化。

## 故障时间线预演：ft

对同一 `key` 在 `times` 各时刻独立模拟 `fr` 的候选资格、哈希环与 D/F/S 规则，只读预演各时刻的重试结果。

- `ft`：精确键序 `op,key,times,timeout,max,now`。`key` 沿用 `route` 校验，`timeout`/`max`/`now` 沿用 `fr`（`now` 为 0..10⁹ 非 bool 整数，纳入共用非递减时钟）；`times` 为 1..60 项严格递增数组，项为 `now`..10⁹ 非 bool 整数。
- 返回键序 `op,key,now,cases`；`cases` 按 `times` 排列，项键序 `at,state,backend,attempts,retries,remaps,latency`，其中 `state`/`backend`/`attempts`/`latency` 同 `fi`，`retries=remaps=max(attempts-1,0)`。
- 键序、字段类型/范围/编码、`times` 容器/项数/次序或时钟倒退报 INPUT/2；未配置环或无合格候选报 STATE/4。仅 `now` 推进时钟，不改连接、粘性、指标、告警或故障状态，失败批回滚。时间 O(PBV)、空间 O(P+BV)（P 为 `times` 项数）。

## 故障差异预演：fd

按 `times` 外层、`keys` 内层，对每个 (时刻, 键) 分别用当前故障时间线与候选计划独立模拟 `fr` 的候选资格、哈希环与 D/F/S 规则，只读比较前后结果。

- `fd`：精确键序 `op,items,keys,times,timeout,max,now`。`items` 为 `fp` 同款全量候选计划（列入 id 的时间线原子替换、未列入的保持原样）；`keys` 为 1..64 项 `route` 键数组，可重复；`times` 为 1..60 项严格递增数组，项为 `now`..10⁹ 非 bool 整数；`timeout`/`max`/`now` 沿用 `fr`（`now` 纳入共用非递减时钟）。
- 返回键序 `op,now,cases,summary`；`cases` 项键序 `at,key,before,after,changed`，`before`/`after` 键序均为 `state,backend,attempts,retries,remaps,latency` 且值义同 `ft`，不同则 `changed=true`。`summary` 键序 `total,stable,changed`，三值为非负整数：cases 数、未变数、变化数。
- 键序、字段类型/范围/编码、容器/项数/次序或时钟倒退报 INPUT/2；候选引用未知后端报 BACKEND/3；未配置环或无合格候选报 STATE/4。仅 `now` 推进时钟，不改状态，失败批回滚。时间 O(T+PKBV)、空间 O(T+PK+BV)（T/P/K 对应 `items`/`times`/`keys` 项数），仅用标准库。

## 故障候选单键预演：fc

对单个 `key` 在 `at` 时刻分别用当前故障时间线与候选计划独立模拟一次 `fr` 的候选资格、哈希环与 D/F/S 规则，只读比较前后结果并按尝试序给出明细。

- `fc`：精确键序 `op,items,key,at,timeout,max,now`。`items` 为 `fp` 同款全量候选计划（列入 id 的时间线原子替换、未列入的保持原样，按 id 分组与基数排序合计 O(T) 解析）；`key` 沿用 `route` 校验；`timeout`/`max`/`now` 沿用 `fr`（`now` 为 0..10⁹ 非 bool 整数，纳入共用非递减时钟）；`at` 为 0..10⁹ 非 bool 整数且 `at≥now`，仅作只读模拟时刻、不进时钟。
- 返回键序 `op,key,at,before,after,changed`；`before`/`after` 键序均为 `state,backend,attempts,retries,remaps,latency,trace`，前六项义同 `ft`；`trace` 按尝试序，项键序 `id,kind,cost,result`：`kind` 为该候选 `at` 时刻活动段的登记种类（`D`/`F`/`S`），段间隙或未登记为 null（F 非故障相位仍记 `F`）；`cost` 为计入 `latency` 的整数（D/F 故障相位 0，S 慢超时为 `timeout`，接纳为 v 或 0）；`result` ∈ `D,T,A`，依次表示下线（D 或 F 故障相位）、慢超时（S 且 v>timeout）、接纳（仅成功末项）。`changed` 为 `before≠after`。
- 键序、字段类型/范围/编码或时钟倒退报 INPUT/2；候选引用未知后端报 BACKEND/3；未配置环或无合格候选报 STATE/4。仅 `now` 推进时钟，不改连接、粘性、指标、告警或故障状态，失败批回滚。时间 O(T+BV·log(BV))、空间 O(T+BV)（T 为 `items` 项数，B/V 为后端数与每后端虚拟节点），紧凑 UTF-8 固定键序 JSON、单换行及 record/replay 逐字节契约不变，仅用标准库，旧行为不变。

## 配置变更审计：al / ai

配置变更（`ci`/`cb`/`cu`/`ca`）成功并分配新 rev 时追加一条审计事件；`rev` 从 1 起递增，事件按 rev 升序仅保留最近 64 条，超额淘汰最旧项。两查询均只读、不推进时钟，失败批次回滚。

- `al`：精确键序仅 `op`，返回键序 `op,events`，原样列出当前保留的全部事件。
- `ai`：精确键序 `op,after,limit`。`after` 为 0..10^18、`limit` 为 1..64 的非 bool 整数；键序、类型或范围非法报 INPUT/2。令 `latest` 为已分配最大 rev（初始 0），`after>latest` 报 STATE/4。
  - 历史非空且 `after` 小于最旧事件 rev 减 1 时，游标之前已有事件被淘汰：`truncated=true` 并自最旧事件读取；否则 `truncated=false`，仅取 `rev>after` 的事件。
  - 至多返回 `limit` 条；`next` 为末条事件 rev，无条目时等于 `after`；`more` 表示该页之后是否仍有可读事件。
  - 返回键序 `op,after,next,truncated,more,events`；空历史的 `after=0` 查询返回 `next=0,truncated=false,more=false,events=[]`。
- 事件项键序 `rev,now,kind,section,before,after`，两查询值义相同：`now` 为该变更操作的显式时钟，`kind` ∈ ci/cb/cu/ca，`section` 仅 `cu` 取被替换的顶层字段、其余为 null，`before`/`after` 为变更前后规范化 version=11 配置的 ct 摘要。
- `ai` 时间 O(A)、额外空间 O(limit)，A≤64，仅用标准库；紧凑 UTF-8 固定键序 JSON、单换行及 record/replay 逐字节契约不变。

## 审计段级差异查询：ad

按审计修订号返回某次配置变更的顶层段级前后指纹与实际差异；与 al/ai 事件同寿命、同淘汰。只读、不推进时钟、不改变淘汰窗口，失败批次回滚。

- `ad`：精确键序仅 `op,rev`；`rev` 为 1..10^18 的非 bool 整数。键集、键序、类型或范围非法报 INPUT/2；`rev` 尚未分配、超过最新修订，或其事件已随六十四条窗口淘汰报 STATE/4。
- 每次 ci/cb/cu/ca 成功分配 rev 时在同一审计事件内固化段级差异（仅存各段前后指纹、不复制整份配置），随事件一起保留和淘汰；即使整配置前后指纹相同也保留事件，此时 `changes=[]`。
- 返回固定键序 `op,rev,now,kind,section,before,after,changes`：前七项与同 rev 的 al 或 ai 事件逐值一致；`changes` 按 ce 规范化 version=11 配置的顶层键序排列但排除 version，只列前后值不同的段，项固定键序 `section,before,after`，两指纹为该段规范化 JSON 值按紧凑 UTF-8 与固定键序编码后的 SHA-256 小写六十四位十六进制。`cu` 的 `section` 仍为请求替换的段名，是否实际变化以 `changes` 为准（规范化后等值的替换不产生差异项）。
- 段级记录额外空间 O(64S)，ad 查询时间与额外空间 O(S)，成功配置变更的段级连同整配置指纹计算为 O(C)（S 为规范化顶层段数，C 为配置编码长度）；重复查询逐字节一致，record/replay 继续覆盖成功与失败结果，仅用标准库。

## 后端并发告警：na

每后端独立的并发滞回告警，窗值取该后端 `w` 窗 `mx` 口径的 `samples`/`peak`（空窗均为 0）。

- `na`：精确键序 `op,id,w,hi,lo,n,now`；`id` 沿用后端标识，`w`/`now` ∈ [0,10⁹]、`hi` ∈ [1,10⁹]、`lo` ∈ [0,10⁹)、`n` ∈ [1,60]，均为非 bool 整数且 `lo<hi`；`now` 纳入共用非递减时钟，须 `max(0,now//60-59)≤w<now//60`。
- 各 id 首评固化 `hi/lo/n` 并自 N 态起评：N 态连续 `n` 窗 `samples>0` 且 `peak≥hi` 转 A；A 态连续 `n` 窗 `samples>0` 且 `peak≤lo` 转 N；否则连续数清 0，转换后亦为 0。`w` 此后仅同值或 +1；同窗同参返回首评结果、不推进状态机。
- 返回键序 `op,id,w,state,samples,peak,run,changed`；`state` 仅 N/A，`changed` 仅转换时为 true。
- 键序、id 编码、类型、范围、关系或时钟倒退报 INPUT/2；未知 id 报 BACKEND/3；窗口未结束或过旧、跳窗、变参报 STATE/4。remove 后重加及 ci/cb/ca/cu 成功清状态，失败批次回滚。时间 O(1)、额外空间 O(B)，仅用标准库。

## 限流告警：le

每标识（`scope`,`id`）独立的限流滞回告警，窗值取该标识 `lh` 的 `w` 窗 `token`、`quota`（空窗均为 0），`value` 为二者之和并封顶 10¹⁸。

- `le`：精确键序 `op,scope,id,w,hi,lo,n,now`；`scope` 仅 B/C/S，`id` 沿用 `ls` 的非空 UTF-8 串校验，`w`/`now` ∈ [0,10⁹]、`hi` ∈ [1,10¹⁸]、`lo` ∈ [0,10¹⁸)、`n` ∈ [1,60]，均为非 bool 整数且 `lo<hi`；`now` 纳入共用非递减时钟，须 `max(0,now//60-59)≤w<now//60`。
- 各标识首评固化 `hi/lo/n` 并自 N 态起评：N 态连续 `n` 窗 `value≥hi` 转 A；A 态连续 `n` 窗 `value≤lo` 转 N；否则连续数清 0，转换后亦为 0。`w` 此后仅同值或 +1；同窗同参返回首评结果、不推进状态机。
- 返回键序 `op,scope,id,w,state,token,quota,value,run,changed`；`state` 仅 N/A，`changed` 仅转换时为 true。
- 键序、字段编码/类型/范围/关系或时钟倒退报 INPUT/2；B 未知 id 报 BACKEND/3；无桶且无配额、窗口未结束或过旧、跳窗或变参报 STATE/4。`ls`/`qs` 重配保留；remove 后重加清该 B 维告警，ci/cb/ca/cu 成功清全部，失败批次回滚。时间 O(1)、额外空间 O(K)，仅用标准库。

## 测试

    python -m unittest discover
