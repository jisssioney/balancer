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

## 审计段级差异：ad

按审计修订号返回该次变更涉及的规范化顶层配置段及各段前后指纹；只读，不推进时钟、不改淘汰窗口。

- `ad`：精确键序 `op,rev`（键须按此序出现）；`rev` 为 1..10^18 的非 bool 整数。键序、字段集合、`rev` 类型或范围非法报 INPUT/2；`rev` 尚未分配、超过最新修订，或其事件已随六十四条窗口淘汰报 STATE/4。失败不产生 stdout 并回滚整批操作。
- 每次 ci/cb/cu/ca 成功分配 rev 时，在同一审计事件内固化段级差异（只存段指纹、不复制整份配置），与现有事件同窗口保留与淘汰。
- 返回固定键序 `op,rev,now,kind,section,before,after,changes`；前七项与同 rev 的 `al` 或 `ai` 事件逐值一致。
- `changes` 按 ce 规范化 version=11 配置的顶层键序排列但排除 `version`（backends,vnodes,limits,overload,sticky,idle,backpressure,scheduler,faults,quotas,queue,capacities,lifetime），只列前后值不同的段；每项固定键序 `section,before,after`，两指纹都是对该段规范化 JSON 值按现有紧凑 UTF-8 与固定键序编码后取得的 64 位小写 SHA-256。即使成功操作生成相同的整配置指纹，事件仍保留且 `changes` 为空数组；cu 的 `section` 继续表示请求替换的段，不据此伪造实际差异。
- 重复查询逐字节一致；record 和 replay 继续覆盖成功与失败结果。段级记录保留空间上界 O(64S)，`ad` 时间与额外空间 O(S)，成功配置变更新增的指纹计算 O(C)，其中 S 为规范化顶层段数、C 为配置编码长度，仅用标准库。

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

## 连续不可用时长告警：ua

按后端与原因独立评估的连续不可用时长阈值告警，单项原因的 `active`/`since`/`duration` 沿用同一时刻 `ru` 的事实口径，不因更高优先级原因存在而遮蔽。

- `ua`：精确键序 `op,id,reason,threshold,now`。`id` 沿用后端标识；`reason` 仅 `drain`、`health`、`circuit`、`fault`；`threshold` 为 1..10⁹ 非 bool 整数；`now` 沿用全局 0..10⁹ 非递减显式时钟。
- 每个 `(id,reason)` 组合首次评估时固定 `threshold`，之后改值返回 STATE/4。
- 原因不生效时取 `active=false`、`since=null`、`duration=0`；生效时 `since` 为本次连续区间起点（health/drain/circuit 为各转换点维护的起点，fault 同 `ru`：D 段为 `a`、F 下线相位对齐到 `a+2v·((now-a)//(2v))`，F 上线相位与 S 不生效），`duration=now-since`。
- 状态仅 N/A：`active` 且 `duration≥threshold` 为 A，否则为 N；`changed` 仅表示相对该组合上次成功评估发生转换。首次以前态 N 计算，故首次已超阈值时 `changed=true`。
- 返回固定键序 `op,id,reason,threshold,state,active,since,duration,changed`。同一 `id,reason,threshold,now` 重报原样返回首次结果、不重复推进状态；同一时刻之后发生的原因变化从更大的 `now` 才可见。
- 键序、字段、编码、类型、范围、reason 或时钟非法报 INPUT/2；未知后端报 BACKEND/3；已登记组合变更 threshold 报 STATE/4（按 INPUT、BACKEND、STATE 判定）；失败批次不留下时钟、缓存或告警状态。remove 后同 id 重加清除对应登记，ci/cb/ca/cu 成功清空全部 ua 状态。ua 除时钟和自身状态外不修改连接、粘性、指标、故障计划或调度结果。fault 活动段查找为 O(log T)、其余三因 O(1)，单次时间 O(log(T+1))、额外空间 O(1)，总空间 O(B)（每后端至多四键，T 为该后端故障段数、B 为后端数）。紧凑 UTF-8 固定键序 JSON、单换行及 run、record、replay 的逐字节结果保持兼容，仅用标准库。

## 可移植运行态检查点：se / si

在现有 JSON 操作流中新增 `se`、`si`，使一次调用导出的状态能在另一条全新调用中恢复并继续处理公开操作；不依赖文件、网络或进程时间。对任意合法后续操作序列，直接继续与“导出后在空实例恢复再继续”所得退出码、stdout、stderr 逐字节一致。

- `se` 只接受 `op` 一个键，不推进显式时钟、不修改状态；返回固定键序 `op,version,digest,state`。`version` 初始为 1（非 bool 整数）；`state` 为规范化 JSON 对象，逐层键序固定，有业务顺序的集合（后端加入序、连接建连序、等待队列 FIFO、提交/审计 rev 序、告警转换事件窗序、分钟窗时序）保持原序，其余集合（粘性键、桶/配额与池级告警标识、老化服务类等）沿用 UTF-8 字节排序（复合键 scope 先按 B/C/S）。`state` 包含全部影响后续公开行为的状态：逻辑时钟、纯登记配置与待生效计划（vnodes、sticky/idle/lifetime、scheduler、overload、backpressure、queue 策略、aging、faults、quotas、capacities）、后端顺序及健康/权重预热/熔断/排空/不可用起点/登记端点、令牌桶（含当前令牌与补充时刻）、固定窗口配额（window、used）、活动连接与建连时端点快照、粘性映射、等待队列、限流配额与各类分钟历史、故障计划/统计/恢复基线、请求与采样指标及 mo 增量游标缓存、全部告警状态机（fe/ea/pa/xa/na/le/ua/wa）及其转换历史、配置提交历史与 next_rev、审计事件与段级差异、配置预约。
- `digest` 为对紧凑 UTF-8 编码的 `{"version":1,"state":...}`（`ensure_ascii=False`、分隔符 `,:`、无末尾换行）计算的小写 SHA-256 十六进制；相同状态逐字节导出相同结果。
- `si` 接受两种形式：现有四键形式精确键序 `op,version,digest,state`（键须按此序出现），无条件原子替换；新增五键乐观并发形式严格按 `op,base,version,digest,state` 排列，`base` 只接受 64 位小写十六进制 SHA-256（取 `sd` 返回的 `before` 或此前 `se` 返回的 digest），`version`、`digest`、`state` 沿用四键形式的含义、检查点版本与 8MiB 限制。两形式成功后均原子替换当前状态，返回固定键序 `op,digest,ok`（`ok=true`）。连续导入同一检查点幂等；恢复后再 `se` 复现相同 version、digest 与 state。
- 五键形式先按四键形式的同一规则校验键序、字段类型、UTF-8、版本、候选紧凑编码大小、候选摘要与状态语义；候选完全合格后再按 `se` 的规范化规则取得执行到该操作时的当前状态及摘要。当前摘要等于候选 `digest` 时视为成功的幂等重报（即使 `base` 已不等于当前摘要也不再次替换）；否则仅当 `base` 等于当前摘要才原子导入，`base` 不匹配报 STATE/4。键序、`base` 格式或其他输入格式错误报 INPUT/2，候选摘要不符仍优先于语义错误报 INPUT/2，候选或当前编码超过上限报 OVERLOAD/7，摘要相符但状态语义非法报 STATE/4。成功结果固定键序 `op,digest,ok`，digest 为候选摘要且 ok 为 true；操作不推进显式时钟，任何失败均无 stdout 并回滚整批操作。受保护导入成功后立即执行 `se` 逐值复现候选 version、digest、state，随后任意合法公开操作的退出码、stdout、stderr 与使用四键形式恢复同一状态后继续执行逐字节一致。五键形式时间与额外空间为 O(N)，N 为当前与候选规范化检查点编码字节数之和。
- 错误判定：键集合、键序、类型、范围、UTF-8 编码、未知 version、摘要格式或摘要不符报 INPUT/2；摘要相符但存在悬空引用、重复标识、矛盾计数或非法状态组合报 STATE/4；state 紧凑编码超过 8388608（8MiB）字节报 OVERLOAD/7（导出与导入同限）。任何失败均无 stdout 并回滚整批状态。
- 摘要不符先于 state 语义校验（INPUT）；状态规范化往返（恢复后重新导出与所给 state 的紧凑编码逐字节一致）保证恢复结果确定。se/si 及校验为 O(N) 时间、O(N) 额外空间，N 为检查点编码字节数；ce/ci、调度、连接、查询及错误优先级不变，run、record、replay 覆盖成功与失败检查点并保持逐字节契约，仅用标准库。

## 候选检查点只读差异：sd

在不替换当前运行态的前提下，判断一份候选检查点相对当前状态会改变哪些 state 顶层段；只读，不推进逻辑时钟、不改运行态或幂等缓存。

- `sd`：精确接受依次排列的 `op,version,digest,state` 四键（键须按此序出现），后三项沿用 `si` 的公开含义（version 仅收 1；digest 为小写 64 位十六进制）。先按 `si` 的规则校验候选检查点（紧凑编码 8MiB 上限、摘要匹配先于语义、完整结构/语义解析与规范化往返），再按 `se` 的规则取得当前状态的规范化内存快照；候选校验成功后，当前状态紧凑编码超过 `se` 导出上限同样报 OVERLOAD/7。
- 返回固定键序 `op,before,after,equal,changes,summary`：`before` 为当前检查点摘要（同 `se` 的 digest），`after` 原样返回输入摘要；`equal` 仅在两份规范化 state 的紧凑 UTF-8 编码（`ensure_ascii=False`、分隔符 `,:`、无末尾换行）逐字节相同时为 true。
- `changes` 只列值不同的 state 顶层段，顺序沿用 `se` 的顶层键序，每项固定键序 `section,before,after`；两指纹分别对该段的规范化 JSON 值用 `ensure_ascii=False`、分隔符 `,:`、无末尾换行编码后计算小写 64 位 SHA-256；是否列出以值比较为准。
- `summary` 固定键序 `sections,changed,bytes_before,bytes_after`，依次为顶层段总数、差异段数及两份 state 紧凑编码的 UTF-8 字节数。状态相同时 `changes` 为空、`changed` 为零，重复查询逐字节一致。
- 字段集合、键序、类型、UTF-8、版本、摘要格式与匹配、状态语义及 8MiB 上限沿用 `si` 的校验规则、错误优先级与 INPUT/2、STATE/4、OVERLOAD/7 分类。任何失败均无 stdout，并回滚整批操作（含此前操作的临时变化）。时间与额外空间 O(N)，`changes` 另占 O(S)，N 为两份 state 编码字节数之和、S 为顶层段数；现有 se/si、配置、调度、连接、查询语义及 run/record/replay 逐字节契约不变，仅用标准库。

## 测试

    python -m unittest discover
