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

## 哈希分布预演：hb

沿用 `hd` 的静态环与 SHA-256 规则，分别按两份规范化配置的后端加入顺序与 `vnodes` 建环，按后端汇总后端增删或虚拟节点调整造成的键落点偏斜；只读，忽略健康、熔断、排空、故障时间线及粘性运行态。

- `hb`：精确键序 `op,config,keys,now`。`config` 的结构、规范化、版本兼容及后端引用检查沿用 `hd`；`keys` 为 1..4096 项允许重复的合法 route 键数组（非空、可直接 UTF-8 编码），重复键按出现次数计数；`now` 为 0..10⁹ 的非 bool 整数，服从全局非递减显式时钟。
- 每个 key 在当前环（base）与候选环（target）独立映射：令牌为 SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，按摘要、加入序、i 排序，key 哈希取首个不小于它的令牌、越界回绕。
- 返回固定键序 `op,base,target,backends,summary`；`base`/`target` 为当前与候选规范化 version=11 配置的 `ct` 摘要。`backends` 先按当前后端加入顺序排列，再按候选顺序补充仅在候选中存在的后端；每项键序 `id,before,after,delta`，后端不存在于某侧时该侧计数为 0，`delta=after-before`。
- `summary` 固定键序 `total,stable,remapped,rate,before_min,before_max,after_min,after_max`：`stable`/`remapped` 按两侧落点是否相同计数（重复键按次数）；`rate` 为 `remapped` 占 `total` 的百分比，`floor(10000*remapped/total)/100` 向下截断为两位定点字符串；每侧 `min`/`max` 只统计该侧实际存在的后端。
- 字段集合、键序、类型、范围、UTF-8 编码、`keys` 数量或时钟倒退报 INPUT/2；候选配置中的 B 维限流、B 配额、故障计划或容量覆盖存在悬空引用时报 BACKEND/3；任一侧未配置 `vnodes` 或没有后端时报 STATE/4，错误优先级沿用 `hd`。仅成功时推进 `now`，不改变其他运行态；失败整批回滚且不产生 stdout。时间 O(N log N+K log N)，额外空间 O(N+B)（N 为两环令牌总数、K 为 keys 项数、B 为两侧后端并集），仅用标准库；紧凑 UTF-8 固定键序 JSON、单末尾换行及 run、record、replay 逐字节契约不变，`hd` 与其他公开行为不变。

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

## 全池排空投影与批量到期：dq / dx

在单后端 `ds`/`dr`/`du`/`dg` 之外提供确定性的全池排空投影与批量执行；既有单后端操作语义与结果不变。

- `dq`：精确键序 `op,now`（键须按此序出现）；`now` 为非负非 bool 整数，纳入共用非递减显式时钟（倒退报 INPUT/2）。除推进时钟外为只读，不改变任何状态。
  - 返回固定键序 `op,now,items`；`items` 按后端加入顺序列出排空状态为 D 或 X 的后端，每项固定键序 `id,state,connections,start,deadline,end,due,remaining,forced`。
  - `due` 仅在 D 且 `now` 不早于 `deadline` 时为 true；D 的 `remaining=max(deadline-now,0)`，X 的 `remaining` 恒为 0。空池或无 D/X 后端时 `items=[]`，不报错。时间 O(B)、结果空间 O(B)。
- `dx`：精确键序 `op,now`，`now` 校验与时钟语义同 `dq`。处理集合为状态 D 且 `deadline<=now` 的后端，按后端加入顺序处理；每个后端的连接按全局建连顺序关闭。
  - 关闭沿用 `dg` 强制到期的联动语义：删除活动连接及建连时端点快照、该后端并发归零、转为 X、`end=deadline`、`forced=本次关闭数`；不消费等待队列、令牌或配额。
  - 返回固定键序 `op,now,backends,closed`；`backends` 只含本次到期项，项固定键序 `id,deadline,cids`，`cids` 按关闭顺序排列，`closed` 为全部 cids 数量。无到期项时为空数组和 0；同一 `now` 重复执行为确定性空操作；处理无失败路径，天然原子，不留部分转换。时间 O(B+C)、结果空间 O(B+C)，B 为后端数、C 为活动连接数。
- 后续路由仍按既有规则处理 X（不参与新映射、旧粘性失效并依环迁移）。两项操作的键集合、键序、`now` 类型或范围非法以及时钟倒退统一返回 INPUT/2，失败无 stdout，并回滚同批此前变化和逻辑时钟。
- `run`、`record`、`replay` 继续逐字节一致；检查点完整保留连接、排空状态与时钟变化，`se` 导出后 `si` 恢复继续执行与直接继续一致。仅使用 Python 标准库，不访问网络或系统时间。

## 可移植运行态检查点：se / si

在现有 JSON 操作流中新增 `se`、`si`，使一次调用导出的状态能在另一条全新调用中恢复并继续处理公开操作；不依赖文件、网络或进程时间。对任意合法后续操作序列，直接继续与“导出后在空实例恢复再继续”所得退出码、stdout、stderr 逐字节一致。

- `se` 只接受 `op` 一个键，不推进显式时钟、不修改状态；返回固定键序 `op,version,digest,state`。`version` 初始为 1（非 bool 整数）；`state` 为规范化 JSON 对象，逐层键序固定，有业务顺序的集合（后端加入序、连接建连序、等待队列 FIFO、提交/审计 rev 序、告警转换事件窗序、分钟窗时序）保持原序，其余集合（粘性键、桶/配额与池级告警标识、老化服务类等）沿用 UTF-8 字节排序（复合键 scope 先按 B/C/S）。`state` 包含全部影响后续公开行为的状态：逻辑时钟、纯登记配置与待生效计划（vnodes、sticky/idle/lifetime、scheduler、overload、backpressure、queue 策略、aging、faults、quotas、capacities）、后端顺序及健康/权重预热/熔断/排空/不可用起点/登记端点、令牌桶（含当前令牌与补充时刻）、固定窗口配额（window、used）、活动连接与建连时端点快照、粘性映射、等待队列、限流配额与各类分钟历史、故障计划/统计/恢复基线、请求与采样指标及 mo 增量游标缓存、全部告警状态机（fe/ea/pa/xa/na/le/ua/wa）及其转换历史、配置提交历史与 next_rev、审计事件与段级差异、配置预约、全池端点切换预约。
- `digest` 为对紧凑 UTF-8 编码的 `{"version":1,"state":...}`（`ensure_ascii=False`、分隔符 `,:`、无末尾换行）计算的小写 SHA-256 十六进制；相同状态逐字节导出相同结果。
- `si` 接受两种严格键序形式（键须按各自次序出现）：四键 `op,version,digest,state` 为无条件原子替换；五键 `op,base,version,digest,state` 为带基线摘要的乐观并发形式，调用方把 `sd` 返回的 `before` 或此前 `se` 返回的 `digest` 作为替换前提，避免预览后运行态已变化时覆盖新状态。两形式 `version` 仅收 1（非 bool 整数）；五键的 `base` 与两形式的 `digest` 均为小写 64 位十六进制 SHA-256；`state` 含义、检查点版本与 8MiB 限制沿用四键形式。
- 五键形式先按现有规则校验键序、字段类型、UTF-8、版本、候选编码大小、候选摘要与状态语义（错误优先级同四键），再按 `se` 的规范化规则取得执行到该操作时的当前状态及摘要。若当前摘要等于候选 `digest`，视为成功的幂等重报——即使 `base` 已不等于当前摘要也不再次替换；否则只有 `base` 等于当前摘要时才原子导入，`base` 不匹配报 STATE/4。成功后原子替换当前状态，返回固定键序 `op,digest,ok`（`ok=true`，`digest` 为候选摘要）；连续重报（含 `base` 已过期的幂等重报）逐字节返回同一结果。
- 错误判定：键集合、键序、类型、范围、UTF-8 编码、未知 version、`base`/`digest` 格式或摘要不符报 INPUT/2；摘要相符但存在悬空引用、重复标识、矛盾计数或非法状态组合，或五键形式 `base` 与当前摘要不符（且非幂等重报），报 STATE/4；候选或当前 state 紧凑编码超过 8388608（8MiB）字节报 OVERLOAD/7（导出与导入同限；五键形式候选校验通过后当前编码超限同样报 OVERLOAD/7）。候选摘要不符仍优先于语义错误报 INPUT/2；摘要相符但状态语义非法报 STATE/4。操作不推进显式时钟，任何失败均无 stdout 并回滚整批状态。
- 摘要不符先于 state 语义校验（INPUT）；状态规范化往返（恢复后重新导出与所给 state 的紧凑编码逐字节一致）保证恢复结果确定。受保护导入成功后立即执行 `se`，逐值复现候选 version、digest 和 state；随后任意合法公开操作的退出码、stdout 与 stderr，与使用旧四键 `si` 恢复同一状态后继续执行逐字节一致。se/si 及校验为 O(N) 时间、O(N) 额外空间，五键 si 的 N 为当前与候选规范化检查点编码字节数之和；ce/ci、调度、连接、查询及错误优先级不变，run、record、replay 覆盖五键请求的成功、幂等重报与失败并保持紧凑 UTF-8 固定键序 JSON、单末尾换行逐字节契约，仅用标准库。

## 候选检查点只读差异：sd

在不替换当前运行态的前提下，判断一份候选检查点相对当前状态会改变哪些 state 顶层段；只读，不推进逻辑时钟、不改运行态或幂等缓存。

- `sd`：精确接受依次排列的 `op,version,digest,state` 四键（键须按此序出现），后三项沿用 `si` 的公开含义（version 仅收 1；digest 为小写 64 位十六进制）。先按 `si` 的规则校验候选检查点（紧凑编码 8MiB 上限、摘要匹配先于语义、完整结构/语义解析与规范化往返），再按 `se` 的规则取得当前状态的规范化内存快照；候选校验成功后，当前状态紧凑编码超过 `se` 导出上限同样报 OVERLOAD/7。
- 返回固定键序 `op,before,after,equal,changes,summary`：`before` 为当前检查点摘要（同 `se` 的 digest），`after` 原样返回输入摘要；`equal` 仅在两份规范化 state 的紧凑 UTF-8 编码（`ensure_ascii=False`、分隔符 `,:`、无末尾换行）逐字节相同时为 true。
- `changes` 只列值不同的 state 顶层段，顺序沿用 `se` 的顶层键序，每项固定键序 `section,before,after`；两指纹分别对该段的规范化 JSON 值用 `ensure_ascii=False`、分隔符 `,:`、无末尾换行编码后计算小写 64 位 SHA-256；是否列出以值比较为准。
- `summary` 固定键序 `sections,changed,bytes_before,bytes_after`，依次为顶层段总数、差异段数及两份 state 紧凑编码的 UTF-8 字节数。状态相同时 `changes` 为空、`changed` 为零，重复查询逐字节一致。
- 字段集合、键序、类型、UTF-8、版本、摘要格式与匹配、状态语义及 8MiB 上限沿用 `si` 的校验规则、错误优先级与 INPUT/2、STATE/4、OVERLOAD/7 分类。任何失败均无 stdout，并回滚整批操作（含此前操作的临时变化）。时间与额外空间 O(N)，`changes` 另占 O(S)，N 为两份 state 编码字节数之和、S 为顶层段数；现有 se/si、配置、调度、连接、查询语义及 run/record/replay 逐字节契约不变，仅用标准库。

## 三方检查点只读预演：sx

以当前运行态、基线检查点与候选检查点区分并发变化，让调用方在执行 `si` 前判断候选是否仍可安全使用。只读：不合并或导入、不推进显式时钟、不改变任何运行态或幂等缓存；现有 se、sd、si 行为不变。

- `sx`：只接受按 `op,base,target` 排列的三个键（键须按此序出现）。`base` 和 `target` 都是按 `version,digest,state` 排列且不含 `op` 的检查点对象（version 仅收 1，非 bool 整数；digest 为小写 64 位十六进制 SHA-256）。
- 先按 `si` 规则校验 `base` 再校验 `target`（各自沿用 8MiB 上限、摘要匹配先于语义、完整结构/语义解析与规范化往返，及 INPUT/2、STATE/4、OVERLOAD/7 的错误优先级），再按 `se` 规则取得当前快照。错误严格按 base、target、current 顺序判定；任一输入检查点或当前 state 紧凑编码超过 8388608（8MiB）字节报 OVERLOAD/7。失败无 stdout 并回滚整批操作。
- 返回固定键序 `op,current,base,target,status,changes,summary`；`current`/`base`/`target` 分别为当前、基线、候选检查点（version=1）的小写 SHA-256 摘要。
- 当前与候选的规范化 state 紧凑 UTF-8 编码逐字节相同时 `status` 为 `SAME`；否则有冲突段时为 `CONFLICT`，其余为 `CLEAN`。
- `changes` 按 state 顶层键序列出三份状态不全相同的段（三份完全相同时为空）；每项固定键序 `section,base,current,target,kind`，`base`/`current`/`target` 为对应规范化段 JSON（`ensure_ascii=False`、分隔符 `,:`、无末尾换行）的小写 SHA-256。当前等于基线而候选不同时 `kind` 为 `TARGET`，候选等于基线而当前不同时为 `CURRENT`，当前等于候选而基线不同时为 `SAME`，其余为 `CONFLICT`。
- `summary` 固定键序 `sections,changed,target,current,same,conflict`，依次为顶层段总数、差异段数及四类 `kind` 计数，四类计数之和等于 `changed`。
- 重复预演逐字节一致；run、record、replay 继续遵守紧凑 UTF-8 固定键序 JSON 与单末尾换行。时间与额外空间均为 O(N)，N 为三份规范化状态紧凑编码的总字节数，仅用标准库。

## 检查点三方合并：sm

调用方提交基线检查点与目标检查点，把目标相对基线的无冲突变化原子合入执行到该操作时的当前运行态。

- `sm`：只接受按 `op,base,target` 排列的三个键（键须按此序出现），不接受 `now`，也不推进显式时钟。`base`、`target` 都沿用 `sx` 的 `version,digest,state` 检查点对象及规范化规则（version 仅收 1，digest 为小写 64 位十六进制）。
- 校验顺序严格按 base、target、current：base 和 target 各自沿用 `si` 的 8MiB 上限、摘要匹配先于语义、完整结构/语义解析与规范化往返，及 INPUT/2、STATE/4、OVERLOAD/7 的错误优先级；两份检查点全部通过后再按 `se` 规则取当前规范化快照，当前 state 紧凑编码超过 8388608 字节报 OVERLOAD/7。失败无 stdout 并回滚同批此前变化。
- 合并以 `se` 规范化 state 顶层段为最小单位，按其固定键序处理：某段 target 等于 base 时保留 current；target 不同于 base 且 current 等于 base 或 target 时采用 target；其余情况视为冲突。存在冲突即拒绝整次合并，不安装任何段（STATE/4）。
- 没有冲突时把组合结果作为完整检查点再次校验：紧凑编码 8MiB 上限、悬空引用、重复标识、矛盾计数或跨段非法组合均拒绝且不改变状态（OVERLOAD/7 或 STATE/4），并以规范化往返兜底确定性；全部通过后才原子安装。
- 成功返回固定键序 `op,before,after,changed,sections,ok`：`before`/`after` 为合并前后按 `se` 规则得到的摘要，`changed` 为实际改变的段数，`sections` 按顶层键序列出这些段名，`ok` 为 true。目标变化已经存在（current 等于 target）或目标未改变（target 等于 base）时幂等成功：`after` 等于当前摘要、`changed` 为 0、`sections` 为空数组。
- 键集合、键序、类型、版本、摘要格式或摘要不符报 INPUT/2；输入状态语义非法、段冲突或组合状态非法报 STATE/4；任一受检或合并后状态超限报 OVERLOAD/7。失败不产生 stdout，并回滚同批此前变化。时间与额外空间上界 O(N+S)，N 是三份状态及合并结果规范化编码总字节数，S 是顶层段数；输出继续使用紧凑 UTF-8 固定键序 JSON 和单个末尾换行，run、record、replay 覆盖成功、幂等与失败结果，现有 se、sd、sx、si 及其他公开行为保持不变，仅用标准库。

## 配置预约取消：cx

在 `cp` 保存的预约生效前，按调用方持有的 `cq` 摘要与触发时刻进行条件取消，避免查询后预约已被另一条 `cp` 替换时误删新计划。

- `cx`：只接受严格键序 `op,digest,at,now`（键须按此序出现）。`digest` 为小写 64 位十六进制 SHA-256（格式同 `ct` 摘要，仅用于匹配，不要求对应当前配置）；`at`、`now` 均为 0..10⁹ 的非 bool 整数。`now` 纳入现有全局非递减显式时钟（倒退报 INPUT/2），`at` 只用于匹配预约触发时刻、不推动时钟。
- 当前存在预约且 `digest`、`at` 都与之相同时，原子删除该预约，按固定键序返回 `op,cancelled,digest,at`，`cancelled=true`，`digest`/`at` 回显请求值；随后 `cq` 报告 `pending=false`，`ca` 按无预约返回 STATE/4。
- 当前没有预约时为幂等空操作：同一键序返回 `cancelled=false` 并回显请求值，不创建修订、审计事件或任何其他状态。当前有预约但摘要或时刻任一不匹配时返回 STATE/4 并保留原预约。
- 键集合、键序、字段类型、范围、摘要格式、UTF-8 编码或时钟倒退均返回 INPUT/2，且 INPUT 判定先于预约匹配；所有失败都不产生 stdout，并回滚本批次内时钟与此前操作造成的变化。
- 取消只影响待生效预约：不应用候选配置，不改变当前配置、后端、连接、队列、粘性、指标、告警、提交历史和 next_rev。后续 `cp` 仍可建立新预约；`ci`、`cb`、`cu`、`ca` 清除预约的既有语义不变，这些操作成功清除预约后，同一 `cx` 请求按无预约处理。
- `se` 必须把取消后的 reservation 导出为 null；`si` 恢复后执行 `cx` 的结果与直接继续执行一致，`sd`、`sx`、`sm` 仍按现有检查点语义观察这项差异。单次时间与额外空间均为 O(1)，仅使用 Python 标准库；run、record、replay 对成功、空操作与失败保持逐字节一致的紧凑 UTF-8 固定键序 JSON 和单末尾换行，其他公开入口及错误优先级不变。

## 配置预约条件替换：cy

在 `cp` 无条件覆盖之外提供并发安全的改期或换配置：调用方依据 `cq` 读到的旧预约摘要与触发时刻提交候选，仅当预约仍是旧值（或已经是该请求的新值）时才写入。

- `cy`：只接受严格键序 `op,base,base_at,config,at,now`（键须按此序出现）。`base` 为旧预约的小写 64 位十六进制 SHA-256 摘要（格式同 `cx.digest`，仅用于匹配）；`base_at` 为旧触发时刻；`config` 沿用 `cp` 的 version=11 配置校验与规范化语义；`at`、`now` 均为 0..10⁹ 的非 bool 整数。`now` 纳入全局非递减显式时钟（倒退报 INPUT/2），`at` 仅表示新触发时刻、不推进时钟，且 `at<now` 报 INPUT/2。
- 请求先校验字段、候选配置与候选后端引用，再读取当前预约。字段集合、键序、类型、范围、UTF-8、摘要格式、配置结构或时钟倒退返回 INPUT/2；候选配置中 B 限流、B 配额、故障计划或容量覆盖引用候选后端集合之外的标识返回 BACKEND/3（错误优先级同 `cp`：`at<now` 的 INPUT 先于 BACKEND，BACKEND 先于预约匹配）。
- 当前没有预约返回 STATE/4；当前预约既不等于 `base`/`base_at` 指定的旧值，也不等于候选配置规范化摘要与 `at` 指定的新值时返回 STATE/4 并保留原预约。旧值匹配时原子替换为规范化候选快照；当前已经等于新值时视为幂等重报——即使旧值已过期（预约已被另一请求改成新值）也成功，且不得重写状态。保留 `cp` 的无条件覆盖语义不变。
- 成功固定返回键序 `op,base,digest,at,ok`：`base` 回显请求的旧摘要，`digest` 为候选规范化配置摘要，`at` 回显新时刻，`ok=true`；首次替换与相同请求重报必须逐字节一致。
- `cy` 只改变待生效预约：不应用配置，不创建 rev 或审计事件，也不改变当前配置、后端、连接、队列、粘性、指标、告警、提交历史和 next_rev。替换后 `cq` 报告新摘要与新时刻，`ca` 只能按新预约生效，`cx` 只能凭新摘要与新时刻取消；旧凭据对 `cx` 和后续 `cy` 不再有效。`se` 必须导出新预约，`si` 恢复后继续执行的结果与未迁移状态一致，`sd`、`sx`、`sm` 按既有检查点规则观察 reservation 差异。
- 任何失败都无 stdout 并回滚同批此前变化及逻辑时钟。单次时间与额外空间上界 O(N)，N 为候选规范化配置大小，仅使用 Python 标准库；run、record、replay 继续输出固定键序紧凑 UTF-8 JSON 和单末尾换行，其他公开操作及错误优先级保持不变。

## 配置预约条件生效：ca（条件形态）

在保留两键 `ca` 直接按当前预约生效的同时，为调用方提供此前 `cq` 读到的身份（摘要与触发时刻）作为前提，避免查询后预约已被另一条 `cp`/`cy` 替换时误激活新计划。

- `ca` 接受两种严格键序形式（键须按各自次序出现）：现有两键 `op,now` 语义完全不变；条件形态为 `op,digest,at,now`。`digest` 为小写 64 位十六进制 SHA-256（格式同 `cx.digest`，仅用于匹配预约身份）；`at`、`now` 均为 0..10⁹ 的非 bool 整数。`now` 纳入现有全局非递减显式时钟（倒退报 INPUT/2），`at` 仅作为预约身份、不推进时钟。
- 字段集合、键序、字段类型、范围、UTF-8 编码、摘要格式或时钟倒退均返回 INPUT/2；失败无 stdout 并回滚整批。
- 条件形态先做身份判定：当前没有预约，或当前预约的规范化配置摘要、触发时刻任一不等于 `digest`、`at` 时返回 STATE/4，且不得应用任何配置、清除预约、分配 rev 或写入审计。身份匹配后沿用直接 `ca` 的全部判定：`now` 早于预约触发时刻、存在活动连接或等待队列、修订号耗尽（next_rev>10¹⁸，同 ci）均返回 STATE/4。
- 全部校验通过后，以 `now` 原子载入预约中的 version=11 快照，重建与普通 `ca` 相同的默认运行态，分配并保留新修订（提交历史仅留最近 16 条），追加 `kind=ca` 的审计事件与段级差异，清除预约；固定键序返回 `op,digest,rev,ok`，其中 `digest` 为已匹配预约的摘要（匹配成功时即请求 `digest`）、`ok=true`。成功结果与同一状态下两键 `ca` 成功后的配置、提交历史与审计状态逐值一致。
- 条件形态只在成功时改变状态；任何失败都随整批回滚，预约、时钟、配置、rev 与审计均不变。成功后旧凭据失效：`cq` 报告 `pending=false`，重复同一条件 `ca` 按无预约返回 STATE/4。
- `se` 导出与 `si` 恢复后，条件 `ca` 的成功或失败与直接继续一致；`sd`、`sx`、`sm` 仍只按既有检查点规则观察 reservation 段的变化。身份比较为 O(1) 时间与额外空间，完整生效仍以预约规范化配置及被重建状态大小 N 为 O(N) 时间和额外空间，仅使用 Python 标准库；run、record、replay 对成功与各类失败保持紧凑 UTF-8 固定键序 JSON、单末尾换行与逐字节一致，`cp`、`cq`、`cx`、`cy`、两键 `ca`、`ci`、`cb`、`cu` 及其他调度、连接、队列、粘性、指标和告警行为不变。

## 配置回滚乐观并发形态：cb（四键形态）

在保留三键 `cb` 无条件按执行时状态回滚的同时，让调用方能够确认自己回滚的正是先前读取的配置：四键形态把操作开始前持有的当前配置 `ct` 摘要作为前提，避免读后配置已被另一条 `ci`/`cb`/`cu`/`ca` 替换时误回滚到新状态之上。

- `cb` 接受两种形式：原三键 `op,rev,now` 仅按键集识别（键序不限），输入、输出与语义完全不变；四键只接受严格键序 `op,rev,base,now`（键须按此序出现）。四键的 `rev` 同为提交历史内保留的目标修订（1..10¹⁸ 非 bool 整数）；`base` 为操作开始前当前规范化 version=11 配置的 `ct` 摘要，格式为小写 64 位十六进制字符串；`now` 为 0..10⁹ 的非 bool 整数并纳入既有非递减显式时钟。
- 四键请求先完成输入校验并确认目标修订仍在十六条提交窗口内，再比较 `base` 与操作开始时的当前配置摘要；不匹配时返回 STATE/4，且不能应用目标快照、推进时钟、清除预约、分配修订或写入审计。匹配后沿用现有 `cb` 的全部判定与效果：修订号尚可分配且不存在活动连接或等待队列时，以 `now` 原子载入目标规范化快照，按既有规则重建默认运行态，清除配置预约，追加一个新的提交修订、`kind=cb` 的审计事件及段级差异，并按固定键序返回 `op,target,rev,ok`；即使目标配置与当前配置逐值相同，也继续采用现有 `cb` 会产生新修订的行为。
- 键集合、键序、`base` 格式、`rev` 或 `now` 的类型与范围、UTF-8 编码以及时钟倒退均返回 INPUT/2；目标修订不存在或已淘汰、`base` 不匹配、修订号耗尽、存在活动连接或等待队列均返回 STATE/4。任何失败都不产生 stdout，并回滚同批此前变化和逻辑时钟。
- `se` 导出、`si` 恢复以及 `sd`、`sx`、`sm` 的后续观察结果与直接继续执行一致；`run`、`record`、`replay` 对成功与失败均保持固定键序紧凑 UTF-8 JSON、单末尾换行与逐字节确定性。身份比较为 O(1)，单次时间与额外空间上界保持 O(16N)，仅使用 Python 标准库；新增校验不改变 `cl`、`ct`、`ci`、`cu`、`ca`、三键 `cb` 及其他公开操作。

## 端点轮换存量查询与可控清理：eq / ec

在不改变 `ep` 覆盖登记与 `fw` 转发快照语义的前提下，让调用方先更新端点，再观察并按建连时间终止仍指向旧端点的连接；只读快照、显式时钟，不自动迁移连接，也不重新消费等待队列。

- `eq`：精确键序 `op,id,now`（键须按此序出现）。`id` 沿用后端标识；`now` 为 0..10⁹ 的非 bool 整数并进入全局非递减时钟。目标后端须存在且已通过 `ep` 配置当前端点：未知后端报 BACKEND/3，未配置端点报 STATE/4。
  - 返回固定键序 `op,id,now,current,total,fresh,stale,items`；`current` 固定键序 `host,port`，为当前端点。`total` 为该后端活动连接总数；`fresh` 为建连时转发快照与当前端点完全相同的连接数；`stale` 为其余连接数（含无快照连接）。
  - `items` 只列 stale 连接，按全局建连顺序排列；每项固定键序 `cid,host,port,opened,age`。无快照连接的 `host`、`port` 为 null，`age=now-opened`。没有活动连接时计数为零且 `items=[]`。`eq` 除推进时钟外不改变任何运行态。
- `ec`：精确键序 `op,id,before,now`（键须按此序出现）。`before` 与 `now` 均为 0..10⁹ 的非 bool 整数且 `before≤now`，`now` 进入全局非递减时钟。只关闭目标后端中快照不同于当前端点且 `opened≤before` 的活动连接，按全局建连顺序删除连接及快照、逐条递减并发；返回固定键序 `op,id,now,closed,cids`，`cids` 保持关闭顺序，无符合项时 `closed=0`、`cids=[]`。
  - 清理使排空 D 状态后端失去最后连接时，沿用 `close` 规则转为 X 且 `end=now`；不扣减令牌或配额，也不触发排队接纳。
- 字段、键序、编码、类型、范围、`before` 关系或时钟倒退报 INPUT/2，未知后端报 BACKEND/3，未配置端点报 STATE/4，并按此顺序判定；失败无 stdout 且整批回滚（含同批此前变化与逻辑时钟）。两项操作时间为 O(C)、额外空间不超过 O(C)（C 为活动连接数）；`ep`、`fw`、配置、检查点、run、record、replay 及既有固定键序 JSON 行为不变，仅用标准库。

## 全池端点轮换盘点与批量清理：er / ex

在单后端 `eq`/`ec` 之外提供确定性的全池端点轮换盘点与批量执行；既有 `ep`、`fw`、`eq`、`ec` 语义与结果不变。

- `er`：精确键序 `op,now`（键须按此序出现）。`now` 为 0..10⁹ 的非 bool 整数，进入全局非递减显式时钟（倒退报 INPUT/2）。除推进时钟外为只读，不改变任何状态。
  - 返回固定键序 `op,now,items,summary`；`items` 按后端加入顺序列出已通过 `ep` 登记当前端点的后端，未登记端点的后端（即使存在连接）不列入。每项固定键序 `id,current,total,fresh,stale,connections`；`current` 固定键序 `host,port`，`total`/`fresh`/`stale` 口径同 `eq`（无快照连接算 stale）。
  - `connections` 沿用 `eq` items 的 `cid,host,port,opened,age` 口径，只列该后端 stale 连接并按全局建连顺序排列；无快照连接 `host`/`port` 为 null，`age=now-opened`。
  - `summary` 固定键序 `backends,total,fresh,stale`：`backends` 为列出的后端数，其余三项为跨列出后端的连接计数之和。空池或没有已登记端点的后端时 `items=[]` 且四项汇总全为 0，不报错。
- `ex`：精确键序 `op,before,now`（键须按此序出现）。`before` 与 `now` 均为 0..10⁹ 的非 bool 整数且 `before≤now`，`now` 进入全局非递减显式时钟（倒退报 INPUT/2）。
  - 处理集合为已登记当前端点的后端；按全局建连顺序关闭其中快照不同于当前端点且 `opened≤before` 的活动连接，删除连接与快照、逐条递减并发。`opened==before` 关闭，fresh 连接（快照与当前端点相同）永不关闭；未登记端点后端的连接（含无快照连接）不在处理集合内。
  - 返回固定键序 `op,now,items,closed`；`items` 按后端加入顺序仅列本次有关闭项的后端，每项固定键序 `id,cids`，`cids` 保持全局关闭次序，`closed` 为总关闭数。无符合项时为成功的确定性空操作：`items=[]`、`closed=0`；同参或更大 `now` 重复执行不影响其他连接。
  - 清理使排空 D 状态后端失去最后连接时沿用 `ec`/`close` 规则转 X 且 `end=now`，不改变 `forced`；不扣减令牌或配额、不消费等待队列，也不触发重新调度。
- 字段集合、键序、数值类型、范围、`before` 关系或时钟倒退统一返回 INPUT/2，失败无 stdout，并原子回滚同批此前变化与逻辑时钟。两项操作单次时间与额外空间上界均为 O(B+C)（B 为后端数、C 为活动连接数）；`run`、`record`、`replay` 逐字节一致，`se` 导出后 `si` 恢复继续执行与直接继续一致。仅使用 Python 标准库，紧凑 UTF-8 固定键序 JSON、单末尾换行契约不变。

## 后端端点原子条件变更：ey

在 `ep` 无条件覆盖之外提供并发安全的端点变更：调用方依据此前读到的当前端点提交 `base` 与候选 `target`，仅当登记仍是旧值（或已经是该请求的目标值）时才写入，避免持有旧读数的调用覆盖较新的端点登记。`ep` 的无条件覆盖语义与所有既有入口行为不变。

- `ey`：输入严格按键序 `op,id,base,target,now` 排列（键须按此序出现）。`base`、`target` 均为 `null` 或按键序 `host,port` 排列的对象；`null` 表示未登记端点，对象沿用 `ep` 对规范 IP 字面量（无区域标识且规范化形式与原文逐字节相同）和 1 至 65535 非 bool 整数端口的校验。`now` 为 0..10⁹ 的非 bool 整数并进入全局非递减显式时钟。
- 请求先完整校验字段、键序、端点结构、编码和时钟，再确认后端存在。后端不存在报 BACKEND/3。当前端点等于 `target` 时，无论 `base` 是否仍匹配，都把请求视为成功的幂等重报且不重写登记；否则只有当前端点与 `base` 逐值相等时才原子写入 `target`，`target` 为 `null` 时清除登记；其余比较失败统一返回 STATE/4。输入或时钟错误返回 INPUT/2，错误优先级依次为 INPUT、BACKEND、STATE。
- 成功固定返回键序 `op,id,endpoint,ok`：`endpoint` 为规范化后的 `target` 对象或 `null`，`ok` 恒为 `true`；首次写入与相同请求重报必须逐字节一致。任何失败都不产生 stdout，并回滚同批此前变化、端点和逻辑时钟。
- `ey` 只改变后端当前端点：不迁移、关闭或重新调度活动连接，不消费令牌、配额或等待队列。已有连接继续由 `fw` 返回其建连时快照；后续连接仅在变更后仍有当前端点时固化新快照。清除后该后端沿用现有未登记端点行为：`eq` 返回 STATE/4，`er` 不列出它，已有快照仍可由 `fw` 查询；重新登记后，旧快照自然按现有口径成为 stale。
- `se` 和 `si` 必须完整保存并恢复结果（登记端点本就在检查点内）。单次时间与额外空间均为 O(1)，仅使用 Python 标准库；`run`、`record`、`replay` 对成功、幂等和失败保持固定键序紧凑 UTF-8 JSON、单末尾换行及逐字节确定性。

## 批量后端端点原子条件变更：eb

在单后端 `ey` 之外把一次服务切换涉及的多个后端作为一个原子单元更新：提交一组后端各自的旧端点与目标端点，仅当全部前提同时成立时才一次性完成切换。`ep`、`ey`、`eq`、`ec`、`er`、`ex` 及所有既有入口行为不变。

- `eb`：输入严格按键序 `op,items,now` 排列（键须按此序出现）。`items` 为 1 至 1000 项数组；每项严格按键序 `id,base,target` 排列，`id` 沿用后端标识且同一 `id` 在数组中不得重复。`base` 与 `target` 均为 `null` 或按键序 `host,port` 排列的对象，含义与校验逐项沿用 `ey`（`null` 表示未登记端点，对象沿用 `ep` 对规范 IP 字面量与 1 至 65535 非 bool 整数端口的校验）。`now` 为 0..10⁹ 的非 bool 整数并进入全局非递减显式时钟。
- 处理顺序固定为：先完整校验整份输入（字段、键序、容器、数量、重复 id、端点结构、编码、数值范围），再按 `items` 顺序确认所有后端存在，最后统一检查条件；任何阶段都不得提前写入。每一项在当前端点等于 `target` 时视为已满足，不要求 `base` 仍匹配且不重写登记；否则当前端点必须逐值等于 `base`，才在提交时写入 `target`（`target` 为 `null` 时清除登记）。因此首次提交、完整重报，以及部分后端已到达目标而其余仍停留在各自基线的重试都得到同一成功结果。只要任一后端的当前端点既不等于 `base` 也不等于 `target`，整项操作返回 STATE/4；任一未知后端返回 BACKEND/3。判定优先级固定为 INPUT、BACKEND、STATE。
- 所有失败均不产生 stdout，并回滚本批此前变化、端点和逻辑时钟。成功后按输入顺序仅写入尚未满足的目标端点，固定返回键序 `op,items,ok`；`items` 按输入顺序排列，每项固定键序 `id,endpoint`，`endpoint` 为对应 `target` 对象或 `null`，`ok` 恒为 `true`。相同请求重报（含部分项已处于目标的重试）逐字节一致。
- `eb` 只改变各后端当前端点：不迁移、关闭或重新调度活动连接，不消费令牌、配额或等待队列。已有连接继续由 `fw` 返回其建连时快照；后续连接沿用更新后的端点固化新快照。变更结果继续由 `ce`、`se` 导出并可由 `ci`、`si` 恢复（登记端点本就在配置与检查点内）。单次时间与额外空间上界均为 O(N)，N 为 `items` 项数，仅使用 Python 标准库；`run`、`record`、`replay` 对成功、幂等和失败保持固定键序紧凑 UTF-8 JSON、单末尾换行及逐字节确定性。

## 批量后端端点切换只读预演：ez

在不执行 `eb` 的前提下，一次预演整批条件切换：既识别条件冲突，又预测切换对旧端点活动连接的影响。只读：除推进时钟外不改变任何运行态；即使存在冲突也返回完整预演明细。`ep`、`ey`、`eb`、`eq`、`er`、`ex`、`fw` 及所有既有入口行为不变。

- `ez`：输入严格按键序 `op,items,now` 排列（键须按此序出现）。`items` 为 1 至 1000 项数组；每项严格按键序 `id,base,target` 排列，`id` 沿用后端标识且同一 `id` 在数组中不得重复。`base` 与 `target` 均为 `null` 或按键序 `host,port` 排列的对象，含义与校验逐项沿用 `eb`/`ey`（`null` 表示未登记端点，对象沿用 `ep` 对规范 IP 字面量与 1 至 65535 非 bool 整数端口的校验）。`now` 为 0..10⁹ 的非 bool 整数并进入全局非递减显式时钟。
- 处理顺序固定为：先完整校验整份输入（字段、键序、容器、数量、重复 id、端点结构、编码、数值范围），再按 `items` 顺序确认所有后端存在，最后按操作起始快照计算；任一字段错误报 INPUT/2，任一未知后端报 BACKEND/3，INPUT 优先于 BACKEND。存在冲突不是错误：除时钟外不写入任何状态。
- 返回固定键序 `op,now,ready,items,summary`。`items` 按请求顺序排列，每项固定键序 `id,current,target,status,total,fresh,stale`：`current` 为该后端当前端点（`null` 或 `host,port` 对象），`target` 回显规范化候选值；`current` 等于 `target` 时 `status` 为 `unchanged`，否则等于 `base` 时为 `applicable`，其余为 `conflict`。仅当整批无 `conflict` 时 `ready` 为 `true`。
- `total` 为该后端活动连接数；`fresh` 与 `stale` 按建连时端点快照是否等于 `target` 划分，连接无快照时按 `null` 比较（故 `target` 为 `null` 时无快照连接计入 `fresh`），恒有 `total=fresh+stale`。
- `summary` 固定键序 `total,applicable,unchanged,conflict,connections,stale`：前四项为 `items` 项数及三种状态计数，`connections` 为各项 `total` 之和，`stale` 为各项 `stale` 之和。
- 字段、键序、容器、数量、重复 id、编码、端点结构、范围或时钟倒退返回 INPUT/2；后端未知返回 BACKEND/3，INPUT 优先于 BACKEND。失败无 stdout 且整批回滚（含同批此前变化与逻辑时钟）。`ez` 成功只推进 `now`，不改变端点、连接、粘性、调度、指标、告警或修订。单次时间 O(N+C)、额外空间 O(N)，N 为 `items` 项数、C 为活动连接数；相同初态相同输入产生逐字节一致的固定键序紧凑 UTF-8 JSON，`run`、`record`、`replay` 结果一致，`se` 和 `si` 保持时钟恢复语义，仅使用 Python 标准库。

## 批量后端端点条件切换与旧连接清理：ej

把一批后端的条件端点切换与指定时间边界内的旧连接清理合并为一次原子操作：全部前提成立时才统一切换并清理，任一项不满足则整批不生效。`ep`、`ey`、`eb`、`eq`、`ec`、`er`、`ex`、`ez`、`fw` 及所有既有入口行为不变。

- `ej`：输入严格按键序 `op,items,before,now` 排列（键须按此序出现）。`items` 为 1 至 1000 项数组；每项严格按键序 `id,base,target` 排列，`id` 沿用后端标识且同一 `id` 在数组中不得重复。`base` 与 `target` 均为 `null` 或按键序 `host,port` 排列的对象，含义与校验逐项沿用 `eb`/`ey`（`null` 表示未登记端点，对象沿用 `ep` 对规范 IP 字面量与 1 至 65535 非 bool 整数端口的校验）。`before`、`now` 均为 0..10⁹ 的非 bool 整数且 `before≤now`，`now` 进入全局非递减显式时钟。
- 处理顺序固定为：先完整校验整份输入（字段、键序、容器、数量、重复 id、端点结构、编码、数值范围与 `before` 关系），再按 `items` 顺序确认所有后端存在，最后按操作开始时的快照统一判定条件；任何阶段都不得提前写入。每一项在当前端点等于 `target` 时视为已满足，不要求 `base` 仍匹配且不重写登记；否则当前端点必须逐值等于 `base` 才允许切换。只要任一后端的当前端点既不等于 `base` 也不等于 `target`，整项操作返回 STATE/4；任一未知后端返回 BACKEND/3。判定优先级固定为 INPUT、BACKEND、STATE。
- 条件全部成立后，先按输入顺序把尚未到达 `target` 的后端端点原子更新到 `target`（`target` 为 `null` 时清除登记）；再按全局建连顺序单遍处理活动连接：仅 `items` 所列后端中 `opened≤before`（`opened==before` 关闭）且建连端点快照不等于该项 `target` 的连接被关闭，删除连接与快照、逐条递减并发。无快照按 `null` 比较，因此 `target` 为 `null` 时无快照连接保留；快照等于 `target` 的连接（含建连时即指向新端点的连接）永不关闭。`items` 后端之外或 `opened>before` 的连接均不动。
- 清理使排空状态 D 的后端因本次关闭失去最后连接时，沿用 `ec`/`ex`/`close` 规则转为 X、`end=now`，`forced` 维持原值不变；不消费令牌、配额或等待队列，也不触发重新调度。无符合连接时为成功的确定性空操作。
- 成功返回固定键序 `op,now,items,closed,ok`；`items` 按请求顺序排列，每项固定键序 `id,endpoint,cids`，`endpoint` 为规范化后的 `target` 对象或 `null`，`cids` 按全局关闭次序列出该后端本次关闭的连接；`closed` 为全部 `cids` 数量，`ok` 恒为 `true`。首次提交与相同请求重报（含部分项已处于目标的重试）逐字节一致。
- 字段、键序、编码、类型、范围、`before` 关系或时钟倒退返回 INPUT/2，未知后端返回 BACKEND/3，条件冲突返回 STATE/4；失败不产生 stdout，并回滚同批此前变化、时钟、端点和连接。单次时间与额外空间均为 O(N+C)（N 为 `items` 项数、C 为活动连接数），仅使用 Python 标准库；`run`、`record`、`replay`、`se`、`si` 与固定键序 JSON、单末尾换行和逐字节确定性保持兼容，既有操作行为不变。

## 全池唯一端点切换预约：es / en / eu / ei

在既有即时端点切换及旧连接清理（`ep`/`ey`/`eb`/`ej` 等）之上，提供全池唯一、可查询、可取消、到期条件生效的端点切换预约：先保存一份规范化切换快照，之后仅凭预约身份（摘要与触发时刻）在指定时刻原子执行 `ej` 同式的切换与清理。

- `es`：严格接收按键序 `op,items,before,at,now` 排列的五键（键须按此序出现，乱序报 INPUT）。`items` 沿用 `ej` 的全部规则：1..1000 项数组，每项严格按键序 `id,base,target` 排列，同一 `id` 不得重复，`base`/`target` 均为 `null` 或按键序 `host,port` 排列的对象（`null` 表示未登记端点，对象沿用 `ep` 的规范 IP 字面量与 1..65535 非 bool 整数端口校验）。`before`、`at`、`now` 均为 0..10⁹ 的非 bool 整数，且 `now≤at`、`before≤at`；`now` 进入全局非递减显式时钟（倒退报 INPUT/2），`at` 与 `before` 不推进时钟。
  - 先完成整份输入校验，再按 `items` 顺序确认全部后端当前存在（首个未知后端报 BACKEND/3）；校验全部后端引用后保存规范化快照，**不**检查端点条件、**不**切换端点、**不**关闭连接。全池至多一个预约：保存时整体替换既有预约；相同身份（同摘要、同 `at`）重报为幂等空操作、不重写快照，相同输入逐字节一致。
  - `digest` 为固定键序对象 `{"items":items,"before":before}`（项键序 `id,base,target`，端点为 `null` 或 `host,port` 对象）的紧凑 UTF-8 JSON（`ensure_ascii=False`、分隔符 `,:`、无末尾换行）的 SHA-256 小写十六进制值。成功固定返回键序 `op,digest,at,ok`，`ok` 恒为 `true`。
- `en`：仅接收 `op` 一个键，不推进时钟、为只读查询。固定返回键序 `op,pending,digest,at,before,items`；有预约时回显身份与按保存顺序排列的规范化项，无预约时 `pending=false`，其后四项依次为 `null`、`null`、`null`、空数组。
- `eu`：严格接收按键序 `op,digest,at,now` 排列的四键。`digest` 为小写 64 位十六进制 SHA-256（仅用于匹配预约身份，不要求对应当前端点）；`at`、`now` 均为 0..10⁹ 非 bool 整数，`now` 进入全局非递减时钟，`at` 仅作身份、不推进时钟。无预约时为成功的幂等空操作，固定返回 `op,digest,at,cancelled` 且 `cancelled=false`；身份（摘要与 `at`）匹配时删除预约并返回 `cancelled=true`；有预约但身份不符报 STATE/4 并保留预约。取消只删除预约，不切换端点、不动连接。
- `ei`：使用与 `eu` 相同的四字段。仅在身份匹配且 `now≥at` 时，按**操作开始快照**执行：端点前提、连接筛选与关闭顺序、排空 D 末连接迁移（转 X、`end=now`、`forced` 维持原值）和成功结果全部沿用 `ej`——按输入顺序写入尚未到达 `target` 的端点，再按全局建连顺序单遍关闭 `items` 后端中 `opened≤before` 且建连快照不等于该项 `target` 的连接，不消费等待队列、令牌或配额，也不触发重新调度。成功固定返回键序 `op,now,items,closed,ok`（项键序 `id,endpoint,cids`，与 `ej` 同构，仅 `op` 为 `ei`），并删除预约。
  - `now<at`（过早）、无预约、身份不符或端点前提冲突（某后端当前端点既不等于其 `base` 也不等于 `target`）均报 STATE/4 且保留预约；身份与时刻校验通过后，若快照中的后端在生效时已被删除，报 BACKEND/3（BACKEND 先于端点条件 STATE）。
- 字段集合、键序、编码、摘要格式、容器、数量、重复 id、数值范围、时间关系或时钟倒退统一报 INPUT/2；建立时引用未知后端报 BACKEND/3；错误优先级固定为 INPUT、BACKEND、STATE。任何失败均无 stdout，并原子回滚逻辑时钟及全部业务状态（预约、端点、连接等）。
- `se` 导出端点切换预约（顶层段 `ep_switch`，无预约为 `null`；有预约时键序 `digest,at,before,items`），`si` 恢复后的行为与直接继续逐字节一致；`sd`、`sx`、`sm` 按既有检查点规则观察该段差异。成功的 `ci`、`cb`、`cu`、`ca` 清除端点预约，其他操作不改变它。`es`、`en` 为 O(N)，`eu` 为 O(1)，`ei` 为 O(N+C)（N 为预约项数、C 为活动连接数）。仅用 Python 标准库，不读系统时间、不联网；`run`、`record`、`replay` 保持固定键序紧凑 UTF-8 JSON、单末尾换行和逐字节一致，既有公开行为不变。

## 全池端点切换预约条件替换：eo

在 `es` 无条件覆盖之外提供并发安全的改期或换内容：调用方依据 `en` 读到的旧预约摘要与触发时刻提交候选，仅当预约仍是旧身份（或已经是该请求的新身份）时才写入，避免持有旧读数的调用覆盖较新的预约。`es`、`en`、`eu`、`ei`、`ej` 的既有语义不变。

- `eo`：只接受严格键序 `op,base,base_at,items,before,at,now`（键须按此序出现）。`base` 为 `en` 读到的旧预约小写 64 位十六进制 SHA-256 摘要（格式同 `eu.digest`，仅用于匹配）；`base_at` 为旧触发时刻；`items`、`before`、`at`、`now` 沿用 `es` 的校验与含义（items 为 1..1000 项、每项键序 `id,base,target`、同一 id 不重复，base/target 为 `null` 或 `host,port` 对象；均为 0..10⁹ 非 bool 整数），且 `now≤at`、`before≤at`；`now` 进入全局非递减显式时钟（倒退报 INPUT/2），`at` 与 `before` 不推进时钟。
  - 先完整校验输入，再按 `items` 顺序确认候选引用的后端当前存在（首个未知后端报 BACKEND/3）；校验全部后端引用后才读取当前预约，不检查端点条件、不切换端点、不关闭连接。候选摘要按 `es` 同式对规范化 items 与 before 的固定键序对象紧凑 UTF-8 JSON 计算。
  - 当前没有预约返回 STATE/4；当前预约既不等于 `base`/`base_at` 指定的旧身份，也不等于候选摘要与 `at` 指定的新身份时返回 STATE/4 并保留原预约。旧身份匹配时原子替换为候选快照；当前已等于新身份时视为幂等重报——即使旧身份已过期（预约已被另一请求改成新值）也成功，且不得重写状态。保留 `es` 的无条件覆盖语义不变。
- 成功固定返回键序 `op,base,digest,at,ok`：`base` 回显请求的旧摘要，`digest` 为候选摘要，`at` 回显新时刻，`ok=true`；首次替换与相同请求重报必须逐字节一致。
- `eo` 只改变待生效预约：不切换端点、不关闭或调度连接，也不消费队列、令牌或配额。替换后 `en` 展示新内容，`eu` 与 `ei` 只能凭新摘要与新触发时刻操作，旧身份不再有效。
- 字段集合、键序、编码、摘要格式、容器、数量、重复 id、端点结构、数值范围、时间关系或时钟倒退统一返回 INPUT/2；候选中首个未知后端返回 BACKEND/3，优先级固定为 INPUT、BACKEND、STATE。任何失败都无 stdout，并回滚同批此前变化和逻辑时钟。`se` 与 `si` 完整保存和恢复替换结果（仍为 `ep_switch` 段），`sd`、`sx`、`sm` 继续按既有 `ep_switch` 段观察差异。单次时间与额外空间上界 O(N)，N 为 items 项数与规范化编码长度之和；仅使用 Python 标准库；`run`、`record`、`replay` 保持固定键序紧凑 UTF-8 JSON、单末尾换行和确定性结果，其他公开入口及错误优先级不变。

## 全池端点切换预约只读预演：ev

在 `ei` 真正生效前，让调用方仅凭预约身份（摘要与触发时刻）在操作开始快照上查看预约的可执行性与切换将影响的连接：未到期、后端删除和端点冲突都作为成功预演的状态而非错误。只读：除推进 `now` 外不改变任何运行态；`es`、`en`、`eu`、`ei`、`eo`、`ej` 及所有既有入口行为不变。

- `ev`：只接受严格键序 `op,digest,at,now`（键须按此序出现）。`digest` 为小写 64 位十六进制 SHA-256（格式同 `eu.digest`，仅用于匹配预约身份，不要求对应当前端点）；`at`、`now` 均为 0..10⁹ 的非 bool 整数，`now` 进入全局非递减显式时钟（倒退报 INPUT/2），`at` 仅作预约身份、不推进时钟。
- 字段集合、键序、编码、摘要格式、类型、范围或时钟倒退统一返回 INPUT/2，且 INPUT 判定先于预约匹配；当前无预约，或摘要、触发时刻任一不匹配返回 STATE/4。失败无 stdout 并回滚整批（含同批此前变化与逻辑时钟）。
- 身份匹配后，ev 按操作开始快照检查预约保存的 `items` 和 `before`：即使整体不可执行也返回完整预演明细。返回固定键序 `op,digest,at,now,ready,status,items,closed`。
  - 整体 `status` 在 `now<at` 时为 `EARLY`；否则存在已删除后端时为 `MISSING`；再否则存在端点冲突时为 `CONFLICT`；仅全部后端存在且当前端点等于各自 `target` 或 `base` 时为 `READY`。`ready` 仅在 `READY` 时为 `true`。
  - `items` 保持预约顺序，每项固定键序 `id,exists,current,target,status,cids`。删除项取 `exists=false`、`current=null`、`status=MISSING` 和空 `cids`；其余按当前端点等于 `target`、等于 `base` 或均不等分别取 `UNCHANGED`、`APPLICABLE`、`CONFLICT`。
  - 存在的后端按全局建连顺序列出 `opened≤before`（`opened==before` 计入）且建连端点快照不等于该项 `target` 的 cids，无快照按 `null` 比较（故 `target` 为 `null` 时无快照连接不计入）；`closed` 为全部 cids 数量。整体不可执行时仍报告其余可计算项。
- `ev` 成功只推进 `now`：不改端点、连接、排空状态、预约、队列、令牌、配额、指标或告警，不关闭连接或删除预约；后续 `ei` 仍按执行时状态判定。相同初态和输入产生逐字节一致的固定键序紧凑 UTF-8 JSON 与单个末尾换行，`run`、`record`、`replay` 结果一致。单次时间和结果空间为 O(N+C)，N 为预约项数、C 为活动连接数；`se`、`si` 保持预约与时钟恢复语义；仅使用 Python 标准库，其他公开入口保持原有行为。

## 测试

    python -m unittest discover
