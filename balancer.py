#!/usr/bin/env python3
"""平滑加权轮询（smooth weighted round-robin）后端池，仅用标准库。

入口：python balancer.py run
stdin 为 UTF-8 JSON：{"ops": [...]}，成功时 stdout 输出
{"results":[...],"backends":[...]}（无多余空白，末尾恰好一个换行）。
任何错误都不产生 stdout，只向 stderr 写一行 {"error":"..."} 并以约定码退出。

连接管理：open/close/get 按 cid 跟踪活动连接；open 选活动连接数最少的
后端（并列取最早加入者），remove 只允许删除无连接的后端。open/close/probe
的 now 共用同一时钟，依序不得递减。

健康探测：后端新增即 healthy，默认阈值 fail=3、success=2，连续计数从 0 起。
hset 只替换阈值并清连续计数，不改健康状态；probe 按 ok 递增对应连续计数
（封顶阈值）并清零另一项，healthy 连续失败达 fail 转 unhealthy，unhealthy
连续成功达 success 转 healthy，迁移时计数与平滑当前权重一并清零。同一
(id, now, ok) 的 probe 重报幂等；同 (id, now) 而 ok 不同属冲突重报，报
INPUT。pick/open 只作用于 healthy 后端；无 healthy 后端时报 STATE。

权重预热与调权：三键 add（op,id,weight）视为 d=0，目标权重立即生效，旧
行为不变；五键 add 键集 op,id,weight,d,now，d=0 立即取目标，d>0 自 now
起从 1.00 线性预热到目标权重，end=start+d，到达后取目标，插值向下取整到
0.01。ws 同键集，调权自当前有效权重线性过渡到新目标并清零平滑 current；
unhealthy 时调权报 STATE。add/ws 同参重报幂等，同 (id,now) 异参报 INPUT，
重复 add、未知 ws/wg 报 BACKEND。后端 unhealthy 期间有效权重固定 0.00；
probe 恢复 healthy 时以该 probe 的 now 从 1.00 按原 d 重启预热。wg 键集
op,id,now，返回键序 op,id,target,effective,stage,start,end：target 为整数
目标权重，effective 为两位小数字符串，stage ∈ warm/steady/unhealthy，仅
warm 带整数 start/end，其余为 null。pick 以当前有效权重执行旧平滑算法；
open 按连接数升序、有效权重降序、加入顺序选取；chash/route 忽略权重；
backends.weight 始终为目标整数权重。

熔断器：cs 键集 op,id,n,m,r,w,q（n,q,r ∈ [1,100]，m ∈ [1,n]，w ∈ [1,10^9]，
均非 bool 整数），首配或异参重配置 C 并清空窗，同参重报幂等；配置可选，
未配后端的 cr/cg 报 STATE，remove 后重加即回到未配。cr 键集 op,id,ok,now
（ok 仅 bool）：C 保留最近 n 次报告，样本数 ≥ m 且失败数×100 ≥ r×样本数
转 O 并记 next=now+w；O 且 now<next 的 cr 报 STATE，否则先转 H；H 中失败
立即重开并重算 next，连续 q 次成功转 C 并清窗。同 (id, now, ok) 的 cr 重报
幂等，同 (id, now) 异 ok 报 INPUT。cg 键集 op,id,now，触发 O 到期转 H，返
回键序 op,id,state,count,fail,reason,next,used：state ∈ C/O/H，count/fail
为窗口样本与失败数，reason 非 C 为 rate 否则 null，next 仅 O 为整数否则
null，used 仅 H 为已报告数否则 0。cs/cr 返回 op,ok。cr/cg 的 now 纳入同一
非递减时钟。pick/open/route 只选 healthy 且熔断状态为 C（含未配）的后端；
粘性目标非 C 时沿原环迁移且不迁回。

一致性哈希：chash 配置每个 healthy 后端的虚拟节点数 vnodes（1..1024），
同值幂等、异值生效。每个 healthy 后端为 i=0..vnodes-1 生成令牌
SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，摘要按 256 位大端无符号数
排序（并列按加入顺序、i 升序）构成哈希环。route 哈希 UTF8(key) 取首个
不小于它的令牌，越界回绕；首见 key 建立映射，目标 healthy 时命中
（sticky），增删后端或改 vnodes 不迁移，目标不可用时依环迁移且不迁回
（remapped）。route 不改连接数；未 chash 或无 healthy 后端时报 STATE。

限时粘性：ss 键集 op,ttl（ttl ∈ [1,10^9] 非 bool 整数），首配或同值返回
op,ok=true，异值报 STATE/4；ss 不携带 now，O(1)，登记值随 ce/ci 导出导入。
route 新增
三键键集 op,key,now（now 为非负非 bool 整数，纳入共用非递减时钟），旧二键
行为与结果不变；三键 route 未 ss、未 chash 或无可选后端报 STATE/4。粘性映射
为 key->[b,e]（b=backend、e=expires）：二键 route 无项按环写 [b,null]，
b 可用即命中且 e 不变，否则重选 [b',null]；三键 route 无项选 b，e=null 时
保留可用 b（不可用则重选），e 非 null 时仅 now<e 且 b 可用才保留、e 不变，
否则重选；无项、e=null 或重选均写 e=now+ttl。三键结果键序
op,key,backend,sticky,remapped,expired,expires：sticky=沿用已有 b，
remapped=已有项且重选后新旧 b 不同，expired=原 e 非 null 且 now>=原 e
（重选回同一 b 仍为 true），其余为 false。ss 后 la、oa、ot 按各自 now 用
三键规则，未 ss 保持旧义（e=null）；粘性映射空间 O(S)，路由沿用上界。

优雅摘除：ds 键集 op,id,t（t ∈ [1,10^9] 非 bool 整数）登记排空时限，
同值幂等，D 时改值报 STATE，否则覆盖。dr/du/dg 键集 op,id,now，now
为非负非 bool 整数，纳入共用非递减时钟。已 ds 的 A 执行 dr：start=now、
deadline=now+t、forced=0，有连接转 D，无连接转 X 且 end=now；D/X 再
dr 幂等。D 不参与 pick、open 与新 route 映射，连接仍可 close，原粘性
route 仍命中；最后连接关闭即转 X，end=close.now。dg 在 D 且
now>=deadline 时强关其全部连接，forced=关闭数，转 X，end=deadline；
X 的旧粘性按原环迁移。du 转 A，取消 D 时 end=now，不恢复已强关连接，
调度仍要求健康且熔断为 C，慢启动不重置。ds/dr/du 返回 op,ok；dg 返回
op,id,state,connections,start,end,deadline,forced，state ∈ A/D/X
（可用/排空/已摘除），未开始时三时间为 null。未知 id 报 BACKEND，
未 ds 的 dr/du/dg 报 STATE。
全池排空投影 dq：精确键序 op,now，now 为非负非 bool 整数并纳入共用非
递减时钟；只读、仅推进时钟，时间 O(B)、结果空间 O(B)。返回键序
op,now,items，items 按后端加入序列出 D/X 后端，项键序
id,state,connections,start,deadline,end,due,remaining,forced；due 仅
D 且 now>=deadline 为 true，D 的 remaining=max(deadline-now,0)，X 的
remaining 恒为 0。空池、无 D/X 后端均返回空 items，不报错。
全池批量到期 dx：精确键序 op,now，now 义同 dq；处理集合为 D 且
deadline<=now 的后端，按加入顺序处理，各后端连接按全局建连顺序关闭，
沿用 dg 强制到期联动（删连接与端点快照、并发归零、转 X、end=deadline、
forced=本次关闭数），不消费等待队列、令牌或配额。返回键序
op,now,backends,closed，backends 只含本次到期项，项键序
id,deadline,cids（cids 按关闭序），closed 为全部 cids 数量；无到期项
返回空数组与 0，同一 now 重复执行为确定性空操作，天然原子。时间
O(B+C)、结果空间 O(B+C)。两操作键集合、键序、now 类型/范围或时钟倒退
统一报 INPUT/2，失败无 stdout 并回滚同批此前变化和逻辑时钟。

令牌桶：ls 键集 op,scope,id,r,b,now，scope ∈ B/C/S（后端/客户端/
服务类），id/c/s/key 均为非空 UTF-8 串，r,b ∈ [1,10^9] 非 bool 整数，
now ≥ 0 纳入共用非递减时钟；B 桶的 id 须为现存后端，否则 BACKEND。
桶以 (scope,id) 唯一，新桶满令牌起步，同 (r,b,now) 重报幂等（不补充
不推进时钟），其余 ls 一律重配置并置 t=b、at=now；remove 同步删除其
B 桶。la 键集 op,c,s,key,now 或 op,c,s,key,bc,cc,sc,now：旧键集等价
于三项成本均为 1；bc/cc/sc 为 B/C/S 三桶各自的扣减成本，均为
[0,10^9] 非 bool 整数且至少一项非零，全零或非法报 INPUT。先按原
route 语义选后端（含粘性建立与迁移，未配环或无可选后端报 STATE），
再检查该后端 B 桶、客户端 C 桶（以 c 标识）、服务类 S 桶（以 s 标
识），未配置即不限制也不扣减。各在配桶先作
t=min(b,t+(now-at)*r)、at=now，均有 t>=对应成本才各减对应成本，
否则 RATE/6（无 stdout、整批原子）；成功返回键序 op,backend,ok，
ok=true。
lg 键集 op,scope,id,now，按同样规则补充但不消费，查未配置桶报 STATE；
返回键序 op,scope,id,r,b,t,at，值均为整数。ls 返回 op,ok。桶操作
O(1)，la 继承 route 的复杂度上界，空间 O(B+K)。

固定窗口配额：qs 键集 op,scope,id,limit,span,now，scope ∈ B/C/S，id
同 ls，limit ∈ [1,10^18]、span ∈ [1,10^9]、now ∈ [0,10^9] 非 bool
整数，now 纳入共用非递减时钟；B 配额的 id 须为现存后端，否则 BACKEND。
配额以 (scope,id) 唯一，同 (limit,span,now) 重报幂等（不推进窗口、不
清已用），其余 qs 一律重配置并置 window=now//span、used=0，返回键序
op,ok，ok=true。qg 键集 op,scope,id,now：跨窗先置 window=now//span
并清 used，返回键序 op,scope,id,limit,span,window,used,remaining；
查未配置配额报 STATE。la 另按 bc/cc/sc 检查对应 B/C/S 配额：未配置
不限，在配配额先按跨窗规则推进；令牌与配额全足才原子扣减（配额 used
加对应成本），否则 RATE/6。remove 同步删除其 B 配额；ci/cb 成功按
配置 quotas 原子重建（v1..v7 视为空即清空，ce 导出与 ci/cb 载入见
配置热加载与提交回滚段）；非法键集、类型、范围、编码或时钟倒退报
INPUT/2。qs 精确重报（limit、span、now 同上次配置）豁免时钟倒退：
时钟已前进仍返回键序 op,ok（true），不回拨时钟并保留 window、used；
其他旧时刻 qs/qg 仍报 INPUT/2。qs/qg 与 la 的新增判定均 O(1)，空间 O(Q)。

排队接纳：os 键集 op,cap,q,ttl（均 1..10^6 非 bool 整数），依次为每后端
连接上限、FIFO 容量、等待时限；首配或同参返回 op,ok，异参报 STATE。
oa 键集 op,cid,flow,c,s,key,now 或
op,cid,flow,c,s,key,bc,cc,sc,now：cid/flow 同 open，c/s/key 同 la，
bc/cc/sc 同 la 的成本校验（旧键集等价于三项均为 1），now
纳入共用非递减时钟。活动或排队中 cid 重复报 CONNECTION。先按 la 的路由
语义选后端（未 chash 或无可选后端报 STATE），路由后按 B 后端、C 客户端、
S 服务类检查在配令牌桶与在配固定窗口配额（未配置项不限）：各桶先补充、
各配额先按 window=now//span 推进（跨窗清零 used），目标另须排空 A 且
连接数 < cap；仅当各桶令牌不少于对应成本、各配额满足 used+成本≤limit
时才原子扣令牌、配额 used 加成本并建连（opened_at=now），否则三项成本
随请求入队阻塞，返回键序
op,cid,state,backend：接纳为 A 加后端 id，阻塞为 Q 加 null；令牌或配额
不足只入队，不报 RATE，队满尾拒绝或 P 态背压仍报 OVERLOAD/7。ot 键集
op,now：先删除全部 now ≥ 入队 now+ttl 的排队项（到期不扣令牌、不扣配额），
再自队首逐项以入队成本按 oa 规则重试：逐项以本次 now 补充桶并推进固定窗
（opened_at=now，接纳才扣令牌、增 used 并建连，前序扣减对后续项可见），
至首个阻塞项连同其已推进未扣减的桶/配额状态整体放回队首即停，
返回 op,expired,admitted，两
数组均按 FIFO 列 cid；ot 至多 q 项、每项至多一次 route，空间 O(q)。
og 键集 op，返回
op,queue，queue 为 FIFO cid 数组。oc 键集 op,cid，cid 沿用非空字符串校验，
须已 os 且 cid 当前在等待队列；成功时 O(1) 从任意位置删除，返回键序
op,cid,ok，ok=true，不扣减或返还令牌、不建连接，入队路由已产生的粘性映射
保留，其余项 FIFO 相对次序不变。oa/ot/og/oc 未 os 报 STATE（oa 另含未
chash）；oc 非法键集或 cid 报 INPUT/2，cid 不在队列（活动中或从未存在）报
CONNECTION/5，按 INPUT、STATE、CONNECTION 顺序判定。失败批次回滚队列顺序、
成员索引与背压状态；入队、ot 接纳或过期、oc 取消及 ci 成功清队列均同步成员
索引，oa 查重与 oc 删除均 O(1)，og 仍 O(q)。非法键、类型、范围、编码或时钟
倒退报 INPUT。qp 精确键序 op,mode（键须按此序出现），mode 为
F/S/P：登记 ot
的出队策略，默认 F，同值幂等，返回键序 op,ok（ok=true）；键序、类型或值
非法报 INPUT/2。F 保持 ot 遇首个阻塞项即停；S 模式 ot 先按原规则删除到期
项，再将其余项按 FIFO 各检查一次：可接纳项沿用路由、容量、令牌桶与配额
规则扣减并建连，阻塞项移至队尾并保持相对次序、继续检查后项；expired、
admitted 按原 FIFO 顺序，各项使用同一 ot.now，前项扣减对后项可见，失败批
回滚运行态。ci/cb 按 queue.dequeue 原子载入（v1..v8 规范化为 F）；登记
值随 ce 经 version=11 的 queue.dequeue 导出；rp/qp 提交（ci/cb）后的后续
修改仅影响当前 ce，不改已存提交快照；record/replay 逐字节覆盖。
S 模式 ot 时间 O(qBV log(BV))、额外空间 O(q)。

老化优先级登记：qa 精确键序 op,step,items（键须按此序出现），step 为
1..10^9 非 bool 整数，items 至多 1000 项；项精确键序 s,p（键须按此序出
现），s 为互异非空 UTF-8 串，p 为 0..10^9 非 bool 整数。原子替换整份登
记，返回键序 op,ok（ok=true）；不推进时钟、不动队列与任何运行态、不要求
已 os；未登记服务类在 P 式 ot 中基础优先级按 p=0。qa 键序、结构、字段或
重复 s 非法报 INPUT/2；其余错误不变，失败批回滚。qp.mode 新增 P：无 qa
登记选 P 报 STATE/4（先于任何变更），P 同值幂等。P 式 ot 先按 FIFO 删到
期项，再将其余项按
score=min(10^18,p+(ot.now-入队 now)//step) 降序、score 并列按 FIFO 序升
序各尝试一次（p 为该项服务类 s 的登记基础优先级，未登记为 0）；沿用路由、
容量、令牌桶、配额判定，仅接纳才扣减建连，阻塞项保持 FIFO 相对序。结果键
序 op,expired,admitted；expired 依 FIFO、admitted 依尝试序；F/S 模式结果
不变。P 不持久化：进入 P 时保留最近 F/S；P 时 ce、cl 的 queue.dequeue 仍
为该保留值且不导出 qa；qp 切 F/S 会更新该值并退出 P；ci/cb/ca 成功即清 qa
并采用所载 F/S。qa 为 O(P log P) 时间、O(P) 额外空间，P 式 ot 为
O(q log q+q BV log BV) 时间、O(q+BV) 额外空间，仅标准库，其他操作不变。

FIFO 满载策略：rp 精确键序 op,mode（键须按此序出现），mode 仅 T/H：T 为
队满尾拒绝（默认，即既有 OVERLOAD/7 行为），H 为队满头淘汰；同值幂等，
返回键序 op,ok（ok=true）。rg 键序仅 op，只读，返回键序
op,mode,evicted,last：evicted 为累计淘汰数（0..10^18 整数，封顶累加），
last 为最近淘汰的 cid 或 null。oa 需排队时先判背压：P 态报 OVERLOAD/7
且无变更；N 态队未满照旧入队；队满时 T 报 OVERLOAD/7，H 则 O(1) 删除
FIFO 队首并把新项放队尾——旧项不耗或返还令牌、配额，入队路由已产生的
粘性映射保留，淘汰 cid 可立即复用，其余项 FIFO 相对次序不变，新入队
照常更新 oh、peak 与背压。H 模式 oa 结果键序
op,cid,state,backend,evicted：A 与 Q 的 backend 均为本次路由选中的后端
id，evicted 仅头淘汰时为旧 cid，立即接纳或普通入队为 null；T 模式保持
原四键（Q 的 backend 为 null）。rp/rg 键序、类型或值非法报 INPUT/2。ci/cb
按 queue.full 原子载入（v1..v8 规范化为 T）并清空队列、置 evicted=0、
last=null，失败批回滚队列、指标、粘性、策略与计数；登记策略随 ce 经
version=11 的 queue.full 导出（evicted、last 不导出），提交后的 rp 修改
仅影响当前 ce，不改已存提交快照。rp、rg 及新增判定均 O(1)，额外空间
O(1)，仅用标准库。

等待队列只读投影：oq 精确键序 op,now（键须按此序出现），now 为 0..10^9
非 bool 整数并进入共用非递减时钟；未 os 报 STATE/4，键序、now 类型/范围
或时钟倒退报 INPUT/2。结果键序 op,items；items 按等待队列 FIFO，项键序
cid,backend,expires,expired,blocked：backend 为字符串或 null，
expires=入队 now+os.ttl，expired 为 bool，blocked 为字符串数组。到期项
（now≥expires）不查环，取 null、true、["E"]；未到期项按其 key、c、s 与
入队三项成本对当前环、粘性、桶和配额作只读投影（各项独立从当前真实桶/
配额投影，前项不影响后项，不读写粘性映射）：环内无合格后端取 null、
false、["R"]，否则 backend 为投影目标，blocked 按 C、T、Q 序列出连接达
os.cap、按 now 只读补充后任一令牌桶令牌不足、推进固定窗后任一配额
used+成本>limit，无阻塞为 []。oq 除推进时钟外不改任何状态，失败批回滚；
环为全项共用的当前快照仅构建一次，时间 O(qBVlog(BV))、额外空间
O(q+BV)，仅用标准库；JSON、record/replay 逐字节契约与旧操作行为不变。

确定性滞回背压：bp 键集 op,low,high，low/high 为 [0,10^6] 非 bool 整数且
low<high≤os.q，未 os 报 STATE/4，非法键集、类型、范围或阈值关系报
INPUT/2。首配或异参重配按当前队长 >=high 置 P，否则 N；同参幂等且不改
状态。bp 返回键序 op,ok，ok=true。bq 键集仅 op，未 bp 报 STATE/4；返回
键序 op,state,queued,low,high,available：state 仅 N/P，其余为整数，
available=os.q-queued。启用后 oa 可立即接纳时仍按原规则扣令牌并建连；
本应排队时 P 态报 OVERLOAD/7 且无变更（不耗令牌、不入队），N 态照常入
队，队长达到 high 即转 P；队满仍 OVERLOAD/7。ot 照常先过期再自队首接
纳，处理完若 P 且队长 ≤low 则转 N，否则不变；oc 取消后 P 且队长 ≤low
立即转 N，其他状态不变。bp 登记值随 ce/ci 导出导入，ci 未携带时取消、
携带时置 N；bp、bq 及 oa 新增判定均 O(1)，ot 仍 O(q)，额外空间 O(1)。

过载分钟历史：oh 精确键序 op,from,to,now（键须按此序出现）；三数为
0..10^9 非 bool 整数，now 纳入共用非递减时钟，须 from≤to≤now//60 且
to-from<60；未配置 os 报 STATE/4。按 window=now//60 记账，全池一份，
只保留最近 60 窗，空窗不预建：oa 返回 A/Q 分别在其 now 窗增加
immediate/queued；ot 每个接纳/过期项在其 now 窗增加 dequeued/expired；
peak 取该窗入队后队长峰值。各计数为非负整数、封顶 10^18。返回键序
op,windows；windows 覆盖 from 至 to 所有窗并升序，项键序
window,immediate,queued,dequeued,expired,peak，空窗全 0。键序、类型、
范围、关系或时钟倒退报 INPUT/2，from 早于 max(0,now//60-59) 报
STATE/4。oh 只读；ci/cb 成功清历史，失败批回滚。记账 O(1)，ot 仍
O(q)，oh 为 O(R) 时间、O(60) 空间，仅标准库；紧凑 UTF-8 固定键序
JSON、末尾一换行及 record/replay 逐字节契约照常，其他子命令不变。

排队等待历史：等待项离队时取 d=离队时刻-入队 now：ot 接纳、过期分别记
admitted、expired（按 ot.now），oc 成功取消按当前逻辑时钟记 cancelled，
H 模式 oa 头淘汰按被淘汰项的 oa.now 记 evicted；仅成功离队记账，立即接
纳、失败批次与未成功的重试不记。事件归离队时刻 window=时刻//60，d 依次
落入 ≤0、≤1、≤10、≤100、>100 五桶，每桶计数封顶 10^18。全池一份，只
保留最近 60 窗，空窗不预建；ci/cb/ca 成功清空。wh 精确键序
op,from,to,now（键须按此序出现）；三数为 0..10^9 非 bool 整数，now 进入
共用非递减时钟，须 from≤to≤now//60 且 to-from<60。返回键序
op,windows；windows 覆盖闭区间并升序，项键序
window,admitted,expired,cancelled,evicted，四项均为 5 个非负整数数组，
空窗全零。键序、类型、范围、关系或时钟倒退报 INPUT/2；未配置 os 或 from
早于 max(0,now//60-59) 报 STATE/4。wh 除时钟外只读，失败批回滚。记账
O(1)，wh 为 O(R) 时间、O(60) 空间，仅标准库。

排队等待分位查询：wp 精确键序 op,kind,from,to,p,now（键须按此序出现）；
kind 仅 A/E/C/V，对应 admitted/expired/cancelled/evicted；from/to/now 为
0..10^9、p 为 1..100 的非 bool 整数，now 进入共用非递减时钟，须
from≤to≤now//60 且 to-from<60。汇总闭区间内该 kind 五桶（等待时长分桶
同 wh），每桶逐窗求和封顶 10^18；samples 为桶和再封顶 10^18。samples>0
时 rank=ceil(p*samples/100)，bucket 取累计首次不小于 rank 的桶（0..4），
upper 依次为 0/1/10/100/null；无样本则 rank=0，bucket 与 upper 为 null。
返回键序 op,kind,from,to,p,samples,buckets,rank,bucket,upper；buckets 为
五个非负整数数组。键序、kind、类型、范围、关系或时钟倒退报 INPUT/2；未
配置 os 或 from 早于 max(0,now//60-59) 报 STATE/4。wp 只推进时钟，失败批
回滚。时间 O(R)、空间 O(1)（R=to-from+1），仅标准库。

排队等待分位告警：wa 精确键序 op,kind,w,p,hi,lo,n,now（键须按此序出现），
kind 仅 A/E/C/V，对应 admitted/expired/cancelled/evicted；w/now 为 0..10^9、
p 为 1..100、hi/lo 为 0..4、n 为 1..60 的非 bool 整数，且 lo<hi。now 纳入
共用非递减时钟，须 max(0,now//60-59)≤w<now//60（w 窗已结束且在最近 60 窗
保留范围内）。取 wait_hist 中该 kind 在 [w,w] 单窗五桶按 wp 规则计算
samples、bucket、upper（单窗 wp 口径，桶上界 0/1/10/100/null）；无样本三者
为 0、null、null 且连续数清零。各 kind 均从 N 态开始并独立维护，首评固化
p/hi/lo/n；N 态连续 n 窗 bucket≥hi 转 A，A 态连续 n 窗 bucket≤lo 转 N，方
向不符清 run，转换后 run=0。后续 w 仅可同前或 +1；同窗同参返回缓存结果且
不推进状态机。结果键序
op,kind,w,p,state,samples,bucket,upper,run,changed；state 仅 N/A，changed
仅转换为 true。键序、kind、类型、范围、关系或时钟倒退报 INPUT/2；未配置
os、窗口越界、跳窗（含回退）或变参报 STATE/4。ci/cb/ca 成功清空告警，失败
批回滚。wa 时空 O(1)，仅标准库；紧凑 UTF-8 JSON、单末尾换行及 record/
replay 逐字节契约照常，其他操作不变。

多维限流历史：oa/ot 检查已配置桶或配额的 B 后端、C 客户端、S 服务类
时，按 window=now//60 记账：接纳则该窗该维 admitted 加 1、units 加该维
成本；未接纳且该维在配桶补充后令牌不足成本则 token 加 1，在配配额推进
后 used+成本>limit 则 quota 加 1，令牌与配额不足可同增；仅因目标非 A
或连接数达 cap 的容量阻塞不记。各计数封顶 10^18，失败批次不记。每维仅
保留最近 60 窗，空窗不预建；ls/qs 重配不清历史，remove 后同 id 重加及
ci/cb 成功清空。lh 精确键序 op,scope,id,from,to,now（键须按此序出现），
scope 仅 B/C/S，id 沿用 ls 的非空 UTF-8 串校验，from/to/now 为 0..10^9
非 bool 整数，now 进入共用非递减时钟，须 from≤to≤now//60 且
to-from<60；返回键序 op,scope,id,windows；windows 覆盖 from 至 to 所有
窗并升序，项键序 window,admitted,units,token,quota，空窗全 0。键序、
字段、关系或时钟倒退报 INPUT/2；B 的未知 id 报 BACKEND/3；该 id 既无在
配桶也无在配配额，或 from 早于 max(0,now//60-59) 报 STATE/4。lh 除时钟
外只读。记账 O(1)，lh 为 O(R)，空间 O(60K)，仅标准库。

限流热点查询：lt 精确键序 op,scope,from,to,k,now（键须按此序出现），scope
仅 B/C/S，from/to/now 为 0..10^9 非 bool 整数，k 为 1..1000 非 bool 整数，
now 进入共用非递减时钟，须 from≤to≤now//60 且 to-from<60；now 推进共用
时钟。汇总该 scope 全部在配（有桶或配额）标识在 from 至 to 闭区间内 lh 的
admitted、units、token、quota，各值逐窗求和并封顶 10^18，blocked=token+
quota 并封顶；按 blocked 降序、id 的 UTF-8 字节升序取至多 k 项。返回键序
op,scope,from,to,items；items 项键序 id,admitted,units,token,quota,blocked，
计数为非负整数，空窗全 0；该 scope 无任一在配标识报 STATE/4。键序、scope、
数值、关系或时钟倒退报 INPUT/2；from 早于 max(0,now//60-59) 报 STATE/4。
lt 除推进时钟外只读，失败批回滚；历史清理同 lh。时间 O(UR+UlogU)、空间
O(U)，U 为该 scope 在配标识数、R 为窗数，仅标准库。

限流告警：le 精确键序 op,scope,id,w,hi,lo,n,now（键须按此序出现），
scope 仅 B/C/S，id 沿用 ls 的非空 UTF-8 串校验，w/now 为 0..10^9、
hi 为 1..10^18、lo 为 0..(10^18-1)、n 为 1..60，皆为非 bool 整数且
lo<hi；now 进入共用非递减时钟，须 max(0,now//60-59)≤w<now//60（w 窗
已结束且在最近 60 窗内）。每标识（scope,id）独立滞回状态机：取该标识
lh 的 w 窗 token、quota，value 为二者之和并封顶 10^18；首评固化
hi/lo/n 并自 N 态起评，N 态连续 n 窗 value≥hi 转 A，A 态连续 n 窗
value≤lo 转 N，否则及转换后连续数清 0；w 此后仅同值或 +1，同窗同参
返回首评结果、不推进状态机。返回键序
op,scope,id,w,state,token,quota,value,run,changed；state 仅 N/A，
changed 仅转换时为 true。键序、字段编码/类型/范围/关系或时钟倒退报
INPUT/2；B 的未知 id 报 BACKEND/3；该标识既无在配桶也无在配配额、
窗口未结束或过旧、跳窗或变参报 STATE/4。ls/qs 重配保留告警状态；
remove 后同 id 重加清该 B 维告警，ci/cb/ca/cu 成功清全部，失败批次
回滚。le 时间 O(1)、额外空间 O(K)（K 为标识数），仅标准库；紧凑
UTF-8 固定键序 JSON、单换行及 record/replay 逐字节契约照常，其他
操作不变。

请求度量：mr 键集 op,id,ok,ms,retries,remaps,now，id 须现存否则 BACKEND，
ok 仅 bool，ms/retries/remaps/now 四数均为 [0,10^9] 非 bool 整数，now 纳入
共用非递减时钟。每后端按 window=now//60 只保留当前窗统计，换窗即全部清零；
每报 requests 加 1，ok=false 时 errors 加 1，并累加 retries/remaps；ms 按
上界 [1,10,100,1000] 落入五整数桶（≤1、≤10、≤100、≤1000、>1000），各计数
封顶 10^18。mr 返回 op,ok，ok=true。mg 键集 op,id,now，返回键序
op,id,window,requests,qps,concurrency,errors,error_rate,latency,retries,
remaps,removed：window=now//60，concurrency 为活动连接数，latency 为五整数
桶，qps=requests/60、error_rate=100*errors/requests（零请求为 0）均下截为
两位定点串；removed 依次取 drain（D/X）、health（unhealthy）、circuit（熔断
非 C）、fault（fs/fb/fp 登记的时间线在 now 有活动段：D 恒为故障，F 仅于
((now-a)//v)%2=0 相位为故障；S、F 非故障相位、段间隙及无前述状态为
null），否则 null；
查询时已跨入新窗（含从未 mr）按零计且不改存储。每次 fx 完成只追加一次同构
度量、不新增结果项：归属 id 取 fx 结果 backend，backend 为 null 时取环遍历
首个后端，环内无候选不记；字段为 ok=(state 为 A)、ms=latency、
retries=remaps、remaps=remaps、now=fx.now。mr 与 fx 自动度量按操作顺序
累加；失败批次不留度量。remove 后重加统计归零。mr/mg 均 O(1)，空间 O(B)。

度量历史：每后端按 window=now//60 分窗保留最近 60 窗；mr、fx/fr 自动
度量写入所属窗，各窗沿用既有封顶值、五延迟桶、错误、重试与重映射累计
规则；remove 后重加、ci 成功均清空历史。mh 精确键集 op,id,from,to,now，
from/to/now 为 [0,10^9] 非 bool 整数，now 纳入共用非递减时钟，须
from≤to≤now//60 且 to-from<60；键集、类型、范围、关系或时钟非法报
INPUT/2，未知 id 报 BACKEND/3，from 早于 max(0,now//60-59) 报 STATE/4。
结果键序 op,id,windows；windows 含 from 至 to 所有窗并升序，项键序
window,requests,qps,errors,error_rate,latency,retries,remaps；整数与五
整数 latency 同 mg，qps、error_rate 仍下截两位小数字符串，空窗计数及
桶为 0、两字符串为 0.00。mh 不改度量，失败原子回滚；record/replay 逐
字节覆盖 mh。mh 时间 O(R)、额外空间 O(60B)。

后端延迟分位查询：lp 精确键序 op,id,from,to,p,now（键须按此序出现），
from/to/now 为 0..10^9、p 为 1..100 的非 bool 整数，now 纳入共用非递减
时钟，须 from≤to≤now//60 且 to-from<60。汇总 mh 区间内五个 latency
桶，各桶求和封顶 10^18，samples 为五桶和再封顶 10^18。令
rank=ceil(p*samples/100)，按桶 0..4 累计并截至 samples，取首个累计≥rank
者；桶上界依次为 1、10、100、1000、null。samples=0 时 rank=0 且
bucket=upper=null。结果键序 op,id,from,to,p,samples,buckets,rank,bucket,
upper；buckets 为五整数数组，bucket 为 0..4 整数或 null，upper 为整数或
null。非法键序、类型、范围、关系或时钟倒退报 INPUT/2；未知 id 报
BACKEND/3；from 早于 max(0,now//60-59) 报 STATE/4。lp 只读，失败批回滚。
紧凑 UTF-8 固定键序 JSON、单换行及 record/replay 逐字节契约照常；时间
O(R)、额外空间 O(1)，仅标准库；其余操作不变。

后端延迟分位告警：pa 精确键序 op,id,w,p,hi,lo,n,now（键须按此序出现），
id 沿用后端标识（未知 id 报 BACKEND/3），w、now 为 0..10^9，p 为 1..100，
hi、lo 为 0..4，n 为 1..60，皆为非 bool 整数且 lo<hi；now 纳入共用非
递减时钟，须 max(0,now//60-59)≤w<now//60（w 窗已结束且在最近 60 窗
保留范围内）。取 w 窗五个延迟桶按 lp 规则计算 samples、bucket、upper
（单窗 lp 口径）；无样本三者为 0、null、null 且连续数清零。首评固化
p、hi、lo、n 并从 N 态开始；N 态连续 n 窗 bucket≥hi 转 A，A 态连续 n
窗 bucket≤lo 转 N，方向不符清 run，转换后 run=0。后续 w 仅可同前或
+1；同窗同参返回缓存结果且不推进状态机。结果键序
op,id,w,p,state,samples,bucket,upper,run,changed；state 仅 N/A，
changed 仅转换为 true。pa 在 N/A 间转换（N→A 或 A→N）时追加一个历史
事件，同窗重报（命中缓存不推进状态机）与未转换不追加；每次推进状态机
的成功评估后删除 window<w-59 的事件。事件键序
window,from,to,p,samples,bucket,upper,hi,lo,n：from/to 仅 N/A，
bucket/upper 沿用该次 pa 的评估值（upper 可为 null），其余为整数，均取
触发评估当次的值。键序、id、类型、范围、关系或时钟倒退报
INPUT/2；未知后端报 BACKEND/3；窗未结束或过旧、跳窗（含回退）或变参
报 STATE/4。remove 后重加及 ci/cb 成功清告警，失败批回滚。pa 记账时间
O(1)、额外空间 O(B)，仅标准库；紧凑 UTF-8 JSON、单换行及 record/replay
逐字节契约照常，其他子命令不变。

后端延迟分位告警转换历史：ph 精确键序 op,id,from,to,now（键须按此序出
现），id 沿用后端标识，from/to/now 为 0..10^9 非 bool 整数，now 纳入共
用非递减时钟，须 from≤to≤now//60 且 to-from<60。返回键序 op,id,events；
events 仅含该后端区间 [from,to]（闭区间）内的转换事件并按 window 升序，
无事件为 []，项键序 window,from,to,p,samples,bucket,upper,hi,lo,n。ph
只读：不推进 pa 状态机、不清理历史。键序、id、数值、关系或时钟非法报
INPUT/2；未知 id 报 BACKEND/3；from 早于 max(0,now//60-59) 报 STATE/4。
remove 后同 id 重加及 ci/cb 成功清空历史；失败批回滚。ph 时间 O(60)、
额外空间 O(60B)，仅标准库；紧凑 UTF-8 固定键序 JSON、单末尾换行及
record/replay 逐字节契约照常；其他操作不变。

每后端重试/重映射告警：xa 精确键序 op,id,k,w,hi,lo,n,now（键须按此序出
现），id 沿用后端标识（未知 id 报 BACKEND/3），k 仅 R/M（分别取 w 窗 mh
口径的 retries/remaps 累计值）；w、now 为 0..10^9，hi 为 1..10^18，lo 为
0..(10^18-1)，n 为 1..60，皆为非 bool 整数且 lo<hi。now 纳入共用非递减
时钟，须 max(0,now//60-59)≤w<now//60（w 窗已结束且在最近 60 窗保留范围
内）。各 (id,k) 均从 N 态开始并独立维护，首评固化 hi/lo/n；取 w 窗 value
（缺窗为 0），N 态连续 n 窗 value≥hi 转 A，A 态连续 n 窗 value≤lo 转 N，
方向不符清 run，转换后 run=0。后续 w 仅可同前或 +1；同窗同参返回缓存结
果且不推进状态机。结果键序 op,id,k,w,state,value,run,changed；state 仅
N/A，value、run 为非负整数，changed 仅转换为 true。键序、id、k、类型、
范围、关系或时钟非法报 INPUT/2；窗口未结束/过旧、跳窗（含回退）或变参
报 STATE/4，依次判定。remove 后重加及 ci/cb/ca 成功清告警，失败批回滚。
xa 追加历史均摊 O(1)、空间 O(B)，仅标准库；紧凑 UTF-8 固定键序 JSON、
单末尾换行及 record/replay 逐字节契约照常；mr/mh/rr 及其余操作行为不变。

每后端重试/重映射告警转换历史：xh 精确键序 op,id,k,from,to,now（键须按
此序出现），id 沿用后端标识，k 仅 R/M，from/to/now 为 0..10^9 非 bool
整数，now 纳入共用非递减时钟，须 from≤to≤now//60 且 to-from<60。返回键
序 op,id,k,events；events 仅含该 (id,k) 区间 [from,to]（闭区间）内的转
换事件并按 window 升序，无事件为 []，项键序
window,from,to,value,hi,lo,n：from/to 仅 N/A，value 取触发转换当次 xa
的评估值，hi/lo/n 为固化阈值，五个数值字段皆为整数。xa 在 N/A 间转换
（N→A 或 A→N）时追加一个事件，同窗重报（命中缓存不推进状态机）与未转
换不追加；每次推进状态机的成功评估后删除 window<w-59 的事件。xh 只读：
不推进 xa 状态机、不清理历史。键序、id、k、数值、关系或时钟非法报
INPUT/2；未知 id 报 BACKEND/3；from 早于 max(0,now//60-59) 报 STATE/4。
remove 后同 id 重加及 ci/cb/ca 成功清空历史；失败批回滚。xh 时间 O(60)、
额外空间 O(60B)，仅标准库；紧凑 UTF-8 固定键序 JSON、单末尾换行及
record/replay 逐字节契约照常，其他子命令不变。

每后端重试/重映射告警汇总：xg 精确键序 op,id,k,from,to,now（键须按此序
出现），id 沿用后端标识，k 仅 R/M，from/to/now 为 0..10^9 非 bool 整数，
now 进入共用非递减时钟，须 from≤to≤now//60 且 to-from<60。返回键序
op,id,k,state,last,total,raised,cleared：state 取该 (id,k) 当前 xa 状态
（N/A），从未评估为 N；last 取现存 60 窗历史中最近转换窗（不限查询区
间），无事件为 null；raised/cleared 分别统计闭区间 [from,to] 内 N→A、
A→N 事件数，total 为两者之和，三者皆为非负整数，无历史时为 0。xg 只
读：不推进 xa 状态机、不裁剪历史。键序、id、k、类型、范围、关系或时钟
倒退报 INPUT/2；未知 id 报 BACKEND/3；from 早于
max(0,now//60-59) 报 STATE/4，依次判定。remove 后同 id 重加及 ci/cb/ca
成功清空告警与历史；失败批回滚。xg 时间 O(60)、额外空间 O(1)，仅标准
库；紧凑 UTF-8 固定键序 JSON、单末尾换行及 record/replay 逐字节契约照
常，xa、xh 不变。

池级重试/重映射告警概览：xp 精确键序 op,from,to,now（键须按此序出现），
from/to/now 为 0..10^9 非 bool 整数，now 进入共用非递减时钟，须
from≤to≤now//60 且 to-from<60。返回键序 op,backends,total：backends 仅
列现存后端并按 id 的 UTF-8 字节升序，项键序 id,R,M，R/M 各按
state,raised,cleared——state 为该 (id,k) 当前 xa 状态（N/A），未评估取
N，raised/cleared 为闭区间 [from,to] 内现存历史的 N→A、A→N 次数；total
键序 R,M，各按 alerting,raised,cleared——alerting 为当前 A 态后端数，
raised/cleared 为全部后端明细之和，三值封顶 10^18；空池返回空数组与全零
total。xp 只读：不推进 xa 状态机、不裁剪历史。键序、类型、范围、关系或
时钟倒退报 INPUT/2；from 早于 max(0,now//60-59) 报 STATE/4，依次判定。
remove 后同 id 重加及 ci/cb/ca 成功清空告警与历史；失败批回滚。xp 时间
O(B log B+60B)、额外空间 O(B)，仅标准库；紧凑 UTF-8 固定键序 JSON、单
末尾换行及 record/replay 逐字节契约照常，xa、xh、xg 不变。

后端采样历史：ms 键集 op,id,now（now 为 [0,10^9] 非 bool 整数，纳入共用
非递减时钟），id 须现存否则 BACKEND。每次采样记录该后端当时的活动连接数
与 removed——removed 沿用 mg 的取值与优先级（drain、health、circuit、
fault 或 null）。按 window=now//60 分窗保留最近 60 窗，每窗至多 60 个
不同 now（每后端至多 3600 样本）。ms 成功时按后端与 window=now//60 同步
维护该窗 samples（窗内采样数）与 peak（窗内并发峰值）聚合：同 (id, now)
同值重报幂等、不重复计数，值变化属冲突重报报 STATE/4（在改聚合前判定，
失败批次天然回滚）；聚合供 na 单次 O(1) 取窗值。ms 返回键序
op,ok，ok=true。mx 精确键集 op,id,from,to,now，数值与关系约束同 mh（非
法键集、类型、范围、关系或时钟倒退报 INPUT/2，未知 id 报 BACKEND/3，
from 早于 max(0,now//60-59) 报 STATE/4）。结果键序 op,id,windows，
windows 含 from 至 to 所有窗并按窗升序，项键序
window,samples,peak,last,removed：samples/peak 取上述窗聚合、last 为末
次采样的并发；空窗 samples/peak 为 0 且 last=null；removed 为键序
drain,health,circuit,fault,none 的非负整数计数对象，removed=null 的样本
计入 none。mx 不改变采样，失败原子回滚；record/replay 逐字节覆盖。remove
后重加、ci 成功均清空采样历史。ms 为 O(1)，mx 为 O(R+S)（R 为窗数、S
为区间内样本数），仅用标准库，其余子命令与既有操作行为不变。

不可用原因分钟历史：按 window=now//60 记账，各后端只保留最近 60 窗，
空窗不预建。probe 使 healthy 转 unhealthy 时该后端 health 加 1；dr 使 A
转 D/X 时 drain 加 1；cr 使 C/H 转 O 时 circuit 加 1；oa 返回 Q 且所选
后端 conns≥os.cap 时该后端 overload 加 1（同次过载与其它原因重叠只记
overload 一次）。重报（probe/cr 幂等重报、D/X 再 dr）与无状态转换一律
不记；oa 报 OVERLOAD/7（P 态背压或队满尾拒绝）回滚整批且不记。各计数
封顶 10^18；每次计数增加时同步为该后端、分钟窗和原因维护
count,first,last——count 沿用 10^18 封顶，first 为首次事件的 now（不随
封顶改变），last 为最近事件的 now，封顶后 last 仍更新（rt 查询此三值）。
rh 精确键序 op,id,from,to,now；from/to/now 为 0..10^9 非
bool 整数，now 进入共用非递减时钟，须 from≤to≤now//60 且 to-from<60。
返回键序 op,id,windows；windows 含 from 至 to 所有窗并按窗升序，项键序
window,health,drain,circuit,overload，值为非负整数，空窗全 0。键集、
类型、范围、关系或时钟倒退报 INPUT/2，未知 id 报 BACKEND/3，from 早于
max(0,now//60-59) 报 STATE/4。rh 只读，失败批次天然回滚；remove 后重加
与 ci/cb 成功清空历史。记账 O(1)，rh 时间 O(R)（R 为窗数）、空间 O(60B)，
record/replay 逐字节覆盖；其他子命令行为不变。

原因事件时刻查询：rt 精确键序 op,id,from,to,now（键须按此序出现），id
沿用后端标识，from/to/now 为 0..10^9 非 bool 整数，now 进入共用非递减
时钟，须 from≤to≤now//60 且 to-from<60。结果键序 op,id,windows；
windows 覆盖闭区间 [from,to] 并升序，项键序
window,health,drain,circuit,overload，四项均为键序 count,first,last 的
对象：count 为该窗该原因事件数（封顶 10^18），first/last 为首次/最近
事件的 now（封顶后 last 仍更新），无事件取 0,null,null。非法键序、id、
数值关系或时钟倒退报 INPUT/2；未知 id 报 BACKEND/3；from 早于
max(0,now//60-59) 报 STATE/4。保留最近 60 窗；remove 后同 id 重加及
ci/cb 成功清空，rt 只读、不推进历史，失败批次天然回滚。rt 时间 O(R)
（R 为窗数）、空间 O(60B)，仅用标准库；沿用紧凑 UTF-8 固定键序 JSON、
单末尾换行及 record/replay 逐字节契约，其他操作不变。

重试/重映射时刻历史：mr 及 fx/fr 向后端写入正数 retries/remaps 时，按
window=now//60 记账：count 增加该数并封顶 10^18，first 为本窗首次写入
的 now，last 为最近一次；封顶后仍更新 last，零值不记，同窗 count 须
等于 mh 同窗对应计数。rr 精确键序 op,id,from,to,now；id 为非空可编码
UTF-8 串，三数为 0..10^9 非 bool 整数，now 进入非递减时钟，须
from≤to≤now//60 且 to-from<60。结果键序 op,id,windows；windows 含闭
区间并升序，项键序 window,retries,remaps，二项为键序 count,first,last
的对象；无记录取 0,null,null。键序、id、数值、关系非法或时钟倒退报
INPUT/2；未知 id 报 BACKEND/3；from 早于 max(0,now//60-59) 报
STATE/4。只保留最近 60 窗；remove 后重加及 ci/cb 成功清空；rr 推进时
钟，失败批回滚。记账 O(1)，rr 时间 O(R)（R 为窗数）、空间 O(60B)，仅
用标准库；沿用紧凑 UTF-8 固定键序 JSON、单换行及 record/replay 逐字节
契约，其他操作不变且不属本题范围。

全池不可用原因汇总：ra 精确键序 op,from,to,now（键须按此序出现），
from/to/now 为 0..10^9 非 bool 整数，now 纳入共用非递减时钟，须
from≤to≤now//60 且 to-from<60；ra 只读汇总全池 rh 分钟窗，返回键序
op,windows；windows 覆盖 from 至 to 所有窗并升序，项键序
window,backends,total；backends 按加入序列出全部现存后端，项键序
id,health,drain,circuit,overload（rh 同款四键计数，缺窗全 0）；total
键序 health,drain,circuit,overload，为全池逐项求和并封顶 10^18；空池
backends 为空数组、total 全 0。键序、类型、范围、关系或时钟倒退报
INPUT/2；from 早于 max(0,now//60-59) 报 STATE/4。ra 只读；已删除后端
不列入也不汇总，同 id 重加只含新实例历史，ci 成功清空历史；失败批次
原子回滚；ra 时空 O(BR)（B 为现存后端数、R 为窗数），仅用标准库；
record/replay 逐字节覆盖 ra。

全池请求指标历史：ma 精确键序 op,from,to,now（键须按此序出现），
from/to/now 为 0..10^9 非 bool 整数，now 纳入共用非递减时钟，须
from≤to≤now//60 且 to-from<60。ma 只读汇总全池现存后端同窗 mh
数据，返回键序 op,windows；windows 覆盖 from 至 to 所有窗并升序，
项键序 window,requests,qps,errors,error_rate,latency,retries,
remaps：四项计数与五个 latency 桶逐项为全池求和并封顶 10^18，
qps=requests/60、error_rate=100*errors/requests（零请求为 0）均按
mh 口径下截为两位小数字符串；缺窗或空池计数与桶为 0、两字符串为
0.00。键序、类型、范围、关系或时钟倒退报 INPUT/2；from 早于
max(0,now//60-59) 报 STATE/4。已删除后端不汇总，同 id 重加只计新
实例，ci 成功清空历史；失败批次天然回滚；ma 时空 O(BR)（B 为现存
后端数、R 为窗数），仅用标准库，record/replay 逐字节覆盖 ma；
mr/mg/mh 行为不变。

全池增量快照：mo 精确键序 op,seq,now（键须按此序出现），seq 为
1..10^18、now 为 0..10^9 的非 bool 整数，now 纳入共用非递减时钟。
首次 seq=1，此后须逐次递增 1；同 seq、now 重报原样返回缓存结果且不
推进时钟与游标，同 seq 异 now、倒序或跳号报 STATE/4。按
window=now//60 返回上次 mo 之后的增量：首次或与上次跨窗时各后端基线
为零，成功后按现存后端把当前窗 mr 累计存为新基线。结果键序
op,seq,window,backends；backends 按现存加入序排列，空池为 []；项键
序
id,requests,qps,concurrency,errors,error_rate,latency,retries,remaps,
removed：requests/errors/retries/remaps 四计数与五整数 latency 桶为
当前窗 mr 累计减基线，concurrency、removed 取同 now 的 mg 现值
（removed 沿用 drain/health/circuit/fault 优先级，无则 null），
qps=requests/60、error_rate=100*errors/requests（零请求为 0）均下截
为两位小数字符串。键序、类型、范围或时钟倒退报 INPUT/2。remove 后同
id 重加按新实例从零计（基线清空）；ci/cb 清游标与缓存、seq 重置为
1；失败批次回滚游标、缓存和时钟。mo 时空 O(B)，沿用既有 JSON 与
record/replay 逐字节契约，其余子命令与既有操作行为不变。

配置导出与热加载：ce 键集仅 op，返回键序 op,config；config 精确键序
{version,backends,vnodes,limits,overload,sticky,idle,backpressure,
scheduler,faults,quotas,queue,capacities,lifetime}：
version=11；backends 按加入序，项 {id,weight,d,fail,success,circuit,drain,
endpoint}，circuit=null 或 {n,m,r,w,q}，drain=null 或登记的 t，endpoint
为 null 或键序 {host,port} 的登记端点，均只含登记值不含运行态；
vnodes=null 或整数；limits 项 {scope,id,r,b}，按 scope 的 B/C/S 序、id 的
UTF-8 字节升序；overload=null 或 {cap,q,ttl}；sticky/idle 为 null 或
{"ttl":整数}（ttl ∈ [1,10^9] 非 bool 整数，登记 ss/ts 才非 null）；
backpressure=null 或键序 {low,high}（low/high ∈ [0,10^6] 非 bool 整数，
登记 bp 才非 null，此时 overload 必非 null 且 low<high≤overload.q）；
scheduler 精确为 {"pick":"W"}、{"pick":"R"}、{"pick":"L"} 或
{"pick":"H"}（只含登记值不含运行态），W 为既有平滑加权，R 为轮询，
L 为最少连接，H 为一致性哈希；faults 为数组，项键序 id,k,a,z,v（id 引用
现存后端，k 仅 D/F/S，a/z/v 为 0..10^9 非 bool 整数，a<z，D 须 v=0、
F/S 须 v>0），同 id 各段 [a,z) 不重叠（相邻端点可接），按后端加入序、
段 a 升序输出，空计划为 []，不含 effect 或运行态；quotas 为数组，项
键序 scope,id,limit,span（scope 仅 B/C/S，id 为非空 UTF-8 串，limit
∈ [1,10^18]、span ∈ [1,10^9] 非 bool 整数），按 scope 的 B/C/S 序、
id 的 UTF-8 字节升序输出，只含登记值，不含 window、used；queue 为
精确键序 {dequeue,full}：dequeue 为 "F"/"S"（ot 遇阻即停/跳过阻塞），
full 为 "T"/"H"（队满尾拒绝/头淘汰），只含登记策略，不含等待项、
evicted 或 last；capacities 为数组（其后为 lifetime），项精确键序
{id,cap}（id 引用本配置后端，cap ∈ [1,10^6] 非 bool 整数），按后端加入序仅列显式
接纳容量覆盖，空为 []（[] 表示各后端使用 overload.cap）；lifetime
末置，为 null 或精确 {"ttl":整数}（ttl ∈ [1,10^9] 非 bool 整数），
为当前 tm 硬时限登记的纯登记值、不含运行态。ci 精确键集
op,config,now（原形式，键序不限），另接受精确键序 op,config,base,now
的乐观并发形式：base 为小写 64 位十六进制串（ct 输出的配置指纹），
格式非法判 INPUT/2；候选完成既有校验后比较 base 与操作前指纹，不等
判 STATE/4 且先于活动连接或排队检查，相等则沿用 ci 全部成功语义。
ct 精确键序仅 op，返回键序 op,digest：digest 为 ce.config 规范化
version=11 对象按逐层键序序列化为 UTF-8 紧凑 JSON（非 ASCII 不转义、
无末尾换行）后的 SHA-256 小写 64 位十六进制；ct 只读且不推进时钟，
时空 O(N)（N 为规范化配置大小）。cv 精确键序 op,config,now（键须按
此序出现）：config 校验与规范化同 ci，now ∈ [0,10^9] 非 bool 整数并
纳入共用非递减时钟；B 限流/配额、faults 与 capacities 引用未知后端判
BACKEND/3，
其余配置错误与 ci 同型同优先级；不应用配置，活动连接或排队项仅令
applicable=false，返回键序 op,applicable,connections,queued,config
（config 为规范化 version=11 回显）。cd 精确键序 op,config,now（键须
按此序出现）：config 校验、规范化与错误优先级同 cv，now 同上并进入
共用非递减时钟；比较当前 ce.config 与候选规范化配置的 backends，不
应用候选，活动连接或排队项不报错。结果键序
op,base,target,added,removed,changed,order：base/target 为 ct 摘要
字符串；added/removed 为 id 字符串数组，分别按候选/当前加入序；
changed 为对象数组，按候选序含共有且变化者，项键序 id,fields，
fields 为字符串数组，按 weight,d,fail,success,circuit,drain,endpoint
顺序列差异；order 为两侧完整 id 序列是否不同的 bool。cv/cd 成功仅
推进时钟，失败批回滚，时空 O(N)。
hd 哈希配置预演，精确键序 op,config,keys,now（键须按此序出现）：
config 校验与规范化同 cv/cd/pd，now ∈ [0,10^9] 非 bool 整数并进入
共用非递减时钟；keys 为 1..256 项非空、可直接 UTF-8 编码的字符串数组
（沿用 route 的 key 校验），可重复。不应用候选配置：以当前与候选配置
各自的 backends、vnodes 分别建环，忽略运行态（健康、熔断、排空、连接）
与粘性，全部后端按配置加入序上环，哈希规则沿用 chash
（SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，按摘要、加入序、i 排序；
key 哈希取首个不小于它的令牌、越界回绕），按 keys 原序各自独立映射。
结果键序 op,base,target,cases,summary：base/target 为当前/候选规范化
version=11 配置的 ct 摘要字符串；cases 按 keys 原序，项键序
key,before,after,changed，before/after 为当前/候选环选中的后端 id
字符串，changed 为 bool；summary 键序 total,stable,remapped，均为非负
整数，total 等于 keys 长度，stable 为前后相同的项数、remapped 为变化
项数。键序、keys（容器、项数、元素非空或编码）、now 非法或时钟倒退报
INPUT/2；候选 B 限流/B 配额/faults 引用未知后端报 BACKEND/3（优先级同
cv/cd/pd）；当前或候选任一 vnodes 为 null 或 backends 为空报 STATE/4。
hd 成功仅推进时钟且不改任何状态，失败批回滚。N 为两环令牌总数、K 为
keys 长度，时间 O(N log N + K log N)，空间 O(N+K)，仅用标准库；紧凑
UTF-8 固定键序 JSON、单换行及 record/replay 逐字节行为照常，其他子命令
不变。
hb 哈希分布预演，精确键序 op,config,keys,now（键须按此序出现）：config
结构、规范化、版本兼容及后端引用检查沿用 hd；keys 为 1..4096 项允许重复
的合法 route 键数组（非空、可直接 UTF-8 编码），按出现次数计数；now ∈
[0,10^9] 非 bool 整数并纳入共用非递减时钟。沿用 hd 的静态环与 SHA-256
规则，分别按两份规范化配置的后端加入顺序与 vnodes 建环，忽略健康、熔断、
排空、故障时间线及粘性运行态，每个 key 在两侧独立映射。不应用候选配置。
结果键序 op,base,target,backends,summary：base/target 为当前/候选规范化
version=11 配置的 ct 摘要字符串；backends 先按当前后端加入顺序排列，再按
候选顺序补充仅候选侧存在的后端，项键序 id,before,after,delta，某侧不存
在时该侧计数为 0，delta=after-before；summary 键序
total,stable,remapped,rate,before_min,before_max,after_min,after_max，
stable/remapped 按两侧落点是否相同计数（重复键按次数），rate 为
floor(10000*remapped/total)/100 的两位定点串（向下截断），每侧 min/max
只统计该侧实际存在的后端。字段集合、键序、类型、范围、UTF-8 编码、keys
数量或时钟倒退报 INPUT/2；候选 B 限流/B 配额/faults/capacities 引用未知
后端报 BACKEND/3；任一侧 vnodes 为 null 或 backends 为空报 STATE/4，错误
优先级同 hd。hb 成功仅推进 now 且不改其他运行态，失败批回滚。N 为两环
令牌总数、K 为 keys 长度、B 为两侧后端并集，时间 O(N log N + K log N)，
额外空间 O(N+B)，仅用标准库；紧凑 UTF-8 固定键序 JSON、单换行及
record/replay 逐字节行为照常，hd 与其他公开行为不变。ci 结果 op,ok=true；now 为非负非
bool 整数并纳入共用非递减
时钟（乐观并发形式的 now 仅收 0..10^9 非 bool 整数，否则 INPUT/2 且
整批回滚），亦接受 version=1 原结构（仅前五键）与 version=2 结构（追加三键），
两者 scheduler 缺省等价于 W；version=3 同为九键但 scheduler 仅收 W/R，
version=4 须含 scheduler 并收 W/R/L，version=5 收 W/R/L/H 且选 H 时
vnodes 须非 null；version=6 同 v5，且 backends 项须在既有七键后含
endpoint（v1..v5 不含该键，一律视为 null）；version=7 在既有九键末追加
faults 且须精确含该键（v1..v6 一律视 faults=[]），backends 项同 v6；
version=8 在既有十键末追加 quotas 且须精确含该键（v1..v7 一律视
quotas=[]），backends 项同 v6；version=9 在既有十一键末追加 queue 且须
精确含该键、精确为键序 dequeue,full 的两键对象（dequeue 仅 F/S、full
仅 T/H；v1..v8 一律视 dequeue=F、full=T），quotas 同 v8，backends 项同
v6；version=10 在既有十二键末追加 capacities 且须精确含末置该键，为
数组，项精确键序 id,cap（cap ∈ [1,10^6] 非 bool 整数，id 重复报
INPUT/2），输入顺序不限、按后端加入序仅列显式覆盖导出（v1..v9 一律视
capacities=[]，成功即清空覆盖），queue 与 backends 项同 v9；
version=11 在既有十三键末追加 lifetime 且须精确含末置该键，为 null 或
精确 {"ttl":整数} 单键对象（ttl ∈ [1,10^9] 非 bool 整数），v1..v10
一律规范化 lifetime=null。
各值沿用 add/hset/ws/chash/cs/ds/ls/os/ss/ts/bp 与 fs/fp 的类型与范围。
scheduler
缺失（v1/v2）合法，v3 多键、类型错误或 pick 非 W/R，v4 的 pick 非 W/R/L，
v5..v11 的 pick 非 W/R/L/H 或选 H 而 vnodes 为 null，
连同其余非法结构、键集、键序、版本、queue 结构/类型或枚举值、capacities
容器/项键序/类型/范围/编码或 id 重复、lifetime 结构/键集/ttl 类型或范围、
重复后端/限流项/故障段/
配额项、编码、
交叉约束（含同 id 段重叠）或时钟倒退判
INPUT/2，B 限流、B 配额、faults 或 capacities 引用未知后端判 BACKEND/3，
base 与操作前
指纹不等判 STATE/4，有活动连接或
排队项
判 STATE/4，依次判错。成功时原子替换配置并以 now 重建默认运行态（全部
healthy、d>0
自 now 起算预热、熔断 C 空窗、排空 A、桶满、配额按 quotas 重建
（window=now//span、used=0，v1..v7 为空即清空）、队空、粘性清空、度量
归零、
平滑 current 与轮询 ticket=0），并按 faults 载入各后端故障时间线
（fq/fx/fr/fi 立即按其生效；统计、分钟历史与恢复基线等运行态仍重置）：
sticky/idle 以登记值作用于新连接（idle
为新连接的空闲
时限，无连接故仅登记），backpressure 携带时置 N、未携带时取消；队列
dequeue/full 策略按 queue 原子载入（v1..v8 默认 F/T），清空队列并置
evicted=0、last=null；每后端接纳容量覆盖按 capacities 原子替换（v10 显式
覆盖按后端加入序恢复，旧版或 [] 清空；ci 后的 pc 只改当前 ce，不改已存
提交）；硬时限按 lifetime 原子载入（v11 携带 ttl 即按快照登记，null 与
v1..v10 清除回到未配，作用于此后新建连接；载入后的 tm 修改只改当前
ce/ct，不改已存提交与预约）；失败回滚
时钟、配置、运行态、rev、历史和预约均不变更。R 模式 pick 按既有健康、熔断闭合、排空 A 条件取得按加入序排列
的可选列表 E，E 空报 STATE/4，否则返回 E[ticket%len(E)] 并将 ticket 加
一；R 忽略权重且不改平滑 current，结果仍键序 op,id。add/remove 或健康、
熔断、排空状态迁移均不重置 ticket；W 及其余旧操作不变。L 模式 pick 仅
考虑 healthy、熔断 C、排空 A 的后端，取活动连接数 conns 最少者，并列取
最早加入者；无候选报 STATE/4，否则结果键序 op,id。L 忽略权重、无游标；
连接操作实时改变 conns，L 的 pick 不改 conns、current 或 ticket，后端
增删及资格迁移仅改变下次候选。H 模式为一致性哈希调度：pick 仅收
{op,key} 或 {op,key,now}（now ∈ [0,10^9] 非 bool 整数，纳入共用非递减
时钟，三键须已 ss），原 {op} 形状与 W/R/L 下带 key 的形状均报 STATE/4；
key 沿用 route 校验。H 共享既有环与粘性映射：首次按环选，原目标合格
（healthy、熔断 C、排空 A）则命中；目标删除或因健康、熔断、排空失格才
依环迁移且不迁回；改 vnodes 不主动迁移；三键沿用到期规则。二键结果键序
op,id,sticky,remapped，三键追加 expired,expires，值义同 route。未配环、
三键未 ss 或无合格后端报 STATE/4。H 不改连接数及 W/R/L 运行态。ce/ci
均 O(B+M+T+Q) 时空（M 为限流项数、T 为故障段数、Q 为配额项数）；R/L 的 pick 均为 O(B)
时间、O(1) 额外
空间，H 的 pick 为 O(BV log(BV)) 时间、O(BV+S) 空间。

配置提交与回滚：ci 成功后把规范化 version=11 配置存为提交，rev 从 1 起
递增，仅保留最近 16 条；失败不分配、不改历史，初始无提交。cl 精确键集
仅 op，返回键序 op,current,commits：current 为最新 rev 或 null，
commits 按 rev 升序，项键序 rev,config，config 复用 ce 的逐层键序与
值格式（含 faults、quotas、queue、capacities 与末置 lifetime）。cb 两种
形式：原三键精确键集 op,rev,now（键序不限）的输入、输出与语义不变，
rev 为 1..10^18
非 bool
整数且须仍被保留，now 沿用 ci 并进入共用非递减时钟；四键乐观形态
op,rev,base,now 为严格键序，rev 同前，base 为操作开始前当前规范化
version=11 配置的 ct 摘要（小写 64 位十六进制串），now 为 0..10^9 非
bool 整数并进入共用非递减时钟。四键先完成输入校验并确认目标修订仍在
十六条提交窗口内，再比较 base 与操作开始时当前配置摘要；不匹配报
STATE/4，不应用目标快照、不推进时钟（失败随整批回滚）、不清除预约、
不分配修订、不写审计。匹配后沿用三键 cb 全部判定与效果：按目标快照
执行 ci
的原子替换与默认运行态重建（恢复目标 faults 时间线并重置故障运行态，
恢复目标 quotas 并以 cb.now 重置各配额 window=now//span、used=0，
恢复目标 queue 的 dequeue/full 策略、清空队列并置 evicted=0、last=null，
恢复目标 capacities 的每后端接纳容量覆盖，按目标 lifetime 载入硬时限、
null 清除），
成功另建新 rev，返回键序 op,target,rev,ok（ok=true，四键的 op 同样恒
为 cb），即使目标配置与当前逐值相同也新建 rev，原历史保留后再按 16 条
淘汰。目标不存在或已淘汰、base 不匹配（仅四键）或 rev 耗尽（下一个 rev
将超过 10^18）报 STATE/4；键集合、键序、rev 类型/范围、base 格式、
now 类型/范围、UTF-8 编码或时钟倒退报 INPUT/2；有活动连接或排队项报
STATE/4。任何失败都不产生 stdout，并回滚同批此前变化与逻辑时钟、配置、
运行态、rev、历史与预约。
record/replay 逐字节覆盖成功与失败；cl 与 cb 两形态的额外时空上界
O(16N)（N 为规范化配置大小）；se 导出、si 恢复及 sd、sx、sm 对四键
cb 成功与失败的后续观察与直接继续一致；其余子命令与既有操作行为不变。

单字段配置热加载：cu 精确键序 op,base,section,value,now（键须按此序
出现）：base 格式同 ct.digest（小写 64 位十六进制串）；section 须为
version=11 的 ce.config 除 version 外任一顶层键
（backends,vnodes,limits,overload,sticky,idle,backpressure,scheduler,
faults,quotas,queue,capacities,lifetime）；value 沿用该字段既有结构、
键序、类型、范围与排序（limits/quotas 项键序与按 scope 的 B/C/S 序、
id 的 UTF-8 字节升序约束同 ci，v11 十四键键序约束同 ci），now 为
0..10^9 非 bool 整数并进入共用非递减时钟。以当前 ce.config 规范化 v11
导出为底稿仅替换 section，再按 ci 整体校验交叉约束（含 scheduler=H 须
vnodes 非 null、非 null backpressure 须 overload 非 null 且
low<high≤overload.q）；B 限流/配额、faults 或 capacities 引用未知候选
后端报 BACKEND/3；键序、base 格式、section、value 的结构/键序/类型/
范围/排序/交叉约束、now 或时钟倒退报 INPUT/2；校验后 base 与操作前指纹
不匹配、有活动连接或排队项、rev 耗尽报 STATE/4，依次判错（BACKEND 先于
base，base 先于活动连接与排队检查）。成功原子应用并沿用 ci 的运行态
重建、清除预约、分配新 rev（值未变也新建）并按 16 条历史淘汰；结果键序
op,base,target,rev,ok，base/target 为操作前后小写 SHA-256 摘要，rev 为
新 rev，ok=true。失败回滚时钟、配置、运行态、rev、历史与预约。时空
O(N)，N 为合成配置大小，仅用标准库；紧凑 UTF-8 固定键序 JSON、单换行
及 record/replay 逐字节契约不变，其他子命令不变。

H pick 记账：扩展 H 模式 pick，调度与映射行为不变，成功项仅记一次并归属
返回 id。无旧映射记 first；旧目标合格且本次未判到期记 sticky；三键旧映射
e 非 null 且 now≥e 记 expired（重选回原 id 也算 expired）；否则按旧目标
不存在（已删除）、unhealthy、熔断非 C、排空非 A 分别记 removed、health、
circuit、drain；重叠按 expired>removed>health>circuit>drain 判定。total
与对应项各 +1 并封顶 10^18；失败 pick 及非 H 操作不记。hm 精确键集
op,id，非法键集或 id→INPUT/2，未知 id→BACKEND/3，返回键序
op,id,total,first,sticky,expired,removed,health,circuit,drain，计数均为
非负整数。hm 只读，失败批次回滚；add、remove 后重加及 ci 成功均清零；
record/replay 逐字节覆盖；记账与 hm 均 O(1)，额外空间 O(B)。

时钟故障演练：fs 键集 op,id,k,a,z,v（a,z,v ∈ [0,10^9] 非 bool 整数，
a<z；k ∈ D/F/S，D 须 v=0，F/S 须 v>0）把该 id 的故障时间线替换为单段，
同参（同为该单段）重报幂等、异参覆盖，remove 清除、ci 按 faults 重建
（v1..v6 为空、version=7 载入目标时间线），返回 op,ok；未知
id 报 BACKEND。fx 键集 op,cid,flow,key,timeout,now（cid/flow/key 沿用
open/route 的校验，timeout/now ∈ [0,10^9] 非 bool 整数，now 纳入共用非
递减时钟），未 chash 报 STATE，重复 cid 报 CONNECTION。环同 route（仅
健康、熔断 C、排空 A 后端），自 key 哈希点遍历不同后端，不读写粘性映射；
按 now 在时间线中取唯一活动段（段按 a 升序且 [a,z) 不重叠，O(log T_b)），
段间隙与未登记按 N：活动段 D 不可用、F 于 ((now-a)//v)%2=0 时不可用、S
可用且耗时 v，否则耗时 0；跳过 D/F 时 remaps 加 1，环外不计。首个可用
后端耗时 ≤ timeout 则按 open 建连、state=A；超限不建连，state=R、
backend=该 id、latency=耗时；无可用项则 R、backend=null、latency=0。结
果键序 op,cid,state,backend,latency,remaps。fx 完成后按归属 id 追加一次
等价 mr 度量（见请求度量段），结果项本身不变。fs O(1)，fx 仍为 O(BV)。

故障时间线：fp 精确键序 op,items（键须按此序出现），items 为 0..4096 项
数组，项精确键序 id,k,a,z,v（键须按此序出现），字段约束同 fs；同一 id
可有多段，规范化按段起点 a 升序（提交顺序允许乱序），半开区间 [a,z) 互不
重叠，相邻段端点可接（z_i=a_{i+1}）。原子替换各列入 id 的时间线（未列入
的后端不变，空 items 为无操作）；非法键序、容器、项数、字段、同后端重叠
或编码报 INPUT/2，未知 id 报 BACKEND/3，依次判定，失败批次原子回滚。同
计划（与段序无关的同一规范化结果）重报幂等，返回 op,ok=true。

批量故障登记与快照：fb 键集 op,items，items 为数组，项键集 id,k,a,z,v；
id 须为现存且互异的后端，k 仅 D/F/S，a/z/v 为 [0,10^9] 非 bool 整数且
a<z，D 须 v=0、F/S 须 v>0（同 fs 各项校验）。items 按后端加入序规范化
后把全体时间线一次替换为各 id 的单段（未列入后端的时间线被清空），空数
组清空；与 fs/fp 同源，fs 仍可改单项，remove 仍清除，ci 按 faults 重建
（v1..v6 为空、version=7 以 faults 替换全体时间线，未列入即清空），fx/fr
与 mg 观察相同结果。计划重报幂等，返回键序 op,ok，ok=true。fq 键集 op,now，now
为非负非 bool 整数，纳入共用非递减时钟；返回键序 op,faults,down,slow：
faults 列出全部段，按后端加入序、同后端段起点 a 升序，项键序
id,k,a,z,v,effect，effect ∈ N/D/S——窗口 [a,z) 外为 N，窗口内 D 为 D、
F 按 ((now-a)//v)%2=0 取 D 否则 N、S 取 S；down/slow 分别计 effect 为
D/S 的后端数（同一后端同一 now 至多一个活动段，故即活动段计数）。非法
键、容器、重复 id、类型、范围或时钟倒退报 INPUT/2，未知 id 报 BACKEND/3，
失败批次原子回滚。fp 时间 O(T log T)、空间 O(T)，fb/fq 时间 O(B+T)、额
外空间 O(T)，单后端活动段查找 O(log T_b)（T 为总段数、T_b 为该后端段
数）。

全池运行态快照：br 精确键序 op,now（键须按此序出现），now 为 0..10^9
非 bool 整数，进入共用非递减时钟。返回键序 op,now,backends；backends
按加入序列出全部现存后端，项键序
id,health,circuit,drain,fault,connections,ready,blocked：health 为
healthy/unhealthy；未配熔断时 circuit=C，否则取 C/O/H；drain 取
A/D/X；fault 按 now 的时间线取 N/D/S（同 fq 的 effect 口径）；
connections 为活动连接数。healthy、circuit=C、drain=A 且 fault≠D 时
ready=true，否则 false；blocked 按 health,circuit,drain,fault 顺序
列出未满足条件的字符串，允许多项，ready 时为空数组，S 不阻断；空池
backends=[]。键序、now 类型或范围、时钟倒退报 INPUT/2；br 除推进
时钟外不改任何状态，成功仅推进时钟，失败批回滚。br 时间
O(B log(T+1))、空间 O(B)（T 为单后端故障段数），仅用标准库；
record/replay 逐字节覆盖。

故障重试：fr 键集 op,cid,flow,key,timeout,max,now，cid/flow/key 同
fx，timeout/now ∈ [0,10^9]、max ∈ [1,1024] 均非 bool 整数，now 纳入
共用非递减时钟。自 key 哈希点按 fx 顺序遍历不同后端至多 max 个（环同
route，仅健康、熔断 C、排空 A），每个候选即一次尝试：窗口内 D 或故障
相位 F 失败、耗时 0，S 且 v>timeout 失败、耗时 timeout，否则成功、
耗时 v 或 0 并终止；耗尽全部尝试仍无成功则拒绝。attempts=尝试数，
retries=remaps=max(attempts-1,0)，latency 为各次耗时之和。成功按 open
建连且 opened_at=now，拒绝不建连。结果键序
op,cid,state,backend,attempts,retries,latency,remaps，state ∈ A/R，
backend 成功为 id 否则 null。未配环或环内无候选报 STATE/4 且先于 cid
判定，重复活动 cid 报 CONNECTION/5，非法键、类型、范围、编码或时钟
倒退报 INPUT/2。每个尝试后端各追加一条 mr 度量（now=fr.now）：仅成功
尝试 ok=true，ms 为该次耗时，首项的 retries/remaps 记总值、余项为 0。
fr 时空 O(BV)；record/replay 照常覆盖 fr，其余契约不变。

组合故障预演：fi 精确键序 op,keys,timeout,max,now（键须按此序出现），
keys 为 1..256 项数组，元素沿用 route 的 key 校验（非空、UTF-8 可编码），
可重复；timeout、now ∈ [0,10^9]、max ∈ [1,1024] 均非 bool 整数，now 纳入
共用非递减时钟。按 keys 顺序在同一 now 各自独立模拟 fr 的哈希遍历与
D/F/S 规则（环同 route，仅健康、熔断 C、排空 A 后端），每键至多尝试 max
个不同后端；除共用时钟按 now 推进外不改任何运行态：不读写粘性映射、不建
连、不记 mr/fm/fh。结果键序 op,now,cases,summary；cases 按 keys 顺序，
项键序 key,state,backend,attempts,latency，值义同 fr：state 仅 A/R，
backend 成功为 id 否则 null，latency 为各次耗时之和。summary 键序
accepted,rejected,attempts,retries,remaps：accepted/rejected 为 state
A/R 的 case 数，attempts 为各 case attempts 求和，retries 与 remaps 均
为各 case max(attempts-1,0) 之和。非法键序、容器、项数、key、数值或时钟
倒退报 INPUT/2；未配置环或环内无合格候选报 STATE/4；fi 只读，失败批次
天然回滚。fi 时间 O(KBV)（K 为 keys 项数）、额外空间 O(K+BV)；紧凑
UTF-8 固定键序 JSON、单换行及 record/replay 逐字节行为照常，仅用标准库，
其他子命令行为不变且不属本题范围。

故障时间线预演：ft 精确键序 op,key,times,timeout,max,now（键须按此序
出现），key 沿用 route 校验（非空、UTF-8 可编码），timeout、now ∈
[0,10^9]、max ∈ [1,1024] 均非 bool 整数（同 fr），now 纳入共用非递减
时钟；times 为 1..60 项严格递增数组，项为 now..10^9 非 bool 整数。按
times 顺序对同一 key 在各时刻独立模拟 fr 的哈希遍历与 D/F/S 规则（环同
route，仅健康、熔断 C、排空 A 后端），每时刻至多尝试 max 个不同后端；
仅 now 推进共用时钟，不改连接、粘性、指标、告警或故障状态，失败批次
天然回滚。结果键序 op,key,now,cases；cases 按 times 顺序，项键序
at,state,backend,attempts,retries,remaps,latency：at 为该次模拟时刻，
state/backend/attempts/latency 值义同 fi（state 仅 A/R，backend 成功为
id 否则 null，latency 为各次耗时之和），retries 与 remaps 均为
max(attempts-1,0)。非法键序、字段类型/范围/编码、times 容器/项数/次序
或时钟倒退报 INPUT/2；未配置环或环内无合格候选报 STATE/4。ft 时间
O(PBV)（P 为 times 项数）、额外空间 O(P+BV)；紧凑 UTF-8 固定键序
JSON、单换行及 record/replay 逐字节行为照常，仅用标准库，其余操作不变。

只读接纳预演：oi 精确键序 op,key,c,s,bc,cc,sc,timeout,max,now（键须
按此序出现），key/c/s 与三项成本沿用 la 新键集校验（bc/cc/sc 不全为
0），timeout/now ∈ [0,10^9]、max ∈ [1,1024] 均非 bool 整数、now 纳入
共用非递减时钟。须已配置 chash 与 os：未配环或未 os 报 STATE/4（先于
时钟），环内无合格候选同样报 STATE/4。自 key 哈希点按 fr 顺序遍历环上
不同后端至多 max 个（环同 route，仅健康、熔断 C、排空 A），每个候选即
一次尝试，按 fault、slow、capacity、quota 优先判失败：窗口内 D 或故障
相位 F 失败、耗时 0，S 且 v>timeout 失败、耗时 timeout，conns≥os.cap
失败、耗时 0，以 now 只读补充令牌并推进固定窗后任一 B/C/S 桶或配额不
足失败、耗时 v（S）或 0；首个全部通过的后端即 A 并停止，耗尽全部尝试
为 R。桶补充与固定窗推进仅在只读投影上计算：每次尝试自当前真实桶/配额
独立投影，不补充、不扣减、不回写。除共用时钟按 now 推进外不改任何运
行态：不读写粘性映射、不建连、不记 mr/fm/fh，失败批次天然回滚。S 的
尝试延迟为 min(v,timeout)，其余为 0，latency 为各次耗时之和。结果键序
op,state,backend,attempts,latency,retries,remaps,fault,slow,capacity,
quota：state ∈ A/R，backend 仅成功为 id 否则 null，fault/slow/capacity/
quota 为对应失败原因的失败尝试数，retries=remaps=max(attempts-1,0)。
非法键（含键序）、类型、范围、编码或时钟倒退报 INPUT/2。oi 时间
O(BV)、额外空间 O(BV)；紧凑 UTF-8 固定键序 JSON、单换行及
record/replay 逐字节行为照常，仅用标准库，其他子命令行为不变。

只读接纳明细：od 精确键序 op,key,c,s,bc,cc,sc,timeout,max,now（键须
按此序出现），字段校验、成本约束、共用非递减时钟与 STATE 前置（未配
环、未 os、环内无合格候选）均同 oi。自 key 哈希点按 fr 顺序遍历环上
不同后端至多 max 个，按 fault、slow、capacity、quota 优先判定，首个
全部通过的后端即 A 并停止，耗尽全部尝试为 R；除共用时钟按 now 推进
外不改任何运行态（桶补充与固定窗推进仅在只读投影上计算，不补充、不
扣减、不回写），失败批次天然回滚。结果键序 op,state,backend,trace：
state ∈ A/R，backend 仅 A 为 id 否则 null；trace 按尝试序，项键序
id,effect,latency,result,blocked：effect 为该候选 now 时刻的 N/D/S，
latency 为 S 时 min(v,timeout)、否则 0，result 为 F（故障）/S（超
时）/C（满载）/Q（限额）/A（接纳），blocked 仅 Q 时按
BT,BQ,CT,CQ,ST,SQ 序列出不足项（T 为按 now 只读补充后的令牌桶，Q
为推进窗口后的固定配额），否则为空数组。非法键（含键序）、字段、编
码、成本或时钟倒退报 INPUT/2；未配置环、os 或无合格候选报 STATE/4。
od 时间 O(BV)、额外空间 O(BV)；紧凑 UTF-8 固定键序 JSON、单换行及
record/replay 逐字节行为照常，仅用标准库，其他子命令行为不变。

故障演练统计：fx/fr 访问后端时按 now 取唯一活动段，依 fq 的 effect 与
活动段种类 D/F/S 记账：effect 为 D 或 S 则当前段种类 affected 加 1；D
失败（fx 跳过、fr 尝试失败）或 S 耗时超 timeout 则 rejected 加 1。fx
跳过 D/F 后端时为其活动段种类 remaps 加 1；fr 失败后确有下一尝试时，为
失败后端该次活动段种类的 retries、remaps 各加 1。同请求同后端至多记一次，
各计数封顶 10^18。统计增量归当前活动段 k；某段被观察为受影响后，首次变
N（F 同段非故障相位、换段或落入段间隙）时 recovered 归上段自身种类 k，
同次新段照常记账；连续 N 不重复，再受影响方可再计。fs/fb/fp 异参替换或
移除只清按段恢复判定基线、不清计数与历史，同参不清；remove 后重加与 ci
成功清零统计。fm 精确键集 op,id，只读；非法键集或 id 报 INPUT/2，未知 id 报 BACKEND/3；结果键序 op,id,D,F,S，D/F/S 各为键序
affected,rejected,retries,remaps,recovered 的非负整数对象。fm 与记账均
O(1)，空间 O(B)；失败批回滚统计与恢复基线；record/replay 逐字节覆盖 fm。

故障统计分钟历史：fx/fr 每次对 fm 产生增量时同步归入 window=now//60
的分钟窗，沿用 fm 的归属、单请求去重与 10^18 封顶；recovered 归首次
观察到 N 的请求窗。每后端只保留最近 60 窗，空窗不预建；fs/fb/fp 重报、
替换或移除不清历史，remove 后重加与 ci 成功清空。fh 精确键集
op,id,from,to,now；from、to、now 为 [0,10^9] 非 bool 整数，now 纳入
共用非递减时钟，须 from≤to≤now//60 且 to-from<60；返回键序
op,id,windows；windows 覆盖 from 至 to 所有窗并升序，项键序
window,D,F,S，D/F/S 各为键序 affected,rejected,retries,remaps,
recovered 的非负整数对象，空窗五项为 0。键集、类型、范围、关系或时钟
倒退报 INPUT/2，未知 id 报 BACKEND/3，from 早于 max(0,now//60-59)
报 STATE/4。fh 只读，不改计数与恢复基线，失败批次天然回滚；记账 O(1)，
fh 为 O(R)（R 为窗数），额外空间 O(60B)，record/replay 逐字节覆盖。

全池故障汇总与阈值告警：fa 精确键序 op,from,to,now（键须按此序出现），
from/to/now 为 [0,10^9] 非 bool 整数，now 纳入共用非递减时钟，须
from≤to≤now//60 且 to-from<60；fa 只读汇总全池 fh 分钟窗，返回键序
op,windows；windows 覆盖 from 至 to 所有窗并升序，项键序
window,backends,total；backends 按加入序列出全部现存后端，项键序
id,D,F,S，三类各为 fh 同款五键对象，缺窗各项为 0；total 键序 D,F,S，
为全池逐项求和并封顶 10^18；空池 backends 为空、total 各项为 0。
fe 精确键序 op,w,hi,lo,n,now（键须按此序出现）：w/now 为 [0,10^9]
非 bool 整数，hi ∈ [1,10^18]、0≤lo<hi、n ∈ [1,60] 均非 bool 整数，
now 纳入共用非递减时钟；首评固化 (hi,lo,n) 并自 N 态起评，此后阈值
须相同且 w 仅同前（同窗原样返回首评结果，不推进状态机）或 +1；w 窗
须已结束（w<now//60）。v 为全池现存后端 w 窗三类 rejected 的逐项
封顶和；N 态连续 n 窗 v≥hi 转 A，A 态连续 n 窗 v≤lo 转 N，否则连续
计数归零；返回键序 op,w,state,v,run,changed，state ∈ N/A，run 为
连续计数，changed 仅转换时为 true 且转换后 run=0。非法键（含键
序）、类型、范围、关系或时钟倒退报 INPUT/2；from/w 早于
max(0,now//60-59) 的保留窗下界、fe 窗未结束、跳窗（含回退）或变阈
值报 STATE/4。fa 只读；remove 的后端不计入、重加无历史；ci 成功清
空历史与告警状态；失败批次原子回滚；fa 时空 O(BR)（R 为窗数），fe
时间 O(B)、额外空间 O(1)；record/replay 逐字节覆盖。

告警转换历史：fe 使状态在 N/A 间转换时追加事件（同窗重报不重复），
事件键序 window,from,to,v,hi,lo,n，记录触发窗、转换前后状态、该次
v 及固化阈值；window/v/hi/lo/n 为整数，from/to 仅 N 或 A。评估后删
除早于 w-59 的事件，未转换不记录。ah 精确键序 op,from,to,now（键须
按此序出现）；三数为 0..10^9 非 bool 整数，now 纳入共用非递减时钟，
须 from≤to≤now//60 且 to-from<60。返回键序 op,events；events 仅含
区间内事件，按 window 升序，项采用上述键序，无转换返回空数组。ah
只读，不推进告警或清理历史。键序、类型、范围、关系或时钟倒退报
INPUT/2；from 早于 max(0,now//60-59) 报 STATE/4。ci 成功清空告警现
态与历史，失败回滚；remove 及同 id 重加不清历史，仅影响后续 fe 的
v；批内失败回滚事件。fe 记账 O(1)，ah 时间 O(60)、空间 O(60)。

连接空闲超时：ts 键集 op,ttl（ttl ∈ [1,10^9] 非 bool 整数）配置全局
空闲时限，首配作用于既有与后续连接，同值幂等、异值报 STATE，登记值随
ce/ci 导出导入；返回 op,ok。凡成功建连（open/oa/ot/fx/fr）均置 last=opened_at。
tk 键集 op,cid,now：未到期（now < last+ttl）才置 last=now，返回 op,ok；
未知 cid 或命中已到期 cid 报 CONNECTION。tg 键集 op,cid,now，返回键序
op,cid,backend,state,opened,last,deadline：deadline=last+ttl，state 为
A（now<deadline）或 E，查询不删除。tx 键集 op,now：按建连顺序删除全部
到期连接并递减后端并发，排空 D 后端末连消失则转 X、end=now；返回
op,expired（cid 数组）。tk/tg/tx 的 now 纳入共用非递减时钟；未 ts 调用
tk/tg 报 STATE，tx 在 ts 或 tm 任一已配时可用。close 与 dg 强关同样清理
连接的空闲状态。ts/tk/tg 为 O(1)，tx 为 O(C)，额外空间 O(C)。

连接硬时限：tm 精确键序 op,ttl（键须按此序出现），ttl 为 1..10^9 非
bool 整数，配置全局硬时限；同值幂等，异值覆盖并立即作用于既有与后续
连接（硬截止恒为 opened_at+当前 ttl，覆盖无需改写每连接状态），返回
op,ok=true。硬截止 opened_at+ttl 不被 tk 延长：tk 命中硬到期同样报
CONNECTION/5 且不刷新 last；tg 只反映空闲时限、行为不变。te 精确键序
op,cid,now（键须按此序出现），now 为 0..10^9 非 bool 整数并进入共用
非递减时钟；返回键序 op,cid,idle,lifetime,deadline,state,reason：
idle 为 last+ts.ttl（未 ts 为 null），lifetime 为 opened_at+tm.ttl（未
tm 为 null），deadline 取非 null 二者最小值；now<deadline 时
state=A、reason=null，否则 state=E，空闲到期 reason=I、硬截止到期
reason=L，二者同时到期取 L；ts、tm 均未配报 STATE/4，未知 cid 报
CONNECTION/5。tx 在已配 ts 或 tm 时可用，依建连序删除全部
now≥deadline（空闲或硬截止到期）的连接，删除联动（端点快照、后端
并发与排空 D→X）不变，结果仍为 op,expired。tm/te 的非法键序、类型、
范围或时钟倒退报 INPUT/2；失败批次原子回滚。硬时限登记自 v11 起经末置
lifetime 随 ce/ci 导出导入：ci/cb/ca 成功按快照载入（null 清除），tm
后续修改只影响当前 ce/ct，不改已有提交与预约。tm/te 为 O(1)，
tx 为 O(C)；紧凑 JSON 键序、单末尾换行及 record/replay 逐字节契约不变，
仅用标准库。

确定性操作记录：record 把原始 stdin 字节作为全新 run 输入执行，无论底层
成功或按既有错误失败，均退出 0、stderr 为空，stdout 输出一行紧凑 JSON
记录，键序 version,stdin,exit,stdout,stderr：version=1（非 bool 整数），
exit 为实际码 0/2/3/4/5/6/7，三个字节字段为带标准填充的 RFC4648 Base64。
replay 只接受该精确键集且拒绝重复键；类型、版本、退出码、解码失败或非
规范 Base64 均报 INPUT/2（无 stdout）。合法记录在全新状态重执解码的
stdin 并逐字节比较退出码、stdout、stderr；不符时 stderr 写
{"error":"REPLAY"} 加换行、退出 8、无 stdout；一致时原样写出记录的
stdout、stderr 并采用记录退出码。两者额外时空 O(I+O)。

后端端点与连接转发快照：ep 键集 op,id,host,port 为后端登记 IP 端点；
host 须为无区域标识（不含 %）的 IP 字面量且满足
str(ipaddress.ip_address(host))==host，port 为 1..65535 非 bool 整数；
同参重报幂等、异参覆盖，返回 op,ok=true；未知 id 报 BACKEND/3，非法
键或字段报 INPUT/2。open、oa、ot、fx、fr 成功建连时快照该后端当时的
endpoint（未配置仍建连、无快照）。fw 键集 op,cid，返回键序
op,cid,backend,host,port，取建连时的快照、不受后续 ep 变更影响；无快
照报 STATE/4，未知 cid 报 CONNECTION/5；删除连接（close/dg/tx）同步
删除快照。ce 统一导出当前规范化 version=11（九键末为 faults，其后为
quotas、queue、capacities 与末置 lifetime），
backends 项保持在既有七键后追加 endpoint（null 或键序 host,port）；ci
兼容 version1..5 并视 endpoint=null，version6 须含 endpoint 但结构无
faults，version7 在九键后追加 faults 且项按 fp 同款校验，成功原子重建并
载入目标时间线、失败回滚。ep/fw
为 O(1)，额外空间 O(B+C)，仅用标准库；其余子命令与既有操作行为不变。

端点轮换存量查询与可控清理：eq 精确键序 op,id,now（键须按此序出现），
now 为 0..10^9 非 bool 整数并进入共用非递减时钟；目标后端须存在且已通过
ep 配置当前端点，未知后端报 BACKEND/3、未配置端点报 STATE/4。返回固定
键序 op,id,now,current,total,fresh,stale,items：current 固定键序
host,port 为当前端点；total 为该后端活动连接总数；fresh 为转发快照与当
前端点完全相同的连接数；stale 为其余连接数（含无快照连接）；items 只列
stale 连接并按全局建连顺序排列，每项固定键序 cid,host,port,opened,age，
无快照连接的 host、port 为 null，age=now-opened；无活动连接时计数为零且
items 为空。eq 除推进时钟外不改变任何运行态。ec 精确键序
op,id,before,now，before 与 now 均为 0..10^9 非 bool 整数且 before≤now；
只关闭目标后端中快照不同于当前端点且 opened≤before 的活动连接，按全局
建连顺序删除连接及快照、逐连接递减并发，返回固定键序 op,id,now,closed,
cids，cids 保持关闭顺序，无符合项为 closed=0 与空数组；不扣减令牌或配
额，也不触发排队接纳。清理使排空 D 状态后端失去最后连接时沿用 close 规
则转 X 且 end=now。字段、键序、编码、类型、范围、before 关系或时钟倒退
报 INPUT/2，未知后端报 BACKEND/3，未配置端点报 STATE/4，并按此前顺序判
定；失败无 stdout 且整批回滚。两项操作时间 O(C)、额外空间不超过 O(C)
（C 为活动连接数），紧凑 UTF-8 固定键序 JSON、单末尾换行及 run、record、
replay 与检查点逐字节契约不变，仅用标准库。

后端端点原子条件变更 ey：精确键序 op,id,base,target,now（键须按此序出
现）；base 与 target 均为 null 或精确键序 host,port 的对象，null 表示未
登记端点，对象沿用 ep 的规范化 IP 字面量与 1..65535 非 bool 端口校验；
now 为 0..10^9 非 bool 整数并进入共用非递减时钟。请求先完整校验字段、
键序、端点结构、编码和时钟，再确认后端存在；当前端点等于 target 时无论
base 是否仍匹配均视为成功的幂等重报，否则只有当前端点与 base 逐值相等
时才原子写入 target（target 为 null 即清除登记），其余比较失败报
STATE/4，未知后端报 BACKEND/3，输入或时钟错误报 INPUT/2，优先级依次为
INPUT、BACKEND、STATE。成功固定返回键序 op,id,endpoint,ok，endpoint 为
规范化后的 target 或 null，ok 恒 true；首次写入与相同请求重报逐字节一
致。ey 只改后端当前端点，不迁移、关闭或重新调度活动连接，也不消费令牌、
配额或等待队列：已有连接继续由 fw 返回其建连快照，后续连接仅在变更后仍
有当前端点时固化新快照；清除后该后端沿用未登记端点行为（eq 报 STATE/4、
er 不列出，已有快照仍可由 fw 查询），重新登记后旧快照自然成为 stale。任
何失败不产生 stdout 并回滚同批此前变化、端点与逻辑时钟。时间与额外空间
均 O(1)，仅用标准库；ep 的无条件覆盖语义与所有既有入口行为不变。

不可用时长查询：ru 精确键序 op,id,now（键须按此序出现），id 为非空
UTF-8 串，now 为 0..10^9 非 bool 整数并进入共用非递减时钟。结果键序
op,id,now,reasons：reasons 列出该后端在 now 时刻的阻断原因，按
drain,health,circuit,fault 排序，无原因时为 []，项键序
reason,since,duration，since 为该原因连续生效起点，duration=now-since。
health 在 probe 转 unhealthy 时起算、恢复 healthy 时清除；drain 在 dr
使 A 转 D/X 时起算（start=now），其后 D 转 X（末连 close、dg 到期强关、
tx）不重置，du 清除；circuit 从 C 首次转 O 起算，O 到期转 H 或 H 失败
重开 O 均不重置，连续 q 次成功回 C 或 cs 重配清除；fault 只读故障时间
线：now 落在 D 段时 since=a，落在 F 下线相位（((now-a)//v)%2=0）时
since=a+2*v*((now-a)//(2*v))，F 上线相位与 S 段不列入。同参重报
（probe/cr/dr）或无迁移（dr 时已在 D/X）不重置 since；remove 后同 id
重加、ci/cb 成功均清除全部 since。未知 id 报 BACKEND/3，键序、id、now
非法或时钟倒退报 INPUT/2。ru 除推进时钟外只读，失败批回滚；时间
O(log(T+1))、空间 O(1)（T 为故障段数），仅用标准库；沿用紧凑 UTF-8
固定键序 JSON、单换行及 record/replay 逐字节契约，其他子命令不变且不
属本题范围。

可移植运行态检查点：在 JSON 操作流中新增 se/si，使一次调用导出的状态
能在另一条全新调用中恢复并继续处理公开操作；不依赖文件、网络或进程时
间。对任意合法后续操作序列，直接继续与“导出后在空实例恢复再继续”所
得退出码、stdout、stderr 逐字节一致。se 精确键序仅 op，不推进显式时
钟、不改状态，返回固定键序 op,version,digest,state：version 初始为
1，state 为逐层固定键序的规范化 JSON 对象，有业务顺序的集合（后端加
入序、连接建连序、等待队列 FIFO、提交/审计 rev 序、告警转换事件窗序、
分钟窗时序）保持原序，其余集合（粘性键、桶/配额与池级告警标识、老化
服务类等）沿用 UTF-8 字节排序（复合键 scope 先按 B/C/S）；state 包含
全部影响后续公开行为的状态——逻辑时钟、纯登记配置与待生效计划、后端
顺序与健康/权重预热/熔断/排空/不可用起点/登记端点、令牌桶当前令牌与补
充时刻、固定窗口配额 window/used、活动连接与建连端点快照、粘性映射、
等待队列、各类分钟历史、故障计划/统计/恢复基线、请求与采样指标及 mo
游标缓存、全部告警状态机（fe/ea/pa/xa/na/le/ua/wa）及转换历史、提交
历史与 next_rev、审计事件与段级差异、配置预约。digest 为紧凑 UTF-8
编码（ensure_ascii=False、分隔符 ,/:、无末尾换行）的
{"version":1,"state":...} 的小写 SHA-256；相同状态逐字节导出相同结
果。si 精确接受 op,version,digest,state（键须按此序出现），校验通过
后原子替换当前状态，返回固定键序 op,digest,ok（ok=true）；连续导入同
一检查点幂等，恢复后 se 复现相同 version/digest/state。键集合、键序、
类型、范围、UTF-8、未知 version、摘要格式或摘要不符报 INPUT/2；摘要
相符但有悬空引用、重复标识、矛盾计数或非法状态组合报 STATE/4；state
紧凑编码超 8388608（8MiB）字节报 OVERLOAD/7（导出导入同限）；任何失
败均无 stdout 并回滚整批状态。摘要不符先于 state 语义校验；以规范化
往返（恢复后重新导出与所给 state 的紧凑编码逐字节一致）兜底确定性。
se/si 及校验为 O(N) 时间、O(N) 额外空间，N 为检查点编码字节数；其余
操作、错误优先级与 run/record/replay 逐字节契约不变，仅用标准库。

候选检查点只读差异 sd：精确接受依次排列的 op,version,digest,state 四
键，后三项沿用 si 的公开含义。sd 先按 si 的规则校验候选检查点（大小、
摘要匹配先于语义，再经完整解析与规范化往返），再按 se 的规则取得当前
状态的规范化内存快照；全程不推进逻辑时钟、不安装候选、不改变任何运行
态或幂等缓存。成功结果固定键序 op,before,after,equal,changes,
summary：before 为当前检查点摘要（同 se digest），after 原样返回输入
摘要；equal 仅在两份规范化 state 的紧凑 UTF-8 编码逐字节相同时为
true；changes 只列值不同的 state 顶层段，顺序沿用 se 的顶层键序，每
项固定 section,before,after，两指纹分别为该段规范化 JSON 值以
ensure_ascii=False、分隔符 ,:、无末尾换行编码后的小写 64 位 SHA-256，
是否列出以值比较为准；summary 固定 sections,changed,bytes_before,
bytes_after，依次为顶层段总数、差异段数及两份 state 紧凑编码的 UTF-8
字节数。状态相同时 changes 为空、changed 为零，重复查询逐字节一致。
字段集合、键序、类型、UTF-8、版本、摘要格式与匹配、状态语义及 8MiB
上限沿用 si 的校验规则、错误优先级与 INPUT/2、STATE/4、OVERLOAD/7
分类；候选校验成功后当前状态超 se 导出上限亦判 OVERLOAD/7。任何失败
均无 stdout 并回滚整批操作（含此前操作的临时变化）。时间与额外空间
O(N)，changes 另占 O(S)，N 为两份 state 编码字节数之和、S 为顶层段
数；仅用标准库，现有 se/si、配置、调度、连接、查询语义与 run/record/
replay 逐字节契约不变。

三方检查点只读预演 sx：以当前运行态（current）、基线检查点（base）与
候选检查点（target）区分并发变化，供调用方在执行 si 前判断候选是否仍
可安全使用；不合并/导入、不推进显式时钟、不改任何运行态或幂等缓存。
sx 精确接受按 op,base,target 排列的三个键；base/target 都是按
version,digest,state 排列且不含 op 的检查点对象（version 仅收 1、
digest 小写 64 位十六进制）。先按 si 规则校验 base 再 target（各自沿
用 8MiB、摘要匹配先于语义、完整结构/语义解析与规范化往返及 INPUT/2、
STATE/4、OVERLOAD/7 优先级），再按 se 规则取得当前规范化快照；错误严
格按 base、target、current 顺序判定，任一输入检查点或当前状态紧凑编码
超 8388608 字节报 OVERLOAD/7。成功结果固定键序
op,current,base,target,status,changes,summary：current/base/target 为
各自检查点（version=1）的小写 SHA-256 摘要，依次对应当前、基线与候
选。当前与候选规范化 state 紧凑编码逐字节相同时 status 为 SAME，否则
存在冲突段时为 CONFLICT，其余为 CLEAN。changes 按 state 顶层键序列出
三份状态不全相同的段（三份完全相同时为空；SAME 整体状态下基线独异段
仍以 kind=SAME 列出），每项固定 section,base,current,target,kind，三
个值为对应规范化段 JSON 的小写 SHA-256；当前等于基线而候选不同为
TARGET，候选等于基线而当前不同为 CURRENT，当前等于候选而基线不同为
SAME，其余为 CONFLICT。summary 固定
sections,changed,target,current,same,conflict，四类计数之和等于
changed。任何失败均无 stdout 并回滚整批；重复预演逐字节一致，run、
record、replay 继续遵守紧凑 UTF-8 固定键序 JSON、单末尾换行。时间与额
外空间 O(N)，N 为三份规范化状态紧凑编码总字节数；仅用标准库，现有
se、sd、si 行为不变。

检查点三方合并 sm：调用方提交基线检查点与目标检查点，把目标相对基线
的无冲突变化原子合入执行到该操作时的当前运行态。sm 精确接受按
op,base,target 排列的三个键，不接受 now、不推进显式时钟；base/target
沿用 sx 的 version,digest,state 检查点对象及规范化规则。校验顺序严格
按 base、target、current：每份检查点沿用 si 规则与错误优先级（8MiB、
摘要匹配先于语义、完整语义解析与规范化往返），当前紧凑编码超 se 的
8MiB 上限报 OVERLOAD/7。合并以 se 规范化 state 顶层段为最小单位并按
固定键序处理：某段 target==base 保留 current；target!=base 且
current==base 或 current==target 时采用 target；其余情况为冲突，存在
冲突即拒绝整次合并、不安装任何段（STATE/4）。无冲突时组合结果作为完
整检查点再次校验（大小、悬空引用、重复标识、矛盾计数、跨段非法组合
与规范化往返），通过后才原子安装。成功返回固定键序
op,before,after,changed,sections,ok：before/after 为合并前后按 se 规
则得到的摘要，changed 为实际改变的段数，sections 按顶层键序列出实际
改变的段；目标变化已存在或目标未改变时幂等成功（after==当前摘要、
changed=0、sections=[]）。键集合/键序/类型/版本/摘要格式或摘要不符
报 INPUT/2；输入状态语义非法、段冲突或组合状态非法报 STATE/4；任一
受检或合并后状态超限报 OVERLOAD/7；失败不产生 stdout 并回滚同批此前
变化。sm 时间与额外空间 O(N+S)，N 为三份状态及合并结果规范化编码总
字节数、S 为顶层段数；输出继续使用紧凑 UTF-8 固定键序 JSON 和单个末
尾换行，run、record、replay 覆盖成功、幂等与失败结果，现有 se、sd、
sx、si 及其他公开行为不变，仅用标准库。
"""

import base64
import bisect
import hashlib
import ipaddress
import json
import json.scanner
import re
import sys
from collections import deque, OrderedDict

EXIT_INPUT = 2
EXIT_BACKEND = 3
EXIT_STATE = 4
EXIT_CONNECTION = 5
EXIT_RATE = 6
EXIT_OVERLOAD = 7
EXIT_REPLAY = 8

DEFAULT_FAIL = 3
DEFAULT_SUCCESS = 2

# mr 各计数（requests/errors/retries/remaps/延迟桶）与 fm 各计数
# （affected/rejected/retries/remaps/recovered）均封顶 10^18。
METRIC_CAP = 10 ** 18


def new_fault_stats():
    """一组故障演练计数：D/F/S 三种登记种类各五键，键序固定
    为 affected,rejected,retries,remaps,recovered。fm 的累计总数与
    fault_hist 各窗快照共用同一结构。"""
    return {
        kind: {
            "affected": 0, "rejected": 0, "retries": 0,
            "remaps": 0, "recovered": 0,
        }
        for kind in "DFS"
    }


class _Failure(Exception):
    """fail() 抛出的内部异常：携带退出码与错误标签，由最外层统一落盘。"""

    def __init__(self, exit_code, label):
        super().__init__(label)
        self.exit_code = exit_code
        self.label = label


def fail(exit_code, label):
    raise _Failure(exit_code, label)


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


_JSON_STRINGCHUNK = re.compile(
    r'(.*?)(["\\\x00-\x1f])', re.VERBOSE | re.MULTILINE | re.DOTALL
).match
_JSON_HEXDIGITS = re.compile(r"[0-9A-Fa-f]{4}").match
_JSON_BACKSLASH = {
    '"': '"', "\\": "\\", "/": "/",
    "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t",
}


def scan_json_string(s, end, strict=True):
    """与 json 纯 Python scanstring 相同的字符串扫描，但不合并代理对：
    \\uXXXX 转义逐段解码，孤立代理与转义写出的代理对都保留为代理码点，
    交由标识符校验判 INPUT；真实非 BMP 字符（原始 UTF-8 字节）不受影响。
    end 为开引号之后的位置；返回 (解码串, 闭引号之后的位置)。"""
    chunks = []
    begin = end - 1
    while True:
        chunk = _JSON_STRINGCHUNK(s, end)
        if chunk is None:
            raise ValueError("Unterminated string starting at: %r" % begin)
        end = chunk.end()
        content, terminator = chunk.groups()
        if content:
            chunks.append(content)
        if terminator == '"':
            break
        if terminator != "\\":
            if strict:
                raise ValueError("Invalid control character: %r" % terminator)
            chunks.append(terminator)
            continue
        try:
            esc = s[end]
        except IndexError:
            raise ValueError("Unterminated string starting at: %r" % begin)
        if esc != "u":
            try:
                char = _JSON_BACKSLASH[esc]
            except KeyError:
                raise ValueError("Invalid \\escape: %r" % esc)
            end += 1
        else:
            match = _JSON_HEXDIGITS(s, end + 1)
            if match is None:
                raise ValueError("Invalid \\uXXXX escape")
            char = chr(int(match.group(), 16))
            end += 5
        chunks.append(char)
    return "".join(chunks), end


def decode_json(text):
    """以不合并代理对的 scanstring 解析 JSON 并拒绝重复键。

    字符串值经自定义 scanstring（纯 Python 扫描器）；对象键只可能是
    字段名，沿用默认扫描不影响判定。"""
    decoder = json.JSONDecoder(object_pairs_hook=reject_duplicate_keys)
    decoder.parse_string = scan_json_string
    decoder.scan_once = json.scanner.py_make_scanner(decoder)
    return decoder.decode(text)


def check_utf8_encodable(value):
    # 公开标识字符串须能直接编码为 UTF-8；JSON 可能解码出孤立高/低
    # 代理项（含转义写出的代理对），一律判 INPUT。真实非 BMP 字符合法。
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        fail(EXIT_INPUT, "INPUT")


def parse_cid(value):
    if not isinstance(value, str) or value == "":
        fail(EXIT_INPUT, "INPUT")
    check_utf8_encodable(value)
    return value


def parse_backend_id(value):
    if not isinstance(value, str) or value == "":
        fail(EXIT_INPUT, "INPUT")
    check_utf8_encodable(value)
    return value


def parse_now(value):
    # bool 是 int 的子类，必须显式排除。
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_warm_now(value):
    # add(d)/ws/wg 与 qs/qg 的 now ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_threshold(value):
    # bool 是 int 的子类，必须显式排除。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 100
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_rate(value):
    # 令牌桶速率 r ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_burst(value):
    # 令牌桶容量 b ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_duration(value):
    # 预热时长 d ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_circuit_window(value):
    # 熔断恢复窗口 w ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_vnodes(value):
    # bool 是 int 的子类，必须显式排除。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 1024
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_attempt_max(value):
    # fr 的 max（至多尝试的不同后端数）∈ [1,1024]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 1024
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_cost(value):
    # 令牌成本 bc/cc/sc ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_quota_limit(value):
    # 固定窗口配额上限 limit ∈ [1, 10^18]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 18
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_quota_span(value):
    # 固定窗口跨度 span ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_xa_hi(value):
    # xa 的 hi ∈ [1,10^18]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 18
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_xa_lo(value):
    # xa 的 lo ∈ [0,10^18)，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value < 10 ** 18
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_na_hi(value):
    # na 的 hi ∈ [1,10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_na_lo(value):
    # na 的 lo ∈ [0,10^9)，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value < 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_le_hi(value):
    # le 的 hi ∈ [1,10^18]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 18
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_le_lo(value):
    # le 的 lo ∈ [0,10^18)，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value < 10 ** 18
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_ua_threshold(value):
    # ua 的 threshold ∈ [1,10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_queue_param(value):
    # os 的 cap/q/ttl ∈ [1, 10^6]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 6
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_bp_num(value):
    # bp 的 low/high ∈ [0, 10^6]，非 bool 整数（low 允许 0）。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 6
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_metric_num(value):
    # mr 的 ms/retries/remaps/now ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_aging_step(value):
    # qa 的 step ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_aging_p(value):
    # qa 项的基础优先级 p ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_fault_num(value):
    # fs 的 a/z/v 与 fx 的 timeout/now ∈ [0, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_fault_segment(fields):
    """校验并规范化单个故障段除 id 外的字段（k,a,z,v）：k ∈ D/F/S，
    a,z,v ∈ [0,10^9] 非 bool 整数且 a<z，D 须 v=0、F/S 须 v>0；
    返回段元组 (k,a,z,v)。fs/fb/fp 三个入口共用同一套字段约束。"""
    k = fields["k"]
    if k not in ("D", "F", "S"):
        fail(EXIT_INPUT, "INPUT")
    a = parse_fault_num(fields["a"])
    z = parse_fault_num(fields["z"])
    v = parse_fault_num(fields["v"])
    if not a < z:
        fail(EXIT_INPUT, "INPUT")
    # D（不可用）须 v=0；F（抖动）/S（慢）须 v>0。
    if k == "D":
        if v != 0:
            fail(EXIT_INPUT, "INPUT")
    elif v == 0:
        fail(EXIT_INPUT, "INPUT")
    return k, a, z, v


def parse_fault_plan(raw_items):
    """fp/fd/fc 共用的全量计划校验：items 为 0..4096 项数组（bool 不是数组），
    项精确键序 id,k,a,z,v（键须按此序出现），字段约束沿用 fs；同一 id
    多段须按 a 升序且 [a,z) 互不重叠（相邻端点可接）。返回
    {id: [segment, ...]}，各 id 段已按 a 规范化排序。分组 O(T)，规范化
    排序用稳定基数排序 sort_fault_segments（O(T)，a∈[0,10^9]<2^32 四轮
    LSD），合计 O(T)。"""
    if (
        not isinstance(raw_items, list)
        or isinstance(raw_items, bool)
        or not 0 <= len(raw_items) <= 4096
    ):
        fail(EXIT_INPUT, "INPUT")
    # 按 id 分组并保序收集段；全部校验先于任何状态变更，失败批回滚。
    grouped = OrderedDict()
    for item in raw_items:
        if (
            not isinstance(item, dict)
            or list(item) != ["id", "k", "a", "z", "v"]
        ):
            fail(EXIT_INPUT, "INPUT")
        item_id = parse_backend_id(item["id"])
        grouped.setdefault(item_id, []).append(
            parse_fault_segment(item)
        )
    plan = {}
    for item_id, segments in grouped.items():
        # 规范化：同 id 段先按 a 升序（稳定基数排序 O(T_b)，合计 O(T)，
        # 与 parse_config_faults 同款），再校验半开区间互不重叠（z_i<=
        # a_{i+1}，相等为相邻可接）。乱序但可排成不重叠序列的计划合法；
        # 同 a 或任何相交在此被拒。
        segments = sort_fault_segments(segments)
        for idx in range(len(segments) - 1):
            if segments[idx][2] > segments[idx + 1][1]:
                fail(EXIT_INPUT, "INPUT")
        plan[item_id] = segments
    return plan


def parse_key(value):
    # key 为 UTF-8 可编码的非空字符串；JSON 可能解码出孤立代理项。
    if not isinstance(value, str) or value == "":
        fail(EXIT_INPUT, "INPUT")
    check_utf8_encodable(value)
    return value


def encode_backend_id(value):
    """chash/route 建环时遇到的 id 也必须 UTF-8 可编码。"""
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError:
        fail(EXIT_INPUT, "INPUT")


def circuit_closed(record):
    """未配置熔断器或熔断状态为 C 的后端才参与调度。"""
    circuit = record["circuit"]
    return circuit is None or circuit["state"] == "C"


def drain_available(record):
    """仅 A（可用）态后端参与 pick/open 与新的 route 映射。"""
    return record["drain"]["state"] == "A"


def parse_drain_timeout(value):
    # 排空时限 t ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_idle_ttl(value):
    # 连接空闲时限 ttl ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_sticky_ttl(value):
    # 限时粘性 ttl ∈ [1, 10^9]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def build_ring(backends, vnodes):
    """按 (摘要, 加入顺序, i) 升序返回 healthy、熔断闭合且 A 态后端的令牌环。"""
    tokens = []
    for join_index, (backend_id, record) in enumerate(backends.items()):
        if (
            not record["healthy"]
            or not circuit_closed(record)
            or not drain_available(record)
        ):
            continue
        encoded = encode_backend_id(backend_id)
        for i in range(vnodes):
            digest = hashlib.sha256(
                encoded + b"\x00" + str(i).encode("ascii")
            ).digest()
            tokens.append((int.from_bytes(digest, "big"), join_index, i, backend_id))
    tokens.sort(key=lambda token: (token[0], token[1], token[2]))
    return tokens


def build_static_ring(backend_ids, vnodes):
    """与 build_ring 同款 chash 规则，但忽略全部运行态：后端按给定加入序
    全部上环，vnodes 个令牌为 SHA-256(UTF8(id)+0x00+无前导零 ASCII(i))，
    摘要按 256 位大端无符号数排序（并列按加入顺序、i 升序）。供 hd 这类
    纯配置预演以配置 backends/vnodes 建环。时间 O(N log N)（N 为令牌总数），
    额外空间 O(N)。"""
    tokens = []
    for join_index, backend_id in enumerate(backend_ids):
        encoded = encode_backend_id(backend_id)
        for i in range(vnodes):
            digest = hashlib.sha256(
                encoded + b"\x00" + str(i).encode("ascii")
            ).digest()
            tokens.append((int.from_bytes(digest, "big"), join_index, i, backend_id))
    tokens.sort(key=lambda token: (token[0], token[1], token[2]))
    return tokens


def lookup_ring(tokens, digests, key):
    """chash 路由：哈希 UTF8(key) 取首个不小于它的令牌，越界回绕到环首。
    tokens/digests 非空（空环由调用方先报 STATE）。O(log N)，N 为令牌数。"""
    key_hash = int.from_bytes(
        hashlib.sha256(key.encode("utf-8")).digest(), "big"
    )
    index = bisect.bisect_left(digests, key_hash)
    if index == len(tokens):
        index = 0  # 越界回绕到环首
    return tokens[index][3]


def parse_flow(value):
    """校验 [源IP,源端口,目的IP,目的端口,协议]，返回规范化五元组。"""
    if not isinstance(value, list) or len(value) != 5:
        fail(EXIT_INPUT, "INPUT")
    src_ip, src_port, dst_ip, dst_port, protocol = value
    for ip in (src_ip, dst_ip):
        if not isinstance(ip, str) or ip == "":
            fail(EXIT_INPUT, "INPUT")
        check_utf8_encodable(ip)
    for port in (src_port, dst_port):
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            fail(EXIT_INPUT, "INPUT")
    if protocol not in ("tcp", "udp"):
        fail(EXIT_INPUT, "INPUT")
    return [src_ip, src_port, dst_ip, dst_port, protocol]


def parse_endpoint_host(value):
    # ep/ci 的 host：无区域标识（不含 %）的 IP 字面量，且规范化形式与
    # 原文逐字节相同（拒绝前导零、非常规缩写等写法）；非字符串、非
    # IP 字面量或含区域标识均判 INPUT。
    if not isinstance(value, str) or "%" in value:
        fail(EXIT_INPUT, "INPUT")
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        fail(EXIT_INPUT, "INPUT")
    if str(parsed) != value:
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_endpoint_port(value):
    # ep/ci 的 port ∈ [1, 65535]，非 bool 整数。
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 65535
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def parse_endpoint_or_none(value):
    # ey 的 base/target：null 表示未登记端点，否则为精确键序 host,port 的
    # 对象，校验沿用 ep（规范化 IP 字面量与 1..65535 非 bool 端口）。
    if value is None:
        return None
    if not isinstance(value, dict) or list(value) != ["host", "port"]:
        fail(EXIT_INPUT, "INPUT")
    return (
        parse_endpoint_host(value["host"]),
        parse_endpoint_port(value["port"]),
    )


def endpoint_json(endpoint):
    """内部端点元组 (host, port) 的规范化 JSON 形态；None 为 null。"""
    if endpoint is None:
        return None
    return {"host": endpoint[0], "port": endpoint[1]}


def parse_endpoint_switch_items(raw_items):
    """es/ei 预约 items 的形状校验（沿用 ej）：1..1000 项数组（bool 不是
    数组），每项精确键序 id,base,target，同一 id 不得重复；base/target 均
    为 null 或精确键序 host,port 的对象。返回规范化 (id, base, target)
    元组列表，保持请求顺序。仅形状/数量/重复 id/端点结构校验；后端是否
    现存留执行期判 BACKEND。"""
    if (
        not isinstance(raw_items, list)
        or isinstance(raw_items, bool)
        or not 1 <= len(raw_items) <= 1000
    ):
        fail(EXIT_INPUT, "INPUT")
    items = []
    seen_ids = set()
    for raw_item in raw_items:
        if (
            not isinstance(raw_item, dict)
            or list(raw_item) != ["id", "base", "target"]
        ):
            fail(EXIT_INPUT, "INPUT")
        item_id = parse_backend_id(raw_item["id"])
        if item_id in seen_ids:
            fail(EXIT_INPUT, "INPUT")
        seen_ids.add(item_id)
        base = parse_endpoint_or_none(raw_item["base"])
        target = parse_endpoint_or_none(raw_item["target"])
        items.append((item_id, base, target))
    return items


def endpoint_switch_digest(items, before):
    """全池端点切换预约身份摘要：固定键序对象
    {"items":items,"before":before} 的紧凑 UTF-8 JSON（非 ASCII 不转义、
    分隔符 ,:、无末尾换行）的 SHA-256 小写十六进制。items 为规范化
    (id, base, target) 元组列表，按保存顺序逐项输出固定键序
    id,base,target，端点 null 或 host,port 对象。"""
    canonical = {
        "items": [
            {"id": item_id, "base": endpoint_json(base),
             "target": endpoint_json(target)}
            for item_id, base, target in items
        ],
        "before": before,
    }
    encoded = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sort_fault_segments(segments):
    """按段起点 a 稳定升序：a ∈ [0,10^9] < 2^32，LSD 基数排序四轮 256
    桶共 O(T)，与比较排序的稳定结果一致（同 a 保持原相对序；同 a 段随后
    必因重叠判 INPUT，故输出与排序实现无关）。"""
    ordered = list(segments)
    for shift in (0, 8, 16, 24):
        digit_buckets = [[] for _ in range(256)]
        for segment in ordered:
            digit_buckets[(segment[1] >> shift) & 0xFF].append(segment)
        ordered = [segment for bucket in digit_buckets for segment in bucket]
    return ordered


def parse_config_faults(value):
    """校验 ci version=7 的 faults 数组并规范化：faults 为数组，项精确键序
    id,k,a,z,v（键须按此序出现），字段约束同 fs/fp；同一 id 可多段，规范
    化按段起点 a 升序，半开区间 [a,z) 互不重叠（相邻端点可接）。返回
    {id: [(k,a,z,v),...]}，未列入的后端时间线为空；空计划为 {}。结构、
    键序、类型、范围、编码、重复或重叠判 INPUT；id 是否现存留执行期判
    BACKEND（同 B 限流）。"""
    if not isinstance(value, list):
        fail(EXIT_INPUT, "INPUT")
    grouped = OrderedDict()
    for item in value:
        if not isinstance(item, dict) or list(item) != ["id", "k", "a", "z", "v"]:
            fail(EXIT_INPUT, "INPUT")
        item_id = parse_backend_id(item["id"])
        grouped.setdefault(item_id, []).append(parse_fault_segment(item))
    plan = {}
    for item_id, segments in grouped.items():
        # 规范化同 fp：同 id 段按 a 升序，再校验半开区间互不重叠（z_i<=
        # a_{i+1}，相等为相邻可接）；同 a 或完全重复的段在此被拒。排序用
        # 稳定基数排序，分组与排序合计 O(T)，v7 规范化整体 O(B+M+T)。
        segments = sort_fault_segments(segments)
        for idx in range(len(segments) - 1):
            if segments[idx][2] > segments[idx + 1][1]:
                fail(EXIT_INPUT, "INPUT")
        plan[item_id] = segments
    return plan


def parse_config(value):
    """校验 ci 的 config 并返回规范化结构；结构、键集、版本、类型、范围、
    编码、重复项或交叉约束一律 INPUT。B 限流与 faults 对后端的引用在执行期
    判 BACKEND。

    version=1 为原结构（仅 version/backends/vnodes/limits/overload 五键），
    sticky/idle/backpressure/faults 一律视为 null/空；version=2 须精确含
    追加三键，sticky/idle 为 null 或 {"ttl":整数}，backpressure 为 null 或
    {"low","high"}，非 null 时 overload 必非 null 且 low<high≤overload.q；
    version=3/4/5/6 在 v2 八键末追加 scheduler，须精确为 {"pick":...} 单键
    对象，v3 仅收 W/R，v4 收 W/R/L，v5/v6 收 W/R/L/H；v5/v6 选 H 时
    vnodes 须非 null。v1/v2 的 scheduler 缺省等价于 W。version=6 的
    backends 项在既有七键后须含 endpoint（null 或精确 {host,port} 对象），
    v1..v5 项不含该键、一律视为 null。version=7 在既有九键末追加 faults
    （数组，项键序 id,k,a,z,v，同 id 段不重叠），backends 项同 v6；
    v1..v6 一律视 faults 为空。version=8 在既有十键末追加 quotas（数组，
    项精确键序 scope,id,limit,span，按 B/C/S 及 id 的 UTF-8 字节升序），
    backends 项同 v6；v1..v7 一律视 quotas 为空。v8 十一键另须严格按
    version,backends,vnodes,limits,overload,sticky,idle,backpressure,
    scheduler,faults,quotas 顺序出现，乱序（含交换 faults/quotas）报
    INPUT。limits/quotas 项的 scope 先验证为字符串再判断 B/C/S，数组或
    对象等同报 INPUT。version=9 在既有十一键末追加 queue（精确两键对象，
    键序 dequeue,full；dequeue 仅 F/S，full 仅 T/H），十二键结构只可能为
    v9 且十二键须严格按声明顺序出现（queue 末置，任何乱序报 INPUT）；
    v1..v8 结构不含 queue，一律视为 dequeue=F、full=T。version=10 在既有
    十二键末追加 capacities（数组，项精确键序 id,cap；id 引用本配置后端、
    cap ∈ [1,10^6] 非 bool 整数；id 重复或项键序/类型/范围/编码非法判
    INPUT，未知 id 留执行期判 BACKEND；输入顺序不限），十三键结构只可能
    为 v10 且十三键须严格按声明顺序出现（capacities 末置，任何乱序报
    INPUT）；v1..v9 结构不含 capacities，一律视为空（[]，即全部使用
    overload.cap）。version=11 在既有十三键末追加 lifetime（null 或精确
    {"ttl":整数} 单键对象，ttl ∈ [1,10^9] 非 bool 整数，同 tm.ttl；不含
    运行态），十四键结构只可能为 v11 且十四键须严格按声明顺序出现
    （lifetime 末置，任何乱序报 INPUT）；v1..v10 结构不含 lifetime，一律
    规范化为 null。返回的规范化结构额外含 "queue": (dequeue, full)、
    "capacities": [(id, cap), ...]（按后端加入序，仅显式覆盖）与
    "lifetime": None/ttl 整数。"""
    if not isinstance(value, dict):
        fail(EXIT_INPUT, "INPUT")
    config_keys = set(value)
    v1_keys = {"version", "backends", "vnodes", "limits", "overload"}
    v2_keys = v1_keys | {"sticky", "idle", "backpressure"}
    v3_keys = v2_keys | {"scheduler"}
    # v7 在既有九键末追加 faults；十键结构只可能为 v7。
    v7_keys = v3_keys | {"faults"}
    # v8 在既有十键末追加 quotas；十一键结构只可能为 v8。
    v8_keys = v7_keys | {"quotas"}
    # v9 在既有十一键末追加 queue；十二键结构只可能为 v9。
    v9_keys = v8_keys | {"queue"}
    # v10 在既有十二键末追加 capacities；十三键结构只可能为 v10。
    v10_keys = v9_keys | {"capacities"}
    # v11 在既有十三键末追加 lifetime；十四键结构只可能为 v11。
    v11_keys = v10_keys | {"lifetime"}
    if config_keys == v1_keys:
        version = 1
    elif config_keys == v2_keys:
        version = 2
    elif config_keys == v3_keys:
        # 九键结构为 v3/v4/v5/v6 共用，具体版本由 version 字段区分。
        version = None
    elif config_keys == v7_keys:
        version = 7
    elif config_keys == v8_keys:
        # v8 十一键须严格按声明顺序出现：交换 faults/quotas 等任何乱序
        # 均报 INPUT（dict 保序即 JSON 键出现顺序）。
        if list(value) != [
            "version", "backends", "vnodes", "limits", "overload",
            "sticky", "idle", "backpressure", "scheduler", "faults",
            "quotas",
        ]:
            fail(EXIT_INPUT, "INPUT")
        version = 8
    elif config_keys == v9_keys:
        # v9 十二键须严格按声明顺序出现，queue 末置：任何乱序均报 INPUT。
        if list(value) != [
            "version", "backends", "vnodes", "limits", "overload",
            "sticky", "idle", "backpressure", "scheduler", "faults",
            "quotas", "queue",
        ]:
            fail(EXIT_INPUT, "INPUT")
        version = 9
    elif config_keys == v10_keys:
        # v10 十三键须严格按声明顺序出现，capacities 末置：任何乱序均报
        # INPUT。
        if list(value) != [
            "version", "backends", "vnodes", "limits", "overload",
            "sticky", "idle", "backpressure", "scheduler", "faults",
            "quotas", "queue", "capacities",
        ]:
            fail(EXIT_INPUT, "INPUT")
        version = 10
    elif config_keys == v11_keys:
        # v11 十四键须严格按声明顺序出现，lifetime 末置：任何乱序均报
        # INPUT。
        if list(value) != [
            "version", "backends", "vnodes", "limits", "overload",
            "sticky", "idle", "backpressure", "scheduler", "faults",
            "quotas", "queue", "capacities", "lifetime",
        ]:
            fail(EXIT_INPUT, "INPUT")
        version = 11
    else:
        fail(EXIT_INPUT, "INPUT")
    raw_version = value["version"]
    if not isinstance(raw_version, int) or isinstance(raw_version, bool):
        fail(EXIT_INPUT, "INPUT")
    if version is None:
        if raw_version not in (3, 4, 5, 6):
            fail(EXIT_INPUT, "INPUT")
        version = raw_version
    elif raw_version != version:
        fail(EXIT_INPUT, "INPUT")

    raw_backends = value["backends"]
    if not isinstance(raw_backends, list):
        fail(EXIT_INPUT, "INPUT")
    normalized_backends = []
    seen_backend_ids = set()
    for item in raw_backends:
        item_keys = {"id", "weight", "d", "fail", "success", "circuit", "drain"}
        if version >= 6:
            # v6+ 项在既有七键后追加 endpoint；v1..v5 项精确为七键。
            item_keys = item_keys | {"endpoint"}
        if not isinstance(item, dict) or set(item) != item_keys:
            fail(EXIT_INPUT, "INPUT")
        backend_id = parse_backend_id(item["id"])
        # 后端 id 须 UTF-8 可编码（建环与输出排序都会用到其字节）。
        encode_backend_id(backend_id)
        if backend_id in seen_backend_ids:
            fail(EXIT_INPUT, "INPUT")
        seen_backend_ids.add(backend_id)
        weight = parse_threshold(item["weight"])
        d = parse_duration(item["d"])
        fail_threshold = parse_threshold(item["fail"])
        success_threshold = parse_threshold(item["success"])
        raw_circuit = item["circuit"]
        if raw_circuit is None:
            circuit_params = None
        else:
            if not isinstance(raw_circuit, dict) or set(raw_circuit) != {
                "n", "m", "r", "w", "q",
            }:
                fail(EXIT_INPUT, "INPUT")
            n = parse_threshold(raw_circuit["n"])
            r = parse_threshold(raw_circuit["r"])
            q = parse_threshold(raw_circuit["q"])
            m = raw_circuit["m"]
            # m ∈ [1,n]，与 cs 同范围；bool 显式排除。
            if not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= n:
                fail(EXIT_INPUT, "INPUT")
            w = parse_circuit_window(raw_circuit["w"])
            circuit_params = (n, m, r, w, q)
        raw_drain = item["drain"]
        drain_t = None if raw_drain is None else parse_drain_timeout(raw_drain)
        if version >= 6:
            raw_endpoint = item["endpoint"]
            if raw_endpoint is None:
                endpoint = None
            else:
                # 精确 {host,port} 对象，校验同 ep。
                if not isinstance(raw_endpoint, dict) or set(raw_endpoint) != {
                    "host", "port",
                }:
                    fail(EXIT_INPUT, "INPUT")
                endpoint = (
                    parse_endpoint_host(raw_endpoint["host"]),
                    parse_endpoint_port(raw_endpoint["port"]),
                )
        else:
            # v1..v5 结构不含 endpoint，一律视为 null。
            endpoint = None
        normalized_backends.append(
            (
                backend_id,
                weight,
                d,
                fail_threshold,
                success_threshold,
                circuit_params,
                drain_t,
                endpoint,
            )
        )

    vnodes = None if value["vnodes"] is None else parse_vnodes(value["vnodes"])

    raw_limits = value["limits"]
    if not isinstance(raw_limits, list):
        fail(EXIT_INPUT, "INPUT")
    normalized_limits = []
    seen_buckets = set()
    scope_rank = {"B": 0, "C": 1, "S": 2}
    previous_order_key = None
    for item in raw_limits:
        if not isinstance(item, dict) or set(item) != {"scope", "id", "r", "b"}:
            fail(EXIT_INPUT, "INPUT")
        scope = item["scope"]
        # 先验证为字符串再判断 B/C/S：数组或对象等不可哈希值同报 INPUT，
        # 不得落入 unhashable 的 traceback。
        if not isinstance(scope, str) or scope not in scope_rank:
            fail(EXIT_INPUT, "INPUT")
        bucket_id = parse_key(item["id"])
        r = parse_rate(item["r"])
        b = parse_burst(item["b"])
        pair = (scope, bucket_id)
        if pair in seen_buckets:
            fail(EXIT_INPUT, "INPUT")
        seen_buckets.add(pair)
        order_key = (scope_rank[scope], bucket_id.encode("utf-8"))
        if previous_order_key is not None and not previous_order_key < order_key:
            # 必须严格按 scope 的 B/C/S 序、id 的 UTF-8 字节升序排列。
            fail(EXIT_INPUT, "INPUT")
        previous_order_key = order_key
        normalized_limits.append((scope, bucket_id, r, b))

    raw_overload = value["overload"]
    if raw_overload is None:
        overload = None
    else:
        if not isinstance(raw_overload, dict) or set(raw_overload) != {
            "cap", "q", "ttl",
        }:
            fail(EXIT_INPUT, "INPUT")
        overload = (
            parse_queue_param(raw_overload["cap"]),
            parse_queue_param(raw_overload["q"]),
            parse_queue_param(raw_overload["ttl"]),
        )

    if version == 1:
        # 原结构不含三项：一律 null（sticky/idle 不登记，背压取消）。
        sticky_ttl = None
        idle_ttl = None
        backpressure = None
    else:
        raw_sticky = value["sticky"]
        if raw_sticky is None:
            sticky_ttl = None
        else:
            # 键序仅影响输出，键集精确为 op 的 ttl 包装 {"ttl":整数}。
            if not isinstance(raw_sticky, dict) or set(raw_sticky) != {"ttl"}:
                fail(EXIT_INPUT, "INPUT")
            sticky_ttl = parse_sticky_ttl(raw_sticky["ttl"])
        raw_idle = value["idle"]
        if raw_idle is None:
            idle_ttl = None
        else:
            if not isinstance(raw_idle, dict) or set(raw_idle) != {"ttl"}:
                fail(EXIT_INPUT, "INPUT")
            idle_ttl = parse_idle_ttl(raw_idle["ttl"])
        raw_bp = value["backpressure"]
        if raw_bp is None:
            backpressure = None
        else:
            if not isinstance(raw_bp, dict) or set(raw_bp) != {"low", "high"}:
                fail(EXIT_INPUT, "INPUT")
            bp_low = parse_bp_num(raw_bp["low"])
            bp_high = parse_bp_num(raw_bp["high"])
            # 交叉约束：非 null 时 overload 必非 null 且 low<high≤overload.q。
            if overload is None or not bp_low < bp_high or bp_high > overload[1]:
                fail(EXIT_INPUT, "INPUT")
            backpressure = (bp_low, bp_high)

    if version >= 3:
        # scheduler 精确为 {"pick":...} 单键对象：缺失（v1/v2 键集不含该
        # 键，已在上文分流）不会出现；多键、非对象、键名错误或 pick 非
        # 字符串均报 INPUT；v3 仅收 W/R，v4 收 W/R/L，v5..v11 收
        # W/R/L/H。
        raw_scheduler = value["scheduler"]
        if (
            not isinstance(raw_scheduler, dict)
            or set(raw_scheduler) != {"pick"}
        ):
            fail(EXIT_INPUT, "INPUT")
        allowed = (
            ("W", "R")
            if version == 3
            else (("W", "R", "L") if version == 4 else ("W", "R", "L", "H"))
        )
        if raw_scheduler["pick"] not in allowed:
            fail(EXIT_INPUT, "INPUT")
        scheduler = raw_scheduler["pick"]
        # v5..v11 选 H 时 vnodes 须非 null（一致性哈希环必须已配置）。
        if scheduler == "H" and vnodes is None:
            fail(EXIT_INPUT, "INPUT")
    else:
        # v1/v2 旧结构等价于既有平滑加权 W。
        scheduler = "W"

    # faults：version>=7 的结构含该键且精确为数组；v1..v6 一律视为空
    # 计划（不校验段，不载入时间线）。
    faults_plan = parse_config_faults(value["faults"]) if version >= 7 else {}

    if version >= 8:
        # quotas 精确为数组，项精确键序 scope,id,limit,span（键须按此序
        # 出现）：scope 仅 B/C/S，id 为非空 UTF-8 串，limit ∈ [1,10^18]、
        # span ∈ [1,10^9] 非 bool 整数；数组按 scope 的 B/C/S 序、id 的
        # UTF-8 字节升序（重复 (scope,id) 亦被严格升序拒绝），不含
        # window、used。结构、键序、类型、范围、编码或顺序非法判 INPUT；
        # B 项引用未知后端留执行期判 BACKEND（同 B 限流）。
        raw_quotas = value["quotas"]
        if not isinstance(raw_quotas, list):
            fail(EXIT_INPUT, "INPUT")
        normalized_quotas = []
        previous_quota_key = None
        for item in raw_quotas:
            if not isinstance(item, dict) or list(item) != [
                "scope", "id", "limit", "span",
            ]:
                fail(EXIT_INPUT, "INPUT")
            scope = item["scope"]
            # 同 limits：先验证为字符串再判断 B/C/S，数组或对象同报 INPUT。
            if not isinstance(scope, str) or scope not in scope_rank:
                fail(EXIT_INPUT, "INPUT")
            quota_id = parse_key(item["id"])
            limit = parse_quota_limit(item["limit"])
            span = parse_quota_span(item["span"])
            order_key = (scope_rank[scope], quota_id.encode("utf-8"))
            if previous_quota_key is not None and not previous_quota_key < order_key:
                fail(EXIT_INPUT, "INPUT")
            previous_quota_key = order_key
            normalized_quotas.append((scope, quota_id, limit, span))
    else:
        # v1..v7 结构不含 quotas，一律视为空（成功即清空全部配额）。
        normalized_quotas = []

    if version >= 9:
        # queue 精确为两键对象且键须按 dequeue,full 顺序出现：dequeue 仅
        # F/S（遇阻即停/跳过阻塞），full 仅 T/H（尾拒绝/头淘汰）；不含
        # 等待项、evicted 或 last 等运行态。结构、键序、类型或枚举值非法
        # 判 INPUT。v9/v10/v11 结构含该键；v1..v8 走缺省 F/T。
        raw_queue = value["queue"]
        if not isinstance(raw_queue, dict) or list(raw_queue) != [
            "dequeue", "full",
        ]:
            fail(EXIT_INPUT, "INPUT")
        dequeue_mode = raw_queue["dequeue"]
        full_policy = raw_queue["full"]
        if (
            not isinstance(dequeue_mode, str)
            or dequeue_mode not in ("F", "S")
            or not isinstance(full_policy, str)
            or full_policy not in ("T", "H")
        ):
            fail(EXIT_INPUT, "INPUT")
        queue_policy = (dequeue_mode, full_policy)
    else:
        # v1..v8 结构不含 queue：dequeue 默认 F，full 默认 T。
        queue_policy = ("F", "T")

    if version >= 10:
        # capacities 精确为数组，项精确键序 id,cap（键须按此序出现）：
        # id 为非空 UTF-8 串且引用本配置后端（未知 id 留执行期判 BACKEND，
        # 同 B 限流/配额与 faults），cap ∈ [1,10^6] 非 bool 整数；输入顺序
        # 不限但 id 不得重复。结构、非数组、项键序、类型、范围、编码非法或
        # id 重复判 INPUT。规范化为 id -> cap 映射（保留未知 id 供执行期
        # BACKEND 判定）；导出时按后端加入序仅列已知显式覆盖。
        raw_capacities = value["capacities"]
        if not isinstance(raw_capacities, list):
            fail(EXIT_INPUT, "INPUT")
        normalized_capacities = {}
        for item in raw_capacities:
            if not isinstance(item, dict) or list(item) != ["id", "cap"]:
                fail(EXIT_INPUT, "INPUT")
            override_id = parse_backend_id(item["id"])
            cap = parse_queue_param(item["cap"])
            if override_id in normalized_capacities:
                fail(EXIT_INPUT, "INPUT")
            normalized_capacities[override_id] = cap
    else:
        # v1..v9 结构不含 capacities：{} 即全部后端使用 overload.cap，
        # 成功热加载时清空已有覆盖。
        normalized_capacities = {}

    if version == 11:
        # lifetime 精确为 null 或 {"ttl":整数} 单键对象（键集精确为 ttl，
        # 键序仅影响输出）：ttl ∈ [1,10^9] 非 bool 整数（同 tm.ttl）。
        # 结构、类型或范围非法判 INPUT；不含任何运行态。null 表示未登记
        # 硬时限，成功热加载时清除既有运行态登记。
        raw_lifetime = value["lifetime"]
        if raw_lifetime is None:
            lifetime_ttl = None
        else:
            if not isinstance(raw_lifetime, dict) or set(raw_lifetime) != {
                "ttl",
            }:
                fail(EXIT_INPUT, "INPUT")
            lifetime_ttl = parse_idle_ttl(raw_lifetime["ttl"])
    else:
        # v1..v10 结构不含 lifetime：一律规范化为 null（成功热加载即清除
        # 既有 tm 硬时限登记）。
        lifetime_ttl = None

    return {
        "backends": normalized_backends,
        "vnodes": vnodes,
        "limits": normalized_limits,
        "overload": overload,
        "sticky": sticky_ttl,
        "idle": idle_ttl,
        "backpressure": backpressure,
        "scheduler": scheduler,
        "faults": faults_plan,
        "quotas": normalized_quotas,
        "queue": queue_policy,
        "capacities": normalized_capacities,
        "lifetime": lifetime_ttl,
    }


def export_normalized_config(config):
    """把 parse_config 的规范化结构导出为 version=11 配置对象：逐层键序、
    值格式与数组排序同 ce.config（backends 按配置出现序，项 id,weight,d,
    fail,success,circuit,drain,endpoint；limits/quotas 已由 parse_config
    强制按 scope 的 B/C/S 序、id 的 UTF-8 字节升序；faults 按后端出现序、
    段 a 升序，项键序 id,k,a,z,v；queue 键序 dequeue,full；capacities
    末置（其后为 lifetime），按后端加入序仅列显式覆盖，项键序 id,cap；
    lifetime 末置，为 null 或 {"ttl":整数}）。纯登记值、不含任何
    运行态；返回全新结构。O(N)，N 为 config 元素数。"""
    exported_backends = []
    for (backend_id, weight, d, fail_threshold, success_threshold,
         circuit_params, drain_t, endpoint) in config["backends"]:
        exported_backends.append(
            {
                "id": backend_id,
                "weight": weight,
                "d": d,
                "fail": fail_threshold,
                "success": success_threshold,
                "circuit": (
                    None
                    if circuit_params is None
                    else {
                        "n": circuit_params[0],
                        "m": circuit_params[1],
                        "r": circuit_params[2],
                        "w": circuit_params[3],
                        "q": circuit_params[4],
                    }
                ),
                "drain": drain_t,
                # 既有七键后追加 endpoint：null 或键序 host,port。
                "endpoint": (
                    None
                    if endpoint is None
                    else {"host": endpoint[0], "port": endpoint[1]}
                ),
            }
        )
    exported_limits = [
        {"scope": scope, "id": bucket_id, "r": r, "b": b}
        for scope, bucket_id, r, b in config["limits"]
    ]
    overload = config["overload"]
    # faults 按后端出现序、同后端段 a 升序（plan 段已为该序）；空计划为 []。
    faults_plan = config["faults"]
    exported_faults = []
    for entry in config["backends"]:
        for k, a, z, v in faults_plan.get(entry[0], ()):
            exported_faults.append(
                {"id": entry[0], "k": k, "a": a, "z": z, "v": v}
            )
    exported_quotas = [
        {"scope": scope, "id": quota_id, "limit": limit, "span": span}
        for scope, quota_id, limit, span in config["quotas"]
    ]
    sticky_ttl = config["sticky"]
    idle_ttl = config["idle"]
    backpressure = config["backpressure"]
    capacities = config["capacities"]
    lifetime = config["lifetime"]
    # 按后端加入序仅列显式覆盖；未知 id 已在执行期判 BACKEND，导出路径上
    # 不会出现，但仍按已知后端过滤以保持纯函数。
    exported_capacities = [
        {"id": backend_entry[0], "cap": capacities[backend_entry[0]]}
        for backend_entry in config["backends"]
        if backend_entry[0] in capacities
    ]
    return {
        "version": 11,
        "backends": exported_backends,
        "vnodes": config["vnodes"],
        "limits": exported_limits,
        "overload": (
            None
            if overload is None
            else {"cap": overload[0], "q": overload[1], "ttl": overload[2]}
        ),
        "sticky": None if sticky_ttl is None else {"ttl": sticky_ttl},
        "idle": None if idle_ttl is None else {"ttl": idle_ttl},
        "backpressure": (
            None
            if backpressure is None
            else {"low": backpressure[0], "high": backpressure[1]}
        ),
        "scheduler": {"pick": config["scheduler"]},
        "faults": exported_faults,
        "quotas": exported_quotas,
        "queue": {"dequeue": config["queue"][0], "full": config["queue"][1]},
        "capacities": exported_capacities,
        "lifetime": None if lifetime is None else {"ttl": lifetime},
    }


def config_digest(exported):
    """ce.config 规范化 version=11 对象的指纹：按逐层键序序列化为 UTF-8
    紧凑 JSON（非 ASCII 不转义、无末尾换行）后取 SHA-256，返回小写 64 位
    十六进制。exported 为 export_config 产出的结构。O(N)，N 为规范化
    配置大小。"""
    canonical = json.dumps(
        exported, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


# ad 段级差异的顶层段序：ce 规范化 version=11 配置的顶层键序（version 首
# 置）排除 version；与 CU_SECTIONS 同序。
AD_SECTIONS = (
    "backends", "vnodes", "limits", "overload", "sticky", "idle",
    "backpressure", "scheduler", "faults", "quotas", "queue",
    "capacities", "lifetime",
)


def section_digest(value):
    """单个顶层段规范化 JSON 值的指纹：与 config_digest 同款紧凑 UTF-8
    （ensure_ascii=False、分隔符 ,/:、固定键序）编码后取 SHA-256，返回
    小写 64 位十六进制。O(L)，L 为该段编码长度。"""
    canonical = json.dumps(
        value, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def config_section_changes(before_export, after_export):
    """计算两份 ce 规范化 v11 导出（操作前/后）的段级差异：按 AD_SECTIONS
    的固定顶层键序遍历（排除 version），仅收前后值不同的段，每项为
    (section, before_fingerprint, after_fingerprint) 三元组（只存指纹、不
    复制整份配置）。两份导出本就逐层固定键序，段值 != 比较与各自规范化编码
    的逐字节比较等价。时间 O(C)，C 为配置编码长度；额外空间 O(S)，S 为
    变化段数（上界为规范化顶层段数 13）。"""
    changes = []
    for section in AD_SECTIONS:
        before_value = before_export[section]
        after_value = after_export[section]
        if before_value != after_value:
            changes.append(
                (
                    section,
                    section_digest(before_value),
                    section_digest(after_value),
                )
            )
    return changes


def parse_base(value):
    # ci 乐观并发的 base：小写 64 位十六进制串（ct 输出的配置指纹）；
    # 类型、长度或字符集不符判 INPUT。
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


# cu 单字段配置热加载允许替换的顶层段：ce.config 逐层键序中除 version 外
# 的任一键（version 恒规范化为 11，不可替换）。
CU_SECTIONS = (
    "backends", "vnodes", "limits", "overload", "sticky", "idle",
    "backpressure", "scheduler", "faults", "quotas", "queue",
    "capacities", "lifetime",
)


def parse_op(raw_op):
    """校验单个操作的形状，返回规范化元组；不合格式直接 INPUT 退出。"""
    if not isinstance(raw_op, dict):
        fail(EXIT_INPUT, "INPUT")

    name = raw_op.get("op")
    if name not in (
        "add", "remove", "pick", "open", "close", "get",
        "hset", "probe", "hget", "chash", "route", "ws", "wg",
        "cs", "cr", "cg", "ds", "dr", "du", "dg", "dq", "dx",
        "ss",
        "ls", "la", "lg", "qs", "qg",
        "os", "pc", "pg", "oa", "ot", "og", "oc", "oh", "wh", "wp", "wa", "bp", "bq", "qp", "rp", "rg",
        "qa",
        "lh",
        "lt",
        "le",
        "oq",
        "mr", "mg", "mh", "ms", "mx", "rh", "rt", "rr", "ra", "ma", "mo", "lp", "pa", "ph", "xa", "xh", "xg", "xp",
        "na",
        "ce", "ci", "cl", "al", "ai", "ad", "ag", "cb", "cu", "cv", "ct", "cd", "pd", "hd", "hb",
        "cp", "cq", "ca", "cx", "cy",
        "fs", "fx", "fr", "fi", "ft", "oi", "od",
        "fb", "fp", "fq", "fd", "fc",
        "br",
        "hm", "fm", "fh",
        "fa", "fe", "ah",
        "ea", "eh",
        "ts", "tm", "te", "tk", "tg", "tx",
        "ep", "fw",
        "eq", "ec",
        "er", "ex",
        "ey", "eb",
        "ez",
        "ej",
        "es", "en", "eu", "ei", "ed", "eo", "ev", "ew", "ek", "el", "em",
        "eg", "ee", "ef", "et",
        "ru",
        "ua",
        "mu",
        "se", "sd", "si", "sx", "sm",
    ):
        fail(EXIT_INPUT, "INPUT")

    keys = set(raw_op)
    if name == "add":
        if keys == {"op", "id", "weight"}:
            # 三键旧形式：d=0，立即取目标权重，now 不参与时钟。
            return (
                "add",
                parse_backend_id(raw_op["id"]),
                parse_threshold(raw_op["weight"]),
                0,
                None,
            )
        if keys == {"op", "id", "weight", "d", "now"}:
            return (
                "add",
                parse_backend_id(raw_op["id"]),
                parse_threshold(raw_op["weight"]),
                parse_duration(raw_op["d"]),
                parse_warm_now(raw_op["now"]),
            )
        fail(EXIT_INPUT, "INPUT")

    if name == "remove":
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        return ("remove", parse_backend_id(raw_op["id"]))

    if name == "pick":
        if keys == {"op"}:
            # 原 pick 键集：W/R/L 模式专用，行为不变。
            return ("pick", None, None)
        if keys == {"op", "key"}:
            # H（一致性哈希）二键形式：now 占位为 None，沿用无过期语义。
            return ("pick", parse_key(raw_op["key"]), None)
        if keys == {"op", "key", "now"}:
            # H 三键形式：限时粘性，now ∈ [0,10^9] 非 bool 整数，纳入共用
            # 非递减时钟；未 ss 留执行期报 STATE。
            return ("pick", parse_key(raw_op["key"]),
                    parse_warm_now(raw_op["now"]))
        fail(EXIT_INPUT, "INPUT")

    if name == "open":
        if keys != {"op", "cid", "flow", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "open",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_now(raw_op["now"]),
        )

    if name == "close":
        if keys != {"op", "cid", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("close", parse_cid(raw_op["cid"]), parse_now(raw_op["now"]))

    if name == "hset":
        if keys != {"op", "id", "fail", "success"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "hset",
            parse_backend_id(raw_op["id"]),
            parse_threshold(raw_op["fail"]),
            parse_threshold(raw_op["success"]),
        )

    if name == "probe":
        if keys != {"op", "id", "ok", "now"}:
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        # ok 只接受真正的 bool。
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return (
            "probe",
            parse_backend_id(raw_op["id"]),
            ok,
            parse_now(raw_op["now"]),
        )

    if name == "hget":
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        return ("hget", parse_backend_id(raw_op["id"]))

    if name == "ws":
        if keys != {"op", "id", "weight", "d", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "ws",
            parse_backend_id(raw_op["id"]),
            parse_threshold(raw_op["weight"]),
            parse_duration(raw_op["d"]),
            parse_warm_now(raw_op["now"]),
        )

    if name == "wg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("wg", parse_backend_id(raw_op["id"]), parse_warm_now(raw_op["now"]))

    if name == "chash":
        if keys != {"op", "vnodes"}:
            fail(EXIT_INPUT, "INPUT")
        return ("chash", parse_vnodes(raw_op["vnodes"]))

    if name == "ss":
        if keys != {"op", "ttl"}:
            fail(EXIT_INPUT, "INPUT")
        return ("ss", parse_sticky_ttl(raw_op["ttl"]))

    if name == "route":
        if keys == {"op", "key"}:
            # 旧二键形式：now 占位为 None，沿用无过期的旧语义。
            return ("route", parse_key(raw_op["key"]), None)
        if keys == {"op", "key", "now"}:
            # 三键形式：限时粘性，未 ss 时执行期报 STATE。
            return ("route", parse_key(raw_op["key"]), parse_now(raw_op["now"]))
        fail(EXIT_INPUT, "INPUT")

    if name == "cs":
        if keys != {"op", "id", "n", "m", "r", "w", "q"}:
            fail(EXIT_INPUT, "INPUT")
        n = parse_threshold(raw_op["n"])
        r = parse_threshold(raw_op["r"])
        q = parse_threshold(raw_op["q"])
        m = raw_op["m"]
        # m ∈ [1, n]，须先校验 n；bool 是 int 的子类，必须显式排除。
        if not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= n:
            fail(EXIT_INPUT, "INPUT")
        return (
            "cs",
            parse_backend_id(raw_op["id"]),
            n,
            m,
            r,
            parse_circuit_window(raw_op["w"]),
            q,
        )

    if name == "cr":
        if keys != {"op", "id", "ok", "now"}:
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        # ok 只接受真正的 bool。
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return (
            "cr",
            parse_backend_id(raw_op["id"]),
            ok,
            parse_now(raw_op["now"]),
        )

    if name == "cg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("cg", parse_backend_id(raw_op["id"]), parse_now(raw_op["now"]))

    if name == "ds":
        if keys != {"op", "id", "t"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "ds",
            parse_backend_id(raw_op["id"]),
            parse_drain_timeout(raw_op["t"]),
        )

    if name in ("dr", "du", "dg"):
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (name, parse_backend_id(raw_op["id"]), parse_now(raw_op["now"]))

    if name in ("dq", "dx"):
        # 全池排空投影（dq）与批量到期执行（dx）：精确键序 op,now（键须按
        # 此序出现，乱序或缺/多键报 INPUT）；now 为非负非 bool 整数
        # （parse_now 不设上界），纳入共用非递减时钟（倒退执行期判 INPUT）。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return (name, parse_now(raw_op["now"]))

    if name == "ls":
        if keys != {"op", "scope", "id", "r", "b", "now"}:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "ls",
            scope,
            parse_key(raw_op["id"]),
            parse_rate(raw_op["r"]),
            parse_burst(raw_op["b"]),
            parse_now(raw_op["now"]),
        )

    if name == "la":
        if keys == {"op", "c", "s", "key", "now"}:
            # 旧键集：等价于三项成本均为 1。
            bc = cc = sc = 1
        elif keys == {"op", "c", "s", "key", "bc", "cc", "sc", "now"}:
            bc = parse_cost(raw_op["bc"])
            cc = parse_cost(raw_op["cc"])
            sc = parse_cost(raw_op["sc"])
            if bc == 0 and cc == 0 and sc == 0:
                # 三项成本全零非法。
                fail(EXIT_INPUT, "INPUT")
        else:
            fail(EXIT_INPUT, "INPUT")
        return (
            "la",
            parse_key(raw_op["c"]),
            parse_key(raw_op["s"]),
            parse_key(raw_op["key"]),
            bc,
            cc,
            sc,
            parse_now(raw_op["now"]),
        )

    if name == "lg":
        if keys != {"op", "scope", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "lg",
            scope,
            parse_key(raw_op["id"]),
            parse_now(raw_op["now"]),
        )

    if name == "qs":
        if keys != {"op", "scope", "id", "limit", "span", "now"}:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "qs",
            scope,
            parse_key(raw_op["id"]),
            parse_quota_limit(raw_op["limit"]),
            parse_quota_span(raw_op["span"]),
            parse_warm_now(raw_op["now"]),
        )

    if name == "qg":
        if keys != {"op", "scope", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "qg",
            scope,
            parse_key(raw_op["id"]),
            parse_warm_now(raw_op["now"]),
        )

    if name == "os":
        if keys != {"op", "cap", "q", "ttl"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "os",
            parse_queue_param(raw_op["cap"]),
            parse_queue_param(raw_op["q"]),
            parse_queue_param(raw_op["ttl"]),
        )

    if name == "pc":
        # 每后端接纳容量覆盖：精确键序 op,id,cap（键须按此序出现）；id 沿用
        # 非空字符串校验（未知 id 执行期判 BACKEND），cap ∈ [1,10^6] 非 bool
        # 整数，键序、类型或范围非法报 INPUT。无 now，不推进时钟。
        if list(raw_op) != ["op", "id", "cap"]:
            fail(EXIT_INPUT, "INPUT")
        return (
            "pc",
            parse_backend_id(raw_op["id"]),
            parse_queue_param(raw_op["cap"]),
        )

    if name == "pg":
        # 每后端接纳容量覆盖查询：精确键序 op,id（键须按此序出现），只读；
        # 非法 id 在解析期判 INPUT，未知 id 执行期判 BACKEND，未配置覆盖判
        # STATE（按此顺序）。无 now，不推进时钟。
        if list(raw_op) != ["op", "id"]:
            fail(EXIT_INPUT, "INPUT")
        return ("pg", parse_backend_id(raw_op["id"]))

    if name == "oa":
        if keys == {"op", "cid", "flow", "c", "s", "key", "now"}:
            # 旧键集：等价于三项成本均为 1。
            bc = cc = sc = 1
        elif keys == {"op", "cid", "flow", "c", "s", "key", "bc", "cc", "sc", "now"}:
            bc = parse_cost(raw_op["bc"])
            cc = parse_cost(raw_op["cc"])
            sc = parse_cost(raw_op["sc"])
            if bc == 0 and cc == 0 and sc == 0:
                # 三项成本全零非法。
                fail(EXIT_INPUT, "INPUT")
        else:
            fail(EXIT_INPUT, "INPUT")
        return (
            "oa",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_key(raw_op["c"]),
            parse_key(raw_op["s"]),
            parse_key(raw_op["key"]),
            bc,
            cc,
            sc,
            parse_now(raw_op["now"]),
        )

    if name == "ot":
        if keys != {"op", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("ot", parse_now(raw_op["now"]))

    if name == "og":
        if keys != {"op"}:
            fail(EXIT_INPUT, "INPUT")
        return ("og",)

    if name == "oc":
        # 精确键集 op,cid，无 now；cid 沿用非空字符串校验。
        if keys != {"op", "cid"}:
            fail(EXIT_INPUT, "INPUT")
        return ("oc", parse_cid(raw_op["cid"]))

    if name == "oh":
        # 过载分钟历史：精确键序 op,from,to,now（键须按此序出现），只读；
        # 数值与窗关系约束同 ra/ma，未 os 与 from 过早的 STATE 留执行期判。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("oh", start, end, now)

    if name == "wh":
        # 排队等待历史：精确键序 op,from,to,now（键须按此序出现），只读；
        # 数值与窗关系约束同 oh，未 os 与 from 过早的 STATE 留执行期判。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("wh", start, end, now)

    if name == "wp":
        # 排队等待分位查询：精确键序 op,kind,from,to,p,now（键须按此序
        # 出现），只读；kind 仅 A/E/C/V（对应
        # admitted/expired/cancelled/evicted），from/to/now 为 0..10^9 非
        # bool 整数，p 为 1..100 非 bool 整数；数值与窗关系约束同 wh，未 os
        # 与 from 过早留执行期判。
        if list(raw_op) != ["op", "kind", "from", "to", "p", "now"]:
            fail(EXIT_INPUT, "INPUT")
        kind = raw_op["kind"]
        if not isinstance(kind, str) or kind not in ("A", "E", "C", "V"):
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        percentile = raw_op["p"]
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(percentile, int)
            or isinstance(percentile, bool)
            or not 1 <= percentile <= 100
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("wp", kind, start, end, percentile, now)

    if name == "wa":
        # 排队等待分位告警：精确键序 op,kind,w,p,hi,lo,n,now（键须按此序
        # 出现）；kind 仅 A/E/C/V（对应 admitted/expired/cancelled/
        # evicted），w/now 为 0..10^9、p 为 1..100、hi/lo 为 0..4、n 为
        # 1..60 的非 bool 整数且 lo<hi；now 纳入共用非递减时钟（倒退执行
        # 期判 INPUT）。键序/kind/类型/范围/lo<hi 在此判 INPUT；窗关系
        # max(0,now//60-59)≤w<now//60、跳窗与变参留执行期判 STATE（未 os
        # 同样留执行期判 STATE）。
        if list(raw_op) != ["op", "kind", "w", "p", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        kind = raw_op["kind"]
        if not isinstance(kind, str) or kind not in ("A", "E", "C", "V"):
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        now = parse_metric_num(raw_op["now"])
        percentile = raw_op["p"]
        if (
            not isinstance(percentile, int)
            or isinstance(percentile, bool)
            or not 1 <= percentile <= 100
        ):
            fail(EXIT_INPUT, "INPUT")
        hi = raw_op["hi"]
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(hi, int)
            or isinstance(hi, bool)
            or not 0 <= hi <= 4
        ):
            fail(EXIT_INPUT, "INPUT")
        lo = raw_op["lo"]
        if (
            not isinstance(lo, int)
            or isinstance(lo, bool)
            or not 0 <= lo <= 4
        ):
            fail(EXIT_INPUT, "INPUT")
        if not lo < hi:
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("wa", kind, w, percentile, hi, lo, n, now)

    if name == "lh":
        # 多维限流历史：精确键序 op,scope,id,from,to,now（键须按此序出现），
        # 只读；scope 仅 B/C/S，id 沿用 ls 的非空 UTF-8 串校验（未知 B id、
        # 无桶无配额与 from 过早留执行期判）。数值与窗关系约束同 oh。
        if list(raw_op) != ["op", "scope", "id", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("lh", scope, parse_key(raw_op["id"]), start, end, now)

    if name == "lt":
        # 限流热点查询：精确键序 op,scope,from,to,k,now（键须按此序出现），
        # 只读；scope 仅 B/C/S，from/to/now 为 0..10^9 非 bool 整数，k 为
        # 1..1000 非 bool 整数；数值与窗关系约束同 lh，from 过早、无在配
        # 标识留执行期判。
        if list(raw_op) != ["op", "scope", "from", "to", "k", "now"]:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        limit_k = raw_op["k"]
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(limit_k, int)
            or isinstance(limit_k, bool)
            or not 1 <= limit_k <= 1000
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("lt", scope, start, end, limit_k, now)

    if name == "le":
        # 限流告警：精确键序 op,scope,id,w,hi,lo,n,now（键须按此序出现）；
        # scope 仅 B/C/S，id 沿用 ls 的非空 UTF-8 串校验（B 的未知 id 留
        # 执行期判 BACKEND），w/now 为 0..10^9、hi 为 1..10^18、lo 为
        # 0..(10^18-1)、n 为 1..60，皆为非 bool 整数且 lo<hi；now 纳入
        # 共用非递减时钟（倒退执行期判 INPUT）。键序、scope、类型、范围
        # 与 lo<hi 在此判 INPUT；窗关系 max(0,now//60-59)≤w<now//60、
        # 无桶无配额、跳窗与变参留执行期判 STATE。
        if list(raw_op) != ["op", "scope", "id", "w", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        scope = raw_op["scope"]
        if scope not in ("B", "C", "S"):
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        hi = parse_le_hi(raw_op["hi"])
        lo = parse_le_lo(raw_op["lo"])
        if not lo < hi:
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        return (
            "le", scope, parse_key(raw_op["id"]),
            w, hi, lo, n, now,
        )

    if name == "bp":
        if keys != {"op", "low", "high"}:
            fail(EXIT_INPUT, "INPUT")
        low = parse_bp_num(raw_op["low"])
        high = parse_bp_num(raw_op["high"])
        # 阈值关系 low < high 在解析期即校验（high ≤ os.q 留执行期）。
        if not low < high:
            fail(EXIT_INPUT, "INPUT")
        return ("bp", low, high)

    if name == "bq":
        if keys != {"op"}:
            fail(EXIT_INPUT, "INPUT")
        return ("bq",)

    if name == "qp":
        # 队列出队策略：精确键序 op,mode（键须按此序出现）；mode 为
        # F/S/P（F 遇阻即停、S 逐项各试一次、P 老化优先），键序、类型或
        # 值非法报 INPUT。
        if list(raw_op) != ["op", "mode"]:
            fail(EXIT_INPUT, "INPUT")
        mode = raw_op["mode"]
        if not isinstance(mode, str) or mode not in ("F", "S", "P"):
            fail(EXIT_INPUT, "INPUT")
        return ("qp", mode)

    if name == "qa":
        # 老化优先级登记：精确键序 op,step,items（键须按此序出现）；step 为
        # 1..10^9 非 bool 整数；items 至多 1000 项，项精确键序 s,p（键须按
        # 此序出现），s 为互异非空 UTF-8 串，p 为 0..10^9 非 bool 整数。
        # 键序、结构、字段或重复 s 非法报 INPUT。原子替换登记，未登记服务
        # 类基础优先级视为 p=0；登记不持久化（不随 ce/cl 导出）。
        if list(raw_op) != ["op", "step", "items"]:
            fail(EXIT_INPUT, "INPUT")
        step = parse_aging_step(raw_op["step"])
        raw_items = raw_op["items"]
        if not isinstance(raw_items, list) or len(raw_items) > 1000:
            fail(EXIT_INPUT, "INPUT")
        items = []
        seen_s = set()
        for raw_item in raw_items:
            if not isinstance(raw_item, dict) or list(raw_item) != ["s", "p"]:
                fail(EXIT_INPUT, "INPUT")
            service = parse_key(raw_item["s"])
            if service in seen_s:
                # 同批 s 互异。
                fail(EXIT_INPUT, "INPUT")
            seen_s.add(service)
            items.append((service, parse_aging_p(raw_item["p"])))
        return ("qa", step, items)

    if name == "rp":
        # FIFO 满载策略：精确键序 op,mode（键须按此序出现）；mode 仅 T/H
        # （T 尾拒绝、H 头淘汰），键序、类型或值非法报 INPUT。
        if list(raw_op) != ["op", "mode"]:
            fail(EXIT_INPUT, "INPUT")
        mode = raw_op["mode"]
        if not isinstance(mode, str) or mode not in ("T", "H"):
            fail(EXIT_INPUT, "INPUT")
        return ("rp", mode)

    if name == "rg":
        # 满载策略查询：精确键序仅 op，只读；非法即 INPUT。
        if list(raw_op) != ["op"]:
            fail(EXIT_INPUT, "INPUT")
        return ("rg",)

    if name == "oq":
        # 等待队列只读投影：精确键序 op,now（键须按此序出现）；now 为
        # 0..10^9 非 bool 整数，纳入共用非递减时钟；未 os 的 STATE 留执行
        # 期判（先于时钟推进），键序/类型/范围在此判 INPUT。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("oq", parse_metric_num(raw_op["now"]))

    if name == "mu":
        # 池级统一观测：精确键序 op,now（键须按此序出现），只读；now 为
        # 0..10^9 非 bool 整数，纳入共用非递减时钟（倒退执行期判 INPUT）。
        # 除推进时钟外不改变任何状态。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("mu", parse_metric_num(raw_op["now"]))

    if name == "mr":
        if keys != {"op", "id", "ok", "ms", "retries", "remaps", "now"}:
            fail(EXIT_INPUT, "INPUT")
        ok = raw_op["ok"]
        # ok 只接受真正的 bool。
        if not isinstance(ok, bool):
            fail(EXIT_INPUT, "INPUT")
        return (
            "mr",
            parse_backend_id(raw_op["id"]),
            ok,
            parse_metric_num(raw_op["ms"]),
            parse_metric_num(raw_op["retries"]),
            parse_metric_num(raw_op["remaps"]),
            parse_metric_num(raw_op["now"]),
        )

    if name == "mg":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "mg",
            parse_backend_id(raw_op["id"]),
            parse_metric_num(raw_op["now"]),
        )

    if name == "mh":
        if keys != {"op", "id", "from", "to", "now"}:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("mh", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "lp":
        # 后端延迟分位查询：精确键序 op,id,from,to,p,now（键须按此序出现），
        # 只读；from/to/now 为 0..10^9 非 bool 整数，p 为 1..100 非 bool
        # 整数；窗关系同 mh（from≤to≤now//60 且 to-from<60），from 过早的
        # STATE 留执行期判（未知 id 先 BACKEND）。
        if list(raw_op) != ["op", "id", "from", "to", "p", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        percentile = raw_op["p"]
        if (
            not isinstance(percentile, int)
            or isinstance(percentile, bool)
            or not 1 <= percentile <= 100
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("lp", parse_backend_id(raw_op["id"]),
                start, end, percentile, now)

    if name == "pa":
        # 后端延迟分位告警：精确键序 op,id,w,p,hi,lo,n,now（键须按此序
        # 出现）；id 沿用非空字符串校验（未知 id 留执行期判 BACKEND），
        # w/now 为 0..10^9、p 为 1..100、hi/lo 为 0..4、n 为 1..60，皆为
        # 非 bool 整数且 lo<hi；now 纳入共用非递减时钟（倒退执行期判
        # INPUT）。键序/类型/范围/lo<hi 在此判 INPUT；窗关系
        # max(0,now//60-59)≤w<now//60、跳窗与变参留执行期判 STATE。
        if list(raw_op) != ["op", "id", "w", "p", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        now = parse_metric_num(raw_op["now"])
        percentile = raw_op["p"]
        if (
            not isinstance(percentile, int)
            or isinstance(percentile, bool)
            or not 1 <= percentile <= 100
        ):
            fail(EXIT_INPUT, "INPUT")
        hi = raw_op["hi"]
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(hi, int)
            or isinstance(hi, bool)
            or not 0 <= hi <= 4
        ):
            fail(EXIT_INPUT, "INPUT")
        lo = raw_op["lo"]
        if (
            not isinstance(lo, int)
            or isinstance(lo, bool)
            or not 0 <= lo <= 4
        ):
            fail(EXIT_INPUT, "INPUT")
        if not lo < hi:
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        return (
            "pa", parse_backend_id(raw_op["id"]),
            w, percentile, hi, lo, n, now,
        )

    if name == "ph":
        # 后端延迟分位告警转换历史查询：精确键序 op,id,from,to,now（键须按
        # 此序出现），只读；id 沿用非空字符串校验（未知 id 留执行期判
        # BACKEND），from/to/now 为 0..10^9 非 bool 整数；窗关系
        # from≤to≤now//60 且 to-from<60，非法即 INPUT；now 纳入共用非
        # 递减时钟（倒退执行期判 INPUT）。from 过早与未知 id 留执行期判。
        if list(raw_op) != ["op", "id", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("ph", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "xa":
        # 每后端重试/重映射告警：精确键序 op,id,k,w,hi,lo,n,now（键须按此
        # 序出现）；id 沿用非空字符串校验（未知 id 留执行期判 BACKEND），
        # k 仅 R/M（分别取 w 窗 mh 口径的 retries/remaps 累计值），w/now
        # 为 0..10^9、hi 为 1..10^18、lo 为 0..(10^18-1)、n 为 1..60，皆为
        # 非 bool 整数且 lo<hi；now 纳入共用非递减时钟（倒退执行期判
        # INPUT）。键序/k/类型/范围/lo<hi 在此判 INPUT；窗关系
        # max(0,now//60-59)≤w<now//60、跳窗与变参留执行期判 STATE。
        if list(raw_op) != ["op", "id", "k", "w", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        kind = raw_op["k"]
        if not isinstance(kind, str) or kind not in ("R", "M"):
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        hi = parse_xa_hi(raw_op["hi"])
        lo = parse_xa_lo(raw_op["lo"])
        if not lo < hi:
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        return (
            "xa", parse_backend_id(raw_op["id"]),
            kind, w, hi, lo, n, now,
        )

    if name == "xh":
        # 每后端重试/重映射告警转换历史查询：精确键序
        # op,id,k,from,to,now（键须按此序出现），只读；id 沿用非空字符串
        # 校验（未知 id 留执行期判 BACKEND），k 仅 R/M，from/to/now 为
        # 0..10^9 非 bool 整数；窗关系 from≤to≤now//60 且 to-from<60，非法
        # 即 INPUT；now 纳入共用非递减时钟（倒退执行期判 INPUT）。from 过早
        # 与未知 id 留执行期判。
        if list(raw_op) != ["op", "id", "k", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        kind = raw_op["k"]
        if not isinstance(kind, str) or kind not in ("R", "M"):
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return (
            "xh", parse_backend_id(raw_op["id"]),
            kind, start, end, now,
        )

    if name == "xg":
        # 每后端重试/重映射告警汇总查询：精确键序 op,id,k,from,to,now（键
        # 须按此序出现），只读；id 沿用非空字符串校验（未知 id 留执行期
        # 判 BACKEND），k 仅 R/M，from/to/now 为 0..10^9 非 bool 整数；窗
        # 关系 from≤to≤now//60 且 to-from<60，非法即 INPUT；now 纳入共用
        # 非递减时钟（倒退执行期判 INPUT）。from 过早与未知 id 留执行期判。
        if list(raw_op) != ["op", "id", "k", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        kind = raw_op["k"]
        if not isinstance(kind, str) or kind not in ("R", "M"):
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return (
            "xg", parse_backend_id(raw_op["id"]),
            kind, start, end, now,
        )

    if name == "xp":
        # 池级 R/M 告警概览查询：精确键序 op,from,to,now（键须按此序出现），
        # 只读；from/to/now 为 0..10^9 非 bool 整数；窗关系
        # from≤to≤now//60 且 to-from<60，非法即 INPUT；now 纳入共用非
        # 递减时钟（倒退执行期判 INPUT）。from 过早留执行期判 STATE。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("xp", start, end, now)

    if name == "na":
        # 每后端并发告警：精确键序 op,id,w,hi,lo,n,now（键须按此序出现）；
        # id 沿用非空字符串校验（未知 id 留执行期判 BACKEND），w/now 为
        # 0..10^9、hi 为 1..10^9、lo 为 0..(10^9-1)、n 为 1..60，皆为非
        # bool 整数且 lo<hi；now 纳入共用非递减时钟（倒退执行期判
        # INPUT）。键序/类型/范围/lo<hi 在此判 INPUT；窗关系
        # max(0,now//60-59)≤w<now//60、跳窗与变参留执行期判 STATE。
        if list(raw_op) != ["op", "id", "w", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        hi = parse_na_hi(raw_op["hi"])
        lo = parse_na_lo(raw_op["lo"])
        if not lo < hi:
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        return (
            "na", parse_backend_id(raw_op["id"]),
            w, hi, lo, n, now,
        )

    if name == "ms":
        if keys != {"op", "id", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("ms", parse_backend_id(raw_op["id"]),
                parse_metric_num(raw_op["now"]))

    if name == "mx":
        if keys != {"op", "id", "from", "to", "now"}:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系同 mh：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("mx", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "rh":
        # 不可用原因分钟历史：精确键序 op,id,from,to,now（键须按此序出现），
        # 只读；数值与关系约束同 mh/mx/fh，from 过早的 STATE 留执行期判
        # （未知 id 先 BACKEND）。
        if list(raw_op) != ["op", "id", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("rh", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "rt":
        # 原因事件时刻查询：精确键序 op,id,from,to,now（键须按此序出现），
        # 只读；数值与关系约束同 rh，from 过早的 STATE 留执行期判
        # （未知 id 先 BACKEND）。
        if list(raw_op) != ["op", "id", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("rt", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "rr":
        # 重试/重映射时刻历史查询：精确键序 op,id,from,to,now（键须按此序
        # 出现），只读；id 为非空可编码 UTF-8 串，from/to/now 为 0..10^9
        # 非 bool 整数；窗关系 from≤to≤now//60 且 to-from<60，非法即
        # INPUT；now 纳入共用非递减时钟（倒退执行期判 INPUT）。from 过早
        # 与未知 id 留执行期判。
        if list(raw_op) != ["op", "id", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("rr", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "ra":
        # 全池不可用原因汇总：精确键序 op,from,to,now（键须按此序出现），
        # 只读；数值与窗关系约束同 rh，from 过早的 STATE 留执行期判。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("ra", start, end, now)

    if name == "ma":
        # 全池请求指标历史：精确键序 op,from,to,now（键须按此序出现），
        # 只读；数值与窗关系约束同 ra，from 过早的 STATE 留执行期判。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("ma", start, end, now)

    if name == "mo":
        # 全池增量快照：精确键序 op,seq,now（键须按此序出现），seq 为
        # 1..10^18 非 bool 整数，now 为 0..10^9 非 bool 整数；时钟倒退、
        # seq 序列关系（首项=1、逐次 +1、同 seq 异 now）留执行期判定。
        if list(raw_op) != ["op", "seq", "now"]:
            fail(EXIT_INPUT, "INPUT")
        seq = raw_op["seq"]
        if (
            not isinstance(seq, int)
            or isinstance(seq, bool)
            or not 1 <= seq <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("mo", seq, parse_metric_num(raw_op["now"]))

    if name == "fs":
        if keys != {"op", "id", "k", "a", "z", "v"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "fs",
            parse_backend_id(raw_op["id"]),
            parse_fault_segment(raw_op),
        )

    if name == "fb":
        if keys != {"op", "items"}:
            fail(EXIT_INPUT, "INPUT")
        raw_items = raw_op["items"]
        if not isinstance(raw_items, list):
            fail(EXIT_INPUT, "INPUT")
        # 先整体校验并规范化（键集、类型、范围、重复 id 均判 INPUT），
        # 未知 id 留到执行期判 BACKEND；失败批次原子回滚。
        plan = {}
        for item in raw_items:
            if not isinstance(item, dict) or set(item) != {"id", "k", "a", "z", "v"}:
                fail(EXIT_INPUT, "INPUT")
            item_id = parse_backend_id(item["id"])
            if item_id in plan:
                # 同一批次内 id 互异。
                fail(EXIT_INPUT, "INPUT")
            plan[item_id] = parse_fault_segment(item)
        return ("fb", plan)

    if name == "fp":
        # 故障时间线原子替换：精确键序 op,items（键须按此序出现）；items
        # 为 fp/fd 同款全量计划（parse_fault_plan：0..4096 项、项键序
        # id,k,a,z,v、同 id 段按 a 升序且不重叠）。键序/容器/项数/字段/
        # 重叠判 INPUT，未知 id 留执行期判 BACKEND。
        if list(raw_op) != ["op", "items"]:
            fail(EXIT_INPUT, "INPUT")
        return ("fp", parse_fault_plan(raw_op["items"]))

    if name == "fq":
        if keys != {"op", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("fq", parse_now(raw_op["now"]))

    if name == "br":
        # 全池运行态快照：精确键序 op,now（键须按此序出现）；now 为
        # 0..10^9 非 bool 整数，纳入共用非递减时钟；键序/类型/范围在此判
        # INPUT，时钟倒退由批前通用时钟判定报 INPUT。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("br", parse_metric_num(raw_op["now"]))

    if name == "ru":
        # 不可用时长查询：精确键序 op,id,now（键须按此序出现）；id 为非空
        # UTF-8 串（未知 id 留执行期判 BACKEND），now 为 0..10^9 非 bool
        # 整数，纳入共用非递减时钟。
        if list(raw_op) != ["op", "id", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("ru", parse_backend_id(raw_op["id"]),
                parse_metric_num(raw_op["now"]))

    if name == "ua":
        # 连续不可用时长告警：精确键序 op,id,reason,threshold,now（键须按
        # 此序出现）；id 为非空 UTF-8 串（未知 id 留执行期判 BACKEND），
        # reason 仅 drain/health/circuit/fault，threshold 为 1..10^9 非
        # bool 整数，now 为 0..10^9 非 bool 整数并纳入共用非递减时钟。
        # 每 (id,reason) 首评固化 threshold，此后改值留执行期判 STATE。
        if list(raw_op) != ["op", "id", "reason", "threshold", "now"]:
            fail(EXIT_INPUT, "INPUT")
        reason = raw_op["reason"]
        if reason not in ("drain", "health", "circuit", "fault"):
            fail(EXIT_INPUT, "INPUT")
        return (
            "ua", parse_backend_id(raw_op["id"]), reason,
            parse_ua_threshold(raw_op["threshold"]),
            parse_metric_num(raw_op["now"]),
        )

    if name == "fx":
        if keys != {"op", "cid", "flow", "key", "timeout", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "fx",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_key(raw_op["key"]),
            parse_fault_num(raw_op["timeout"]),
            parse_fault_num(raw_op["now"]),
        )

    if name == "hm":
        # H pick 记账查询：精确键集 op,id，只读；非法 id 在解析期判 INPUT，
        # 未知 id 在执行期判 BACKEND。
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        return ("hm", parse_backend_id(raw_op["id"]))

    if name == "fm":
        # 故障演练统计查询：精确键集 op,id，只读；非法 id 在解析期判 INPUT，
        # 未知 id 在执行期判 BACKEND。
        if keys != {"op", "id"}:
            fail(EXIT_INPUT, "INPUT")
        return ("fm", parse_backend_id(raw_op["id"]))

    if name == "fh":
        # 故障统计分钟历史：精确键集 op,id,from,to,now，只读；数值与关系
        # 约束同 mh/mx，from 过早的 STATE 留执行期判（未知 id 先 BACKEND）。
        if keys != {"op", "id", "from", "to", "now"}:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("fh", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "fa":
        # 全池故障汇总：精确键序 op,from,to,now（键须按此序出现），只读；
        # 数值与窗关系约束同 fh，from 过早的 STATE 留执行期判。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("fa", start, end, now)

    if name == "fe":
        # 全池故障阈值告警：精确键序 op,w,hi,lo,n,now（键须按此序出现）；
        # w/now ∈ [0,10^9]，hi ∈ [1,10^18]，0≤lo<hi，n ∈ [1,60]，均非
        # bool 整数；窗未结束/跳窗/变阈值的 STATE 留执行期判。
        if list(raw_op) != ["op", "w", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        hi = raw_op["hi"]
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(hi, int)
            or isinstance(hi, bool)
            or not 1 <= hi <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        lo = raw_op["lo"]
        if (
            not isinstance(lo, int)
            or isinstance(lo, bool)
            or not 0 <= lo < hi
        ):
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("fe", w, hi, lo, n, parse_metric_num(raw_op["now"]))

    if name == "ah":
        # 告警转换历史查询：精确键序 op,from,to,now（键须按此序出现），
        # 只读；数值与窗关系约束同 fa，from 过早的 STATE 留执行期判。
        if list(raw_op) != ["op", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("ah", start, end, now)

    if name == "ea":
        # 后端错误率告警：精确键序 op,id,w,hi,lo,n,now（键须按此序出现）；
        # id 沿用非空字符串校验（未知 id 留执行期判 BACKEND），w/now 为
        # 0..10^9 非 bool 整数，hi 为 1..10000，lo 为 0..9999，n 为 1..60，
        # 均非 bool 整数且 lo<hi；now 纳入共用非递减时钟（倒退执行期判
        # INPUT）。键序/类型/范围/lo<hi 在此判 INPUT；窗口越界（w 未结束或
        # 超出最近 60 窗保留下界）、跳窗与变阈值留执行期判 STATE。
        if list(raw_op) != ["op", "id", "w", "hi", "lo", "n", "now"]:
            fail(EXIT_INPUT, "INPUT")
        w = parse_metric_num(raw_op["w"])
        now = parse_metric_num(raw_op["now"])
        hi = raw_op["hi"]
        # bool 是 int 的子类，必须显式排除。
        if (
            not isinstance(hi, int)
            or isinstance(hi, bool)
            or not 1 <= hi <= 10000
        ):
            fail(EXIT_INPUT, "INPUT")
        lo = raw_op["lo"]
        if (
            not isinstance(lo, int)
            or isinstance(lo, bool)
            or not 0 <= lo <= 9999
        ):
            fail(EXIT_INPUT, "INPUT")
        if not lo < hi:
            fail(EXIT_INPUT, "INPUT")
        n = raw_op["n"]
        if (
            not isinstance(n, int)
            or isinstance(n, bool)
            or not 1 <= n <= 60
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("ea", parse_backend_id(raw_op["id"]), w, hi, lo, n, now)

    if name == "eh":
        # 后端错误率告警转换历史查询：精确键序 op,id,from,to,now（键须按
        # 此序出现），只读；id 沿用非空字符串校验（未知 id 留执行期判
        # BACKEND），from/to/now 为 0..10^9 非 bool 整数；窗关系
        # from≤to≤now//60 且 to-from<60，非法即 INPUT；now 纳入共用非
        # 递减时钟（倒退执行期判 INPUT）。from 过早与未知 id 留执行期判。
        if list(raw_op) != ["op", "id", "from", "to", "now"]:
            fail(EXIT_INPUT, "INPUT")
        start = parse_metric_num(raw_op["from"])
        end = parse_metric_num(raw_op["to"])
        now = parse_metric_num(raw_op["now"])
        # 窗关系：from≤to≤now//60 且 to-from<60，非法即 INPUT。
        if not start <= end <= now // 60 or end - start >= 60:
            fail(EXIT_INPUT, "INPUT")
        return ("eh", parse_backend_id(raw_op["id"]), start, end, now)

    if name == "fr":
        if keys != {"op", "cid", "flow", "key", "timeout", "max", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "fr",
            parse_cid(raw_op["cid"]),
            parse_flow(raw_op["flow"]),
            parse_key(raw_op["key"]),
            parse_fault_num(raw_op["timeout"]),
            parse_attempt_max(raw_op["max"]),
            parse_fault_num(raw_op["now"]),
        )

    if name in ("oi", "od"):
        # 只读接纳预演（oi）与只读接纳明细（od）：精确键序
        # op,key,c,s,bc,cc,sc,timeout,max,now（键须按此序出现）；key/c/s 与
        # 三项成本沿用 la 新键集校验（成本不全为 0），timeout/max/now 沿用
        # fr；未配环或 os、环内无合格候选的 STATE 留执行期判（先于时钟）。
        if list(raw_op) != [
            "op", "key", "c", "s", "bc", "cc", "sc", "timeout", "max", "now",
        ]:
            fail(EXIT_INPUT, "INPUT")
        bc = parse_cost(raw_op["bc"])
        cc = parse_cost(raw_op["cc"])
        sc = parse_cost(raw_op["sc"])
        if bc == 0 and cc == 0 and sc == 0:
            # 三项成本全零非法（同 la 新键集）。
            fail(EXIT_INPUT, "INPUT")
        return (
            name,
            parse_key(raw_op["key"]),
            parse_key(raw_op["c"]),
            parse_key(raw_op["s"]),
            bc,
            cc,
            sc,
            parse_fault_num(raw_op["timeout"]),
            parse_attempt_max(raw_op["max"]),
            parse_fault_num(raw_op["now"]),
        )

    if name == "fi":
        # 组合故障预演：精确键序 op,keys,timeout,max,now（键须按此序出现）；
        # keys 为 1..256 项数组，元素沿用 route 的 key 校验，可重复；未配环或
        # 无合格候选的 STATE 留执行期判（fr 同款，先于时钟）。
        if list(raw_op) != ["op", "keys", "timeout", "max", "now"]:
            fail(EXIT_INPUT, "INPUT")
        raw_keys = raw_op["keys"]
        if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= 256:
            fail(EXIT_INPUT, "INPUT")
        return (
            "fi",
            [parse_key(raw_key) for raw_key in raw_keys],
            parse_fault_num(raw_op["timeout"]),
            parse_attempt_max(raw_op["max"]),
            parse_fault_num(raw_op["now"]),
        )

    if name == "ft":
        # 故障时间线预演：精确键序 op,key,times,timeout,max,now（键须按此序
        # 出现）；key 沿用 route 校验，timeout/max/now 沿用 fr（now 纳入
        # 共用非递减时钟）；times 为 1..60 项严格递增数组，项为
        # now..10^9 非 bool 整数。未配环或无合格候选的 STATE 留执行期判
        # （fi 同款）。
        if list(raw_op) != ["op", "key", "times", "timeout", "max", "now"]:
            fail(EXIT_INPUT, "INPUT")
        now = parse_fault_num(raw_op["now"])
        raw_times = raw_op["times"]
        if not isinstance(raw_times, list) or not 1 <= len(raw_times) <= 60:
            fail(EXIT_INPUT, "INPUT")
        times = []
        for raw_at in raw_times:
            at = parse_fault_num(raw_at)
            if at < now or (times and at <= times[-1]):
                # 项须落在 now..10^9 且严格递增。
                fail(EXIT_INPUT, "INPUT")
            times.append(at)
        return (
            "ft",
            parse_key(raw_op["key"]),
            times,
            parse_fault_num(raw_op["timeout"]),
            parse_attempt_max(raw_op["max"]),
            now,
        )

    if name == "fd":
        # 故障差异预演：精确键序 op,items,keys,times,timeout,max,now（键须
        # 按此序出现）；items 为 fp 同款全量候选计划（parse_fault_plan），
        # keys 为 1..64 项数组、元素沿用 route 的 key 校验（可重复），
        # times 为 1..60 项严格递增数组、项为 now..10^9 非 bool 整数，
        # timeout/max/now 沿用 fr（now 纳入共用非递减时钟）。候选引用未知
        # id 留执行期判 BACKEND；未配环或无合格候选的 STATE 留执行期判
        # （ft 同款）。
        if list(raw_op) != [
            "op", "items", "keys", "times", "timeout", "max", "now",
        ]:
            fail(EXIT_INPUT, "INPUT")
        plan = parse_fault_plan(raw_op["items"])
        now = parse_fault_num(raw_op["now"])
        raw_keys = raw_op["keys"]
        if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= 64:
            fail(EXIT_INPUT, "INPUT")
        raw_times = raw_op["times"]
        if not isinstance(raw_times, list) or not 1 <= len(raw_times) <= 60:
            fail(EXIT_INPUT, "INPUT")
        times = []
        for raw_at in raw_times:
            at = parse_fault_num(raw_at)
            if at < now or (times and at <= times[-1]):
                # 项须落在 now..10^9 且严格递增。
                fail(EXIT_INPUT, "INPUT")
            times.append(at)
        return (
            "fd",
            plan,
            [parse_key(raw_key) for raw_key in raw_keys],
            times,
            parse_fault_num(raw_op["timeout"]),
            parse_attempt_max(raw_op["max"]),
            now,
        )

    if name == "fc":
        # 故障候选单键预演：精确键序 op,items,key,at,timeout,max,now（键须
        # 按此序出现）；items 为 fp 同款全量候选计划（parse_fault_plan），
        # key 沿用 route 校验，timeout/max/now 沿用 fr（now 纳入共用非递减
        # 时钟），at 为 0..10^9 非 bool 整数且 at>=now（at 不进时钟，模拟
        # 时刻可以晚于 now）。候选引用未知 id 留执行期判 BACKEND；未配环或
        # 无合格候选的 STATE 留执行期判（fd 同款）。
        if list(raw_op) != [
            "op", "items", "key", "at", "timeout", "max", "now",
        ]:
            fail(EXIT_INPUT, "INPUT")
        plan = parse_fault_plan(raw_op["items"])
        key = parse_key(raw_op["key"])
        at = parse_fault_num(raw_op["at"])
        timeout = parse_fault_num(raw_op["timeout"])
        max_attempts = parse_attempt_max(raw_op["max"])
        now = parse_fault_num(raw_op["now"])
        if at < now:
            # 只读模拟时刻不得早于当前时钟输入值。
            fail(EXIT_INPUT, "INPUT")
        return ("fc", plan, key, at, timeout, max_attempts, now)

    if name in ("ce", "ci"):
        if name == "ce":
            if keys != {"op"}:
                fail(EXIT_INPUT, "INPUT")
            return ("ce",)
        if keys == {"op", "config", "now"}:
            # 原三键形式：仅键集匹配（键序不限），不带 base。
            base = None
        elif list(raw_op) == ["op", "config", "base", "now"]:
            # 乐观并发形式：精确键序 op,config,base,now；base 须为小写
            # 64 位十六进制串（ct 输出的配置指纹），格式非法报 INPUT。
            base = parse_base(raw_op["base"])
        else:
            fail(EXIT_INPUT, "INPUT")
        if base is None:
            # 原三键形式：now 为非负非 bool 整数（上界不限），时钟倒退在
            # 执行期与其余操作同序判定。
            parse_now(raw_op["now"])
        else:
            # 乐观并发形式：now 仅收 0..10^9 非 bool 整数（同 cv/cd），
            # 越界、负数、bool 或类型不符一律 INPUT/2 且整批回滚。
            parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("ci", config, base, raw_op["now"])

    if name == "cv":
        # 配置预检：精确键序 op,config,now（键须按此序出现，乱序报
        # INPUT）；config 校验与规范化同 ci，now ∈ [0,10^9] 非 bool 整数，
        # 进入共用非递减时钟（倒退在执行期与其余操作同序判 INPUT）。B 限流/
        # 配额、faults 与 capacities 引用未知后端留执行期判 BACKEND；不应用
        # 配置。
        if list(raw_op) != ["op", "config", "now"]:
            fail(EXIT_INPUT, "INPUT")
        parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("cv", config, raw_op["now"])

    if name == "cd":
        # 后端配置变更预览：精确键序 op,config,now（键须按此序出现，乱序报
        # INPUT）；config 校验、规范化与错误优先级同 cv，now ∈ [0,10^9]
        # 非 bool 整数，进入共用非递减时钟（倒退在执行期与其余操作同序判
        # INPUT）。B 限流/配额、faults 与 capacities 引用未知后端留执行期判
        # BACKEND；不应用候选配置。
        if list(raw_op) != ["op", "config", "now"]:
            fail(EXIT_INPUT, "INPUT")
        parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("cd", config, raw_op["now"])

    if name == "pd":
        # 策略配置差异预览：精确键序 op,config,now（键须按此序出现，乱序报
        # INPUT）；config 校验、规范化与错误优先级同 cv，now ∈ [0,10^9]
        # 非 bool 整数，进入共用非递减时钟（倒退在执行期与其余操作同序判
        # INPUT）。B 限流/配额、faults 与 capacities 引用未知候选后端留
        # 执行期判 BACKEND；不应用候选配置。
        if list(raw_op) != ["op", "config", "now"]:
            fail(EXIT_INPUT, "INPUT")
        parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("pd", config, raw_op["now"])

    if name == "hd":
        # 哈希配置预演：精确键序 op,config,keys,now（键须按此序出现，乱序报
        # INPUT）；config 校验与规范化同 cv/cd（B 限流/配额、faults 与
        # capacities 引用未知后端留执行期判 BACKEND）；keys 为 1..256 项
        # 非空、可直接 UTF-8 编码的字符串数组（沿用 route 的 key 校验），
        # 可重复；now ∈ [0,10^9] 非
        # bool 整数并纳入共用非递减时钟（倒退在执行期与其余操作同序判
        # INPUT）。不应用候选配置；任一 vnodes 为 null 或 backends 为空的
        # STATE 留执行期判。
        if list(raw_op) != ["op", "config", "keys", "now"]:
            fail(EXIT_INPUT, "INPUT")
        raw_keys = raw_op["keys"]
        if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= 256:
            fail(EXIT_INPUT, "INPUT")
        keys = [parse_key(raw_key) for raw_key in raw_keys]
        parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("hd", config, keys, raw_op["now"])

    if name == "hb":
        # 哈希分布预演：精确键序 op,config,keys,now（键须按此序出现，乱序报
        # INPUT）；config 校验与规范化同 hd（B 限流/配额、faults 与
        # capacities 引用未知后端留执行期判 BACKEND）；keys 为 1..4096 项
        # 非空、可直接 UTF-8 编码的字符串数组（沿用 route 的 key 校验），
        # 可重复并按出现次数计数；now ∈ [0,10^9] 非 bool 整数并纳入共用
        # 非递减时钟（倒退在执行期与其余操作同序判 INPUT）。不应用候选
        # 配置；任一 vnodes 为 null 或 backends 为空的 STATE 留执行期判。
        if list(raw_op) != ["op", "config", "keys", "now"]:
            fail(EXIT_INPUT, "INPUT")
        raw_keys = raw_op["keys"]
        if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= 4096:
            fail(EXIT_INPUT, "INPUT")
        keys = [parse_key(raw_key) for raw_key in raw_keys]
        parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("hb", config, keys, raw_op["now"])

    if name == "cp":
        # 配置预约：精确键序 op,config,at,now（键须按此序出现，乱序报
        # INPUT）；config 校验、规范化与错误优先级同 cv（B 限流/配额、
        # faults 与 capacities 引用未知后端留执行期判 BACKEND）；at、now
        # 均为 0..10^9 非 bool 整数，now 进入共用非递减时钟（倒退在执行
        # 期与其余操作同序判 INPUT），at 仅表示触发时刻、不推进时钟，
        # at<cp.now 留执行期判 INPUT。成功保存 v11 快照但不应用。
        if list(raw_op) != ["op", "config", "at", "now"]:
            fail(EXIT_INPUT, "INPUT")
        at = raw_op["at"]
        if (
            not isinstance(at, int)
            or isinstance(at, bool)
            or not 0 <= at <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        parse_warm_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("cp", config, at, raw_op["now"])

    if name == "cq":
        # 预约查询：精确键序 op,now；now ∈ [0,10^9] 非 bool 整数并进入
        # 共用非递减时钟；只读快照、不受连接与队列限制，O(1)。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("cq", parse_warm_now(raw_op["now"]))

    if name == "ca":
        # 预约生效：两种严格键序形式。两键 op,now 为现有直接生效形态：
        # now ∈ [0,10^9] 非 bool 整数并进入共用非递减时钟，仅 now≥at 时按
        # 快照执行 ci 的原子替换、默认态重建并新建 rev。四键
        # op,digest,at,now 为条件生效形态：digest 为小写 64 位十六进制
        # SHA-256（格式同 cx.digest，仅用于匹配预约身份），at、now 均为
        # 0..10^9 非 bool 整数；now 同样进入共用时钟（倒退在执行期判
        # INPUT），at 仅作为预约身份、不推进时钟。无预约或身份不符、
        # now<at、有连接或排队项留执行期判 STATE；本函数只做形状与字段
        # 校验。条件形态以独立首元素 "ca_cond" 区分，时钟集合仍收 "ca"。
        if list(raw_op) == ["op", "now"]:
            return ("ca", parse_warm_now(raw_op["now"]))
        if list(raw_op) == ["op", "digest", "at", "now"]:
            digest = parse_base(raw_op["digest"])
            at = raw_op["at"]
            if (
                not isinstance(at, int)
                or isinstance(at, bool)
                or not 0 <= at <= 10 ** 9
            ):
                fail(EXIT_INPUT, "INPUT")
            now = parse_warm_now(raw_op["now"])
            return ("ca_cond", digest, at, now)
        fail(EXIT_INPUT, "INPUT")

    if name == "cx":
        # 配置预约取消：精确键序 op,digest,at,now（键须按此序出现，乱序报
        # INPUT）；digest 为小写 64 位十六进制 SHA-256（格式同 ct 摘要，
        # 仅用于条件匹配、不校验是否对应当前配置），at、now 均为 0..10^9
        # 非 bool 整数，now 进入共用非递减时钟（倒退在执行期与其余操作
        # 同序判 INPUT），at 仅用于匹配预约触发时刻、不推进时钟。预约匹配
        # 与否留执行期判 STATE；本函数只做形状与字段校验。
        if list(raw_op) != ["op", "digest", "at", "now"]:
            fail(EXIT_INPUT, "INPUT")
        digest = parse_base(raw_op["digest"])
        at = raw_op["at"]
        if (
            not isinstance(at, int)
            or isinstance(at, bool)
            or not 0 <= at <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_warm_now(raw_op["now"])
        return ("cx", digest, at, now)

    if name == "cy":
        # 配置预约条件替换：精确键序 op,base,base_at,config,at,now（键须按
        # 此序出现，乱序报 INPUT）；base 为旧预约的小写 64 位十六进制
        # SHA-256 摘要（仅格式校验，是否对得上预约留执行期判 STATE），
        # base_at 为旧触发时刻；config 校验、规范化同 cp（version=11
        # 语义；B 限流/配额、faults 与 capacities 引用候选后端集合之外的
        # 标识留执行期判 BACKEND）；at、now 均为 0..10^9 非 bool 整数，
        # now 进入共用非递减时钟（倒退在执行期判 INPUT），at 仅表示新触
        # 发时刻、不推进时钟，at<now 留执行期判 INPUT。无预约或旧值/
        # 新值均不匹配留执行期判 STATE；本函数只做形状与字段校验。
        if list(raw_op) != ["op", "base", "base_at", "config", "at", "now"]:
            fail(EXIT_INPUT, "INPUT")
        base = parse_base(raw_op["base"])
        base_at = raw_op["base_at"]
        if (
            not isinstance(base_at, int)
            or isinstance(base_at, bool)
            or not 0 <= base_at <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        config = parse_config(raw_op["config"])
        at = raw_op["at"]
        if (
            not isinstance(at, int)
            or isinstance(at, bool)
            or not 0 <= at <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        now = parse_warm_now(raw_op["now"])
        return ("cy", base, base_at, config, at, now)

    if name == "ct":
        # 配置指纹查询：精确键序仅 op，只读且不推进时钟。
        if list(raw_op) != ["op"]:
            fail(EXIT_INPUT, "INPUT")
        return ("ct",)

    if name == "cl":
        # 配置提交历史查询：精确键集仅 op，只读。
        if keys != {"op"}:
            fail(EXIT_INPUT, "INPUT")
        return ("cl",)

    if name == "al":
        # 配置变更审计查询：精确键序仅 op，只读、不推进时钟。
        if list(raw_op) != ["op"]:
            fail(EXIT_INPUT, "INPUT")
        return ("al",)

    if name == "ai":
        # 审计游标查询：精确键序 op,after,limit（键须按此序出现），只读、
        # 不推进时钟。after 为 0..10^18、limit 为 1..64 的非 bool 整数；
        # 键序、类型或范围非法报 INPUT，after 大于已分配最大 rev 留执行期
        # 判 STATE。
        if list(raw_op) != ["op", "after", "limit"]:
            fail(EXIT_INPUT, "INPUT")
        after = raw_op["after"]
        if (
            not isinstance(after, int)
            or isinstance(after, bool)
            or not 0 <= after <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        limit = raw_op["limit"]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 64
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("ai", after, limit)

    if name == "ad":
        # 审计段级差异查询：精确键序 op,rev（键须按此序出现），只读、不推进
        # 时钟。rev 为 1..10^18 的非 bool 整数；键序、类型或范围非法报
        # INPUT，rev 尚未分配、超过最新修订或其事件已随 64 条窗口淘汰留执行
        # 期判 STATE。
        if list(raw_op) != ["op", "rev"]:
            fail(EXIT_INPUT, "INPUT")
        rev = raw_op["rev"]
        if (
            not isinstance(rev, int)
            or isinstance(rev, bool)
            or not 1 <= rev <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("ad", rev)

    if name == "ag":
        # 审计区间汇总查询：精确键序 op,after,until（键须按此序出现），
        # 只读、不推进时钟。after/until 均为 0..10^18 的非 bool 整数且
        # after<=until（左开右闭区间）；键序、字段集合、整数类型、范围或
        # 区间关系非法报 INPUT，先于修订状态判断。until 超过最新修订留执
        # 行期判 STATE。
        if list(raw_op) != ["op", "after", "until"]:
            fail(EXIT_INPUT, "INPUT")
        after = raw_op["after"]
        if (
            not isinstance(after, int)
            or isinstance(after, bool)
            or not 0 <= after <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        until = raw_op["until"]
        if (
            not isinstance(until, int)
            or isinstance(until, bool)
            or not 0 <= until <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        if after > until:
            fail(EXIT_INPUT, "INPUT")
        return ("ag", after, until)

    if name == "cb":
        # 配置回滚：两种形式。原三键 op,rev,now 仅按键集识别（键序不限，
        # 与既有行为一致），rev 为 1..10^18 非 bool 整数（是否仍被保留留
        # 执行期判 STATE），now 沿用 ci 并进入共用非递减时钟（倒退在执行
        # 期与其余操作同序判 INPUT）。四键乐观形态 op,rev,base,now 为严格
        # 键序：base 为操作开始前当前规范化 version=11 配置的 ct 摘要（小
        # 写 64 位十六进制串），作为回滚前提；now 收 0..10^9 非 bool 整数
        # （同其他乐观形态），进入共用时钟。目标 rev 是否仍在 16 条提交
        # 窗口、base 是否匹配留执行期判 STATE。四键以独立首元素
        # "cb_cond" 区分，时钟集合仍收 "cb"。
        if keys == {"op", "rev", "now"}:
            rev = raw_op["rev"]
            if (
                not isinstance(rev, int)
                or isinstance(rev, bool)
                or not 1 <= rev <= 10 ** 18
            ):
                fail(EXIT_INPUT, "INPUT")
            return ("cb", rev, parse_now(raw_op["now"]))
        if list(raw_op) == ["op", "rev", "base", "now"]:
            rev = raw_op["rev"]
            if (
                not isinstance(rev, int)
                or isinstance(rev, bool)
                or not 1 <= rev <= 10 ** 18
            ):
                fail(EXIT_INPUT, "INPUT")
            base = parse_base(raw_op["base"])
            now = parse_warm_now(raw_op["now"])
            return ("cb_cond", rev, base, now)
        fail(EXIT_INPUT, "INPUT")

    if name == "cu":
        # 单字段配置热加载：精确键序 op,base,section,value,now（键须按此序
        # 出现，乱序报 INPUT）。base 格式同 ct.digest（小写 64 位十六进制
        # 串）；section 须为 version=11 的 ce.config 除 version 外任一顶层
        # 键；value 沿用该字段既有结构、键序、类型、范围与排序，执行期以
        # 当前配置为底稿仅替换该段后按 ci 整体校验（含交叉约束）；now 为
        # 0..10^9 非 bool 整数并进入共用非递减时钟。
        if list(raw_op) != ["op", "base", "section", "value", "now"]:
            fail(EXIT_INPUT, "INPUT")
        base = parse_base(raw_op["base"])
        section = raw_op["section"]
        if not isinstance(section, str) or section not in CU_SECTIONS:
            fail(EXIT_INPUT, "INPUT")
        now = parse_warm_now(raw_op["now"])
        return ("cu", base, section, raw_op["value"], now)

    if name == "ts":
        if keys != {"op", "ttl"}:
            fail(EXIT_INPUT, "INPUT")
        return ("ts", parse_idle_ttl(raw_op["ttl"]))

    if name == "tm":
        # 连接硬时限：精确键序 op,ttl（键须按此序出现）；ttl 为
        # 1..10^9 非 bool 整数，同值幂等、异值覆盖，留执行期处理。
        if list(raw_op) != ["op", "ttl"]:
            fail(EXIT_INPUT, "INPUT")
        return ("tm", parse_idle_ttl(raw_op["ttl"]))

    if name == "te":
        # 连接时限检查：精确键序 op,cid,now（键须按此序出现）；now 为
        # 0..10^9 非 bool 整数并进入共用非递减时钟；未配 ts/tm 与未知
        # cid 留执行期分别判 STATE/CONNECTION。
        if list(raw_op) != ["op", "cid", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("te", parse_cid(raw_op["cid"]), parse_warm_now(raw_op["now"]))

    if name in ("tk", "tg"):
        if keys != {"op", "cid", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return (name, parse_cid(raw_op["cid"]), parse_now(raw_op["now"]))

    if name == "tx":
        if keys != {"op", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("tx", parse_now(raw_op["now"]))

    if name == "ep":
        # 后端 IP 端点登记：精确键集 op,id,host,port；未知 id 留执行期判
        # BACKEND。
        if keys != {"op", "id", "host", "port"}:
            fail(EXIT_INPUT, "INPUT")
        return (
            "ep",
            parse_backend_id(raw_op["id"]),
            parse_endpoint_host(raw_op["host"]),
            parse_endpoint_port(raw_op["port"]),
        )

    if name == "fw":
        # 连接转发快照查询：精确键集 op,cid，无 now；未知 cid 与无快照
        # 均留执行期判定。
        if keys != {"op", "cid"}:
            fail(EXIT_INPUT, "INPUT")
        return ("fw", parse_cid(raw_op["cid"]))

    if name == "eq":
        # 端点轮换存量查询：精确键序 op,id,now（键须按此序出现）；id 为
        # 非空 UTF-8 串（未知 id 留执行期判 BACKEND），now 为 0..10^9 非
        # bool 整数并进入共用非递减时钟；未配置端点留执行期判 STATE。
        if list(raw_op) != ["op", "id", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("eq", parse_backend_id(raw_op["id"]),
                parse_metric_num(raw_op["now"]))

    if name == "ec":
        # 端点轮换存量清理：精确键序 op,id,before,now（键须按此序出现）；
        # before/now 均为 0..10^9 非 bool 整数且 before<=now，now 进入共用
        # 非递减时钟；未知 id 与未配置端点留执行期分别判 BACKEND/STATE。
        if list(raw_op) != ["op", "id", "before", "now"]:
            fail(EXIT_INPUT, "INPUT")
        before = parse_metric_num(raw_op["before"])
        now = parse_metric_num(raw_op["now"])
        if before > now:
            fail(EXIT_INPUT, "INPUT")
        return ("ec", parse_backend_id(raw_op["id"]), before, now)

    if name == "er":
        # 全池端点轮换盘点：精确键序 op,now（键须按此序出现）；now 为
        # 0..10^9 非 bool 整数并进入共用非递减时钟；除推进时钟外只读。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        return ("er", parse_metric_num(raw_op["now"]))

    if name == "ex":
        # 全池端点轮换批量清理：精确键序 op,before,now（键须按此序出现）；
        # before/now 均为 0..10^9 非 bool 整数且 before<=now，now 进入共用
        # 非递减时钟。
        if list(raw_op) != ["op", "before", "now"]:
            fail(EXIT_INPUT, "INPUT")
        before = parse_metric_num(raw_op["before"])
        now = parse_metric_num(raw_op["now"])
        if before > now:
            fail(EXIT_INPUT, "INPUT")
        return ("ex", before, now)

    if name == "ey":
        # 后端端点原子条件变更：精确键序 op,id,base,target,now（键须按此
        # 序出现，乱序报 INPUT）；base/target 均为 null 或精确键序
        # host,port 的对象，null 表示未登记端点，对象沿用 ep 的规范化 IP
        # 字面量与 1..65535 非 bool 端口校验；now 为 0..10^9 非 bool 整
        # 数并进入共用非递减时钟（倒退在执行期判 INPUT）。未知 id 与当
        # 前端点不匹配留执行期分别判 BACKEND/STATE；本函数只做形状、字
        # 段与时钟域校验。
        if list(raw_op) != ["op", "id", "base", "target", "now"]:
            fail(EXIT_INPUT, "INPUT")
        backend_id = parse_backend_id(raw_op["id"])
        base = parse_endpoint_or_none(raw_op["base"])
        target = parse_endpoint_or_none(raw_op["target"])
        now = parse_metric_num(raw_op["now"])
        return ("ey", backend_id, base, target, now)

    if name == "eb":
        # 批量后端端点原子条件变更：精确键序 op,items,now（键须按此序出
        # 现，乱序报 INPUT）；items 为 1..1000 项数组（bool 不是数组），
        # 每项精确键序 id,base,target（键须按此序出现），同一 id 在数组
        # 中不得重复；base/target 均沿用 parse_endpoint_or_none（null 或
        # 精确键序 host,port 的对象，校验同 ey/ep）；now 为 0..10^9 非
        # bool 整数并进入共用非递减时钟（倒退在执行期判 INPUT）。未知识
        # 别与条件不匹配留执行期分别判 BACKEND/STATE；本函数只做形状、
        # 字段、数量、重复 id 与时钟域校验，且整份输入先于任何状态检查。
        if list(raw_op) != ["op", "items", "now"]:
            fail(EXIT_INPUT, "INPUT")
        raw_items = raw_op["items"]
        if (
            not isinstance(raw_items, list)
            or isinstance(raw_items, bool)
            or not 1 <= len(raw_items) <= 1000
        ):
            fail(EXIT_INPUT, "INPUT")
        items = []
        seen_ids = set()
        for raw_item in raw_items:
            if (
                not isinstance(raw_item, dict)
                or list(raw_item) != ["id", "base", "target"]
            ):
                fail(EXIT_INPUT, "INPUT")
            item_id = parse_backend_id(raw_item["id"])
            if item_id in seen_ids:
                fail(EXIT_INPUT, "INPUT")
            seen_ids.add(item_id)
            base = parse_endpoint_or_none(raw_item["base"])
            target = parse_endpoint_or_none(raw_item["target"])
            items.append((item_id, base, target))
        now = parse_metric_num(raw_op["now"])
        return ("eb", items, now)

    if name == "ez":
        # 批量后端端点切换只读预演：精确键序 op,items,now（键须按此序出
        # 现，乱序报 INPUT）；items 为 1..1000 项数组（bool 不是数组），
        # 每项精确键序 id,base,target（键须按此序出现），同一 id 在数组
        # 中不得重复；base/target 均沿用 parse_endpoint_or_none（null 或
        # 精确键序 host,port 的对象，校验同 ey/eb/ep）；now 为 0..10^9
        # 非 bool 整数并进入共用非递减时钟（倒退在执行期判 INPUT）。未
        # 知识别留执行期判 BACKEND；本函数只做形状、字段、数量、重复 id
        # 与时钟域校验，且整份输入先于任何状态检查。除推进时钟外只读。
        if list(raw_op) != ["op", "items", "now"]:
            fail(EXIT_INPUT, "INPUT")
        raw_items = raw_op["items"]
        if (
            not isinstance(raw_items, list)
            or isinstance(raw_items, bool)
            or not 1 <= len(raw_items) <= 1000
        ):
            fail(EXIT_INPUT, "INPUT")
        items = []
        seen_ids = set()
        for raw_item in raw_items:
            if (
                not isinstance(raw_item, dict)
                or list(raw_item) != ["id", "base", "target"]
            ):
                fail(EXIT_INPUT, "INPUT")
            item_id = parse_backend_id(raw_item["id"])
            if item_id in seen_ids:
                fail(EXIT_INPUT, "INPUT")
            seen_ids.add(item_id)
            base = parse_endpoint_or_none(raw_item["base"])
            target = parse_endpoint_or_none(raw_item["target"])
            items.append((item_id, base, target))
        now = parse_metric_num(raw_op["now"])
        return ("ez", items, now)

    if name == "ej":
        # 批量端点条件切换并清理旧连接：精确键序 op,items,before,now（键
        # 须按此序出现，乱序报 INPUT）；items 为 1..1000 项数组（bool 不
        # 是数组），每项精确键序 id,base,target（键须按此序出现），同一
        # id 在数组中不得重复；base/target 均沿用
        # parse_endpoint_or_none（null 或精确键序 host,port 的对象，校验
        # 同 ey/eb/ep）；before、now 均为 0..10^9 非 bool 整数且
        # before<=now，now 进入共用非递减时钟（倒退在执行期判 INPUT）。
        # 未知 id 与条件不匹配留执行期分别判 BACKEND/STATE；本函数只做
        # 形状、字段、数量、重复 id、关系与时钟域校验，且整份输入先于任
        # 何状态检查。
        if list(raw_op) != ["op", "items", "before", "now"]:
            fail(EXIT_INPUT, "INPUT")
        raw_items = raw_op["items"]
        if (
            not isinstance(raw_items, list)
            or isinstance(raw_items, bool)
            or not 1 <= len(raw_items) <= 1000
        ):
            fail(EXIT_INPUT, "INPUT")
        items = []
        seen_ids = set()
        for raw_item in raw_items:
            if (
                not isinstance(raw_item, dict)
                or list(raw_item) != ["id", "base", "target"]
            ):
                fail(EXIT_INPUT, "INPUT")
            item_id = parse_backend_id(raw_item["id"])
            if item_id in seen_ids:
                fail(EXIT_INPUT, "INPUT")
            seen_ids.add(item_id)
            base = parse_endpoint_or_none(raw_item["base"])
            target = parse_endpoint_or_none(raw_item["target"])
            items.append((item_id, base, target))
        before = parse_metric_num(raw_op["before"])
        now = parse_metric_num(raw_op["now"])
        if before > now:
            fail(EXIT_INPUT, "INPUT")
        return ("ej", items, before, now)

    if name == "es":
        # 全池端点切换预约建立：精确键序 op,items,before,at,now（键须按此
        # 序出现，乱序报 INPUT）；items 沿用 ej 的数量（1..1000）、顺序、
        # 唯一 id、base/target（null 或精确键序 host,port 对象）规则；
        # before、at、now 均为 0..10^9 非 bool 整数，now 进入共用非递减
        # 时钟（倒退在执行期判 INPUT），at、before 不推进时钟，关系
        # now<=at、before<=at 在此一并判 INPUT（at<now 先于后端引用
        # BACKEND 检查，同全局优先级）。未知后端留执行期判 BACKEND；本函
        # 数只做形状、字段、数量、重复 id、关系与时钟域校验。
        if list(raw_op) != ["op", "items", "before", "at", "now"]:
            fail(EXIT_INPUT, "INPUT")
        items = parse_endpoint_switch_items(raw_op["items"])
        before = parse_metric_num(raw_op["before"])
        at = parse_metric_num(raw_op["at"])
        now = parse_metric_num(raw_op["now"])
        if now > at or before > at:
            fail(EXIT_INPUT, "INPUT")
        return ("es", items, before, at, now)

    if name == "en":
        # 全池端点切换预约查询：精确键序仅 op；不推进时钟、只读，O(N) 输
        # 出预约快照。
        if list(raw_op) != ["op"]:
            fail(EXIT_INPUT, "INPUT")
        return ("en",)

    if name in ("eu", "ei"):
        # 全池端点切换预约取消（eu）/条件生效（ei）：均严格按键序
        # op,digest,at,now 排列（乱序报 INPUT）；digest 为小写 64 位十六
        # 进制 SHA-256（仅用于匹配预约身份，不要求对应当前状态），at、
        # now 均为 0..10^9 非 bool 整数，now 进入共用非递减时钟（倒退在
        # 执行期判 INPUT），at 仅作预约身份、不推进时钟。无预约、身份不
        # 符、过早（ei 的 now<at）、端点冲突或生效时后端已删除留执行期
        # 分别判 STATE/BACKEND；本函数只做形状与字段校验。
        if list(raw_op) != ["op", "digest", "at", "now"]:
            fail(EXIT_INPUT, "INPUT")
        digest = cp_hex_digest(raw_op["digest"])
        at = parse_metric_num(raw_op["at"])
        now = parse_metric_num(raw_op["now"])
        return (name, digest, at, now)

    if name == "ed":
        # 全池端点切换预约轮询到期处理：严格按键序 op,now 排列（乱序报
        # INPUT）；轮询者不持预约身份，仅提交 now，原子检查并处理当前预
        # 约，避免查询与执行之间预约被替换。now 为 0..10^9 非 bool 整数，
        # 进入共用非递减时钟（倒退在执行期判 INPUT）。无预约 EMPTY、未到
        # 期 WAITING、到期但后端已删除 MISSING、端点冲突 CONFLICT 均为成
        # 功结果而非错误，留执行期判定；本函数只做形状、字段类型/范围
        # 校验。
        if list(raw_op) != ["op", "now"]:
            fail(EXIT_INPUT, "INPUT")
        now = parse_metric_num(raw_op["now"])
        return ("ed", now)

    if name == "eo":
        # 全池端点切换预约条件替换（并发安全的改期/换内容）：精确键序
        # op,base,base_at,items,before,at,now（键须按此序出现，乱序报
        # INPUT）；base 为 en 读到的旧预约小写 64 位十六进制 SHA-256 摘要
        # （仅格式校验，是否对得上预约留执行期判 STATE），base_at 为旧触
        # 发时刻；items、before 沿用 es 的数量（1..1000）、顺序、唯一 id、
        # base/target（null 或精确键序 host,port 对象）规则；at、now 均为
        # 0..10^9 非 bool 整数，now 进入共用非递减时钟（倒退在执行期判
        # INPUT），at、before 不推进时钟，关系 now<=at、before<=at 在此
        # 一并判 INPUT（先于候选后端引用 BACKEND 检查，同 es 与全局优先
        # 级）。未知后端留执行期判 BACKEND；无预约或旧值/新值均不匹配留
        # 执行期判 STATE。本函数只做形状、字段、数量、重复 id、关系与时
        # 钟域校验。
        if list(raw_op) != [
            "op", "base", "base_at", "items", "before", "at", "now",
        ]:
            fail(EXIT_INPUT, "INPUT")
        base = cp_hex_digest(raw_op["base"])
        base_at = parse_metric_num(raw_op["base_at"])
        items = parse_endpoint_switch_items(raw_op["items"])
        before = parse_metric_num(raw_op["before"])
        at = parse_metric_num(raw_op["at"])
        now = parse_metric_num(raw_op["now"])
        if now > at or before > at:
            fail(EXIT_INPUT, "INPUT")
        return ("eo", base, base_at, items, before, at, now)

    if name == "ev":
        # 全池端点切换预约只读预演：严格按键序 op,digest,at,now 排列（乱
        # 序报 INPUT）；digest 为小写 64 位十六进制 SHA-256（仅用于匹配预
        # 约身份），at、now 均为 0..10^9 非 bool 整数，now 进入共用非递
        # 减时钟（倒退在执行期判 INPUT），at 仅作预约身份、不推进时钟，
        # 也不要求 now 与 at 的大小关系（未到期 EARLY 是预演结果而非错
        # 误）。无预约或身份任一不符留执行期判 STATE；本函数只做形状、
        # 摘要格式与字段类型/范围校验。
        if list(raw_op) != ["op", "digest", "at", "now"]:
            fail(EXIT_INPUT, "INPUT")
        digest = cp_hex_digest(raw_op["digest"])
        at = parse_metric_num(raw_op["at"])
        now = parse_metric_num(raw_op["now"])
        return ("ev", digest, at, now)

    if name == "ew":
        # 端点切换预约候选替换只读预演（提交 eo 前的并发前提与影响预
        # 估）：精确键序 op,base,base_at,items,before,at,now（键须按此序
        # 出现，乱序报 INPUT）。base 为 en 读到的旧预约小写 64 位十六进制
        # SHA-256 摘要（仅格式校验，是否匹配留执行期归类，不报错）；
        # base_at 为旧触发时刻；items、before、at、now 沿用 eo/es 的数量
        # （1..1000）、顺序、唯一 id、base/target（null 或精确键序 host,
        # port 对象）规则，均为 0..10^9 非 bool 整数，关系 now<=at、
        # before<=at 在此一并判 INPUT（先于候选后端引用 BACKEND 检查，同
        # eo 与全局优先级）。now 进入共用非递减时钟（倒退在执行期判
        # INPUT），at、before 不推进时钟。候选后端是否现存留执行期判
        # BACKEND；预约身份（无预约/已等于候选/等于旧身份/其余）不报错，
        # 作为成功预演的 status 返回。本函数只做形状、字段、数量、重复
        # id、关系与时钟域校验。
        if list(raw_op) != [
            "op", "base", "base_at", "items", "before", "at", "now",
        ]:
            fail(EXIT_INPUT, "INPUT")
        base = cp_hex_digest(raw_op["base"])
        base_at = parse_metric_num(raw_op["base_at"])
        items = parse_endpoint_switch_items(raw_op["items"])
        before = parse_metric_num(raw_op["before"])
        at = parse_metric_num(raw_op["at"])
        now = parse_metric_num(raw_op["now"])
        if now > at or before > at:
            fail(EXIT_INPUT, "INPUT")
        return ("ew", base, base_at, items, before, at, now)

    if name == "ek":
        # 端点切换预约生命周期审计游标查询：精确键序 op,after,limit（键须按
        # 此序出现，乱序报 INPUT），只读、不推进时钟、不改预约与审计窗口。
        # after 为 0..10^18、limit 为 1..64 的非 bool 整数；键序、类型、
        # 布尔混入或范围非法报 INPUT，after 大于已分配最大 seq 留执行期判
        # STATE（同 ai 口径）。
        if list(raw_op) != ["op", "after", "limit"]:
            fail(EXIT_INPUT, "INPUT")
        after = raw_op["after"]
        if (
            not isinstance(after, int)
            or isinstance(after, bool)
            or not 0 <= after <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        limit = raw_op["limit"]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 64
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("ek", after, limit)

    if name == "el":
        # 端点切换预约生命周期审计的只读筛选查询：精确键序
        # op,after,limit,actions,since,until（键须按此序出现，乱序报
        # INPUT），只读、不推进时钟、不改事件生成/淘汰窗口/下一 seq。
        # after 为 0..10^18、limit 为 1..64 的非 bool 整数（同 ek）；
        # actions 为 1..5 项数组，元素限 SET/REPLACE/CANCEL/APPLY/CLEAR，
        # 不重复且须按该固定枚举次序排列（故仅判断相邻项严格递增即可同时
        # 拒绝重复与乱序）；since/until 为 0..10^9 的非 bool 整数且
        # since<=until。字段集合、键序、编码、类型、范围、枚举、重复、次
        # 序、空数组或时间关系非法统一判 INPUT；after 大于已分配最大 seq
        # 留执行期判 STATE（INPUT 判定先于游标状态，同 ek/ai 口径）。
        if list(raw_op) != [
            "op", "after", "limit", "actions", "since", "until",
        ]:
            fail(EXIT_INPUT, "INPUT")
        after = raw_op["after"]
        if (
            not isinstance(after, int)
            or isinstance(after, bool)
            or not 0 <= after <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        limit = raw_op["limit"]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 64
        ):
            fail(EXIT_INPUT, "INPUT")
        actions = raw_op["actions"]
        if not isinstance(actions, list) or not 1 <= len(actions) <= 5:
            fail(EXIT_INPUT, "INPUT")
        action_order = {
            "SET": 0, "REPLACE": 1, "CANCEL": 2, "APPLY": 3, "CLEAR": 4,
        }
        previous_rank = -1
        for action in actions:
            if not isinstance(action, str) or action not in action_order:
                fail(EXIT_INPUT, "INPUT")
            rank = action_order[action]
            # 元素须严格按 SET、REPLACE、CANCEL、APPLY、CLEAR 次序排列；
            # 重复（同秩）与乱序（秩倒退）一并拒绝。
            if rank <= previous_rank:
                fail(EXIT_INPUT, "INPUT")
            previous_rank = rank
        since = raw_op["since"]
        if (
            not isinstance(since, int)
            or isinstance(since, bool)
            or not 0 <= since <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        until = raw_op["until"]
        if (
            not isinstance(until, int)
            or isinstance(until, bool)
            or not 0 <= until <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        if since > until:
            fail(EXIT_INPUT, "INPUT")
        return ("el", after, limit, tuple(actions), since, until)

    if name == "em":
        # 端点切换预约身份追查（只读）：精确键序
        # op,digest,at,side,after,limit（键须按此序出现，乱序报 INPUT）。
        # digest 为 64 位小写十六进制 SHA-256；at 为 0..10^9 的非 bool 整
        # 数；side 仅 BEFORE/AFTER/EITHER，分别匹配变更前身份、变更后身份
        # 或任一侧（两侧同时命中只计一次）；after 为 0..10^18、limit 为
        # 1..64 的非 bool 整数。字段集合、键序、编码、摘要格式、side 枚
        # 举、整数类型或范围非法统一判 INPUT；after 大于已分配最大 seq 留
        # 执行期判 STATE（INPUT 判定先于游标状态，同 ek/el 口径）。
        if list(raw_op) != [
            "op", "digest", "at", "side", "after", "limit",
        ]:
            fail(EXIT_INPUT, "INPUT")
        digest = cp_hex_digest(raw_op["digest"])
        at = raw_op["at"]
        if (
            not isinstance(at, int)
            or isinstance(at, bool)
            or not 0 <= at <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        side = raw_op["side"]
        if side not in ("BEFORE", "AFTER", "EITHER"):
            fail(EXIT_INPUT, "INPUT")
        after = raw_op["after"]
        if (
            not isinstance(after, int)
            or isinstance(after, bool)
            or not 0 <= after <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        limit = raw_op["limit"]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 64
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("em", digest, at, side, after, limit)

    if name == "eg":
        # 端点切换预约身份区间汇总（只读）：精确键序
        # op,digest,at,after,until（键须按此序出现，乱序报 INPUT）。
        # digest、at 沿用 em 校验（digest 为 64 位小写十六进制 SHA-256；
        # at 为 0..10^9 的非 bool 整数），未知身份按无匹配处理。after、
        # until 为 seq 左开右闭区间端点：0..10^18 的非 bool 整数且
        # after<=until。字段集合、键序、编码、摘要格式、整数类型/范围或
        # 区间关系非法统一判 INPUT，且先于游标状态判断；until 大于已分配
        # 最大 seq 留执行期判 STATE。
        if list(raw_op) != [
            "op", "digest", "at", "after", "until",
        ]:
            fail(EXIT_INPUT, "INPUT")
        digest = cp_hex_digest(raw_op["digest"])
        at = raw_op["at"]
        if (
            not isinstance(at, int)
            or isinstance(at, bool)
            or not 0 <= at <= 10 ** 9
        ):
            fail(EXIT_INPUT, "INPUT")
        after = raw_op["after"]
        if (
            not isinstance(after, int)
            or isinstance(after, bool)
            or not 0 <= after <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        until = raw_op["until"]
        if (
            not isinstance(until, int)
            or isinstance(until, bool)
            or not 0 <= until <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        if after > until:
            fail(EXIT_INPUT, "INPUT")
        return ("eg", digest, at, after, until)

    if name == "ee":
        # 端点切换预约审计序号快照批量查询（只读）：精确键序 op,seqs（键须
        # 按此序出现，乱序报 INPUT）。seqs 为 1..64 项数组，每项为
        # 0..10^18 的非 bool 整数且严格递增、不重复。不接受 now：不推进显
        # 式时钟。字段集合、键序、容器、数量、整数类型、范围或递增关系非
        # 法统一判 INPUT；任一 seq 大于已分配最大 seq 留执行期判 STATE
        # （INPUT 判定先于状态）。
        if list(raw_op) != ["op", "seqs"]:
            fail(EXIT_INPUT, "INPUT")
        seqs = raw_op["seqs"]
        if not isinstance(seqs, list) or not 1 <= len(seqs) <= 64:
            fail(EXIT_INPUT, "INPUT")
        previous_seq = -1
        normalized_seqs = []
        for seq in seqs:
            if (
                not isinstance(seq, int)
                or isinstance(seq, bool)
                or not 0 <= seq <= 10 ** 18
            ):
                fail(EXIT_INPUT, "INPUT")
            # 严格递增即同时排除重复（相等）与乱序（倒退）。
            if seq <= previous_seq:
                fail(EXIT_INPUT, "INPUT")
            previous_seq = seq
            normalized_seqs.append(seq)
        return ("ee", tuple(normalized_seqs))

    if name == "ef":
        # 端点切换预约审计序号区间状态区段压缩（只读）：精确键序
        # op,first,last（键须按此序出现，乱序报 INPUT）。first、last 为
        # 0..10^18 的非 bool 整数且 first<=last，共同给出闭区间
        # [first,last]。不接收 now：不推进显式时钟。字段集合、键序、整数
        # 类型、范围或区间关系非法统一判 INPUT；last 大于已分配最大 seq
        # 留执行期判 STATE（INPUT 判定先于状态，口径同 ee）。
        if list(raw_op) != ["op", "first", "last"]:
            fail(EXIT_INPUT, "INPUT")
        first = raw_op["first"]
        if (
            not isinstance(first, int)
            or isinstance(first, bool)
            or not 0 <= first <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        last = raw_op["last"]
        if (
            not isinstance(last, int)
            or isinstance(last, bool)
            or not 0 <= last <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        if first > last:
            fail(EXIT_INPUT, "INPUT")
        return ("ef", first, last)

    if name == "et":
        # 端点切换预约审计业务时间快照批量查询（只读）：精确键序 op,times
        # （键须按此序出现，乱序报 INPUT）。times 为 1..64 项数组，每项为
        # 0..10^9 的非 bool 整数且严格递增、不重复。不接受 now：不推进显
        # 式时钟。字段集合、键序、容器、数量、整数类型、范围或递增关系非
        # 法统一判 INPUT；任一 time 晚于操作开始时的全局逻辑时钟留执行期
        # 判 STATE（INPUT 判定先于状态，口径同 ee/ef）。
        if list(raw_op) != ["op", "times"]:
            fail(EXIT_INPUT, "INPUT")
        times = raw_op["times"]
        if not isinstance(times, list) or not 1 <= len(times) <= 64:
            fail(EXIT_INPUT, "INPUT")
        previous_time = -1
        normalized_times = []
        for time_value in times:
            if (
                not isinstance(time_value, int)
                or isinstance(time_value, bool)
                or not 0 <= time_value <= 10 ** 9
            ):
                fail(EXIT_INPUT, "INPUT")
            # 严格递增即同时排除重复（相等）与乱序（倒退）。
            if time_value <= previous_time:
                fail(EXIT_INPUT, "INPUT")
            previous_time = time_value
            normalized_times.append(time_value)
        return ("et", tuple(normalized_times))

    if name == "se":
        # 运行态检查点导出：精确键序仅 op，不推进时钟、不改状态；执行期
        # 规范化全部状态并按 O(N) 计算摘要，state 紧凑编码超 8MiB 报
        # OVERLOAD/7。
        if list(raw_op) != ["op"]:
            fail(EXIT_INPUT, "INPUT")
        return ("se",)

    if name == "si":
        # 运行态检查点导入：两种严格键序形式。四键
        # op,version,digest,state 为无条件原子替换；五键
        # op,base,version,digest,state 为带基线摘要的乐观并发形式，base
        # 为替换前提（sd 的 before 或此前 se 的 digest）。两形式 version 仅
        # 收 1；digest/base 均为小写 64 位十六进制；state 的结构、类型、
        # 范围、UTF-8 编码与未知版本留执行期判 INPUT，悬空引用、重复标
        # 识、矛盾计数或非法状态组合判 STATE；摘要不符先于 state 结构校验
        # 判 INPUT。紧凑编码超 8MiB 报 OVERLOAD/7。
        if list(raw_op) == ["op", "version", "digest", "state"]:
            base = None
        elif list(raw_op) == ["op", "base", "version", "digest", "state"]:
            base = cp_hex_digest(raw_op["base"])
        else:
            fail(EXIT_INPUT, "INPUT")
        version = raw_op["version"]
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != 1
        ):
            fail(EXIT_INPUT, "INPUT")
        digest = cp_hex_digest(raw_op["digest"])
        return ("si", base, version, digest, raw_op["state"])

    if name == "sd":
        # 候选检查点只读差异：精确键序 op,version,digest,state，后三项沿用
        # si 的公开含义与解析期校验（version=1；digest 小写 64 位十六进
        # 制）。state 的结构/类型/范围/UTF-8/未知版本与摘要匹配留执行期，
        # 完全沿用 si 的规则、错误优先级与 INPUT/2、STATE/4、OVERLOAD/7
        # 分类。
        if list(raw_op) != ["op", "version", "digest", "state"]:
            fail(EXIT_INPUT, "INPUT")
        version = raw_op["version"]
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != 1
        ):
            fail(EXIT_INPUT, "INPUT")
        digest = raw_op["digest"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("sd", version, digest, raw_op["state"])

    if name == "sx":
        # 三方检查点只读预演：精确键序 op,base,target。base/target 各为按
        # version,digest,state 排列且不含 op 的检查点对象。两对象的形状、
        # version/digest 格式、state 全部校验（含错误优先级）留执行期，按
        # base、target、current 的顺序逐一进行，以免解析期先校验 target
        # 而越过 base 的更早错误；外层 op 形状错误先于一切。
        if list(raw_op) != ["op", "base", "target"]:
            fail(EXIT_INPUT, "INPUT")
        return ("sx", raw_op["base"], raw_op["target"])

    if name == "sm":
        # 检查点三方合并：精确键序 op,base,target。base/target 各为按
        # version,digest,state 排列且不含 op 的检查点对象。两对象的形状、
        # version/digest 格式、state 全部校验（含错误优先级）留执行期，按
        # base、target、current 的顺序逐一进行；外层 op 形状错误先于一切。
        # 不接受 now，也不推进显式时钟。
        if list(raw_op) != ["op", "base", "target"]:
            fail(EXIT_INPUT, "INPUT")
        return ("sm", raw_op["base"], raw_op["target"])

    # get
    if keys != {"op", "cid"}:
        fail(EXIT_INPUT, "INPUT")
    return ("get", parse_cid(raw_op["cid"]))


CHECKPOINT_VERSION = 1
# state 紧凑 UTF-8 编码上限：8 MiB。
CHECKPOINT_LIMIT = 8 * 1024 * 1024


def cp_nonneg(value):
    """检查点字段：非负非 bool 整数。"""
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def cp_time(value):
    """检查点时刻字段：0..10^9 非 bool 整数（显式时钟与各 now 同域）。"""
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 10 ** 9
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def cp_bool(value):
    if not isinstance(value, bool):
        fail(EXIT_INPUT, "INPUT")
    return value


def cp_list(value):
    if not isinstance(value, list):
        fail(EXIT_INPUT, "INPUT")
    return value


def cp_state_int():
    """非法状态组合（悬空引用、重复标识、矛盾计数、非法组合）。"""
    fail(EXIT_STATE, "STATE")


def cp_hex_digest(value):
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        fail(EXIT_INPUT, "INPUT")
    return value


def effective_weight(record, now):
    """返回 now 时刻的有效权重（百分制整数）；unhealthy 固定 0。

    warm 段 [start, end) 内自 warm_from 线性过渡到 target*100，插值向下
    取整到 0.01；end 起取目标。新增与恢复的 warm_from 为 100（1.00），
    调权时 warm_from 为调权当时的有效权重（百分制）。
    """
    if not record["healthy"]:
        return 0
    # pick 不携带 now，取最近时钟值；此时只可能存在 steady 后端
    # （任何 warm 后端都来自携带 now 的 add/ws/恢复 probe）。
    if now is None or record["stage"] != "warm" or now >= record["warm_end"]:
        return record["weight"] * 100
    target = record["weight"] * 100
    span = record["warm_end"] - record["warm_start"]
    elapsed = now - record["warm_start"]
    return record["warm_from"] + (target - record["warm_from"]) * elapsed // span


def run(raw):
    """在全新状态执行一批操作，返回 stdout 字节；错误经 fail 抛出。"""
    try:
        text = raw.decode("utf-8")
        data = decode_json(text)
    except (UnicodeDecodeError, ValueError):
        fail(EXIT_INPUT, "INPUT")

    if not isinstance(data, dict) or set(data) != {"ops"}:
        fail(EXIT_INPUT, "INPUT")
    ops = data["ops"]
    if not isinstance(ops, list):
        fail(EXIT_INPUT, "INPUT")

    # dict 保序即加入顺序；每个后端记录：
    # weight 目标权重（整数），current 平滑加权累加值（百分制），conns
    # 活动连接数，healthy 健康状态，fail/success 迁移阈值，failures/
    # successes 当前连续计数，probe_now/probe_ok 最近一次生效的 probe
    # （用于幂等重报判定）。预热：stage ∈ warm/steady，warm_from 为段内
    # 起始有效权重（百分制，新增/恢复为 100，调权为当时有效值），
    # warm_start/warm_end 为预热区间，warm_d 为登记的 d（恢复时复用）。
    # last_op 记录最近一次写操作的形状用于同参/冲突重报判定。circuit 为
    # 熔断器状态（未配为 None，remove 后重加即回到未配）：params 为
    # (n, m, r, w, q)，state ∈ C/O/H，window 为 C 态最近 n 次报告的
    # deque，next 为 O 态恢复时刻，used 为 H 态已报告数，cr_now/cr_ok
    # 为最近一次生效的 cr（用于幂等重报判定）。drain 为优雅摘除状态：
    # t 为 ds 登记的排空时限（未 ds 为 None，remove 后重加即回到未配），
    # state ∈ A/D/X（可用/排空/已摘除），start/end/deadline 为本次摘除
    # 的三个时刻（未开始为 None），forced 为 dg 强关的连接数。
    backends = {}
    # 活动连接：cid -> [backend_id, flow, opened_at, last]；关闭即删除，cid
    # 可复用。last 为最近活动时刻（建连时置 opened_at，tk 未到期时置 now），
    # 供 ts/tk/tg/tx 的空闲超时判定；dict 保序即建连顺序。
    connections = {}
    # 连接转发快照：cid -> (host, port)，仅在成功建连（open/oa/ot/fx/fr）
    # 当时后端已 ep 登记 endpoint 时写入；删除连接（close/dg/tx）同步删除。
    # fw 只读快照，不受后续 ep 变更影响；键集恒为 connections 键集的子集。
    # 额外空间 O(C)。
    conn_endpoints = {}
    # 一致性哈希：ring_vnodes 未 chash 时为 None；粘性映射 key -> [backend_id,
    # expires]，只在目标不可用（三键还包括到期）时依环改写（不迁回），增删
    # 后端或改 vnodes 均不动它。expires 为 None 表示无到期（未 ss 或二键
    # route 建立/沿用），否则为三键 route 写入的 now+sticky_ttl 绝对时刻。
    ring_vnodes = None
    sticky_map = {}
    # 限时粘性：sticky_ttl 未 ss 时为 None，否则为登记时限；异值重配报 STATE，
    # 登记值随 ce/ci 导出导入；三键 route 与 ss 后的 la/oa/ot 依赖它。
    sticky_ttl = None
    # 令牌桶以 (scope, id) 唯一：scope ∈ B/C/S（后端/客户端/服务类）。
    # B 桶 id 必须是现存后端；remove 即删。每桶 r/b 为速率与容量，t/at
    # 为当前令牌与最近补充时刻，last 为最近一次 ls 的 (r,b,now) 用于重报。
    buckets = {}
    # 固定窗口配额以 (scope, id) 唯一：scope ∈ B/C/S。B 配额的 id 须为现存
    # 后端；remove 即删，ci/cb 成功清空。每项 limit/span 为窗口上限与跨度，
    # window/used 为当前窗口序号（now//span）与窗内已用量，last 为最近一次
    # qs 的 (limit,span,now)，用于同参重报幂等判定。
    quotas = {}
    # 排队接纳：queue_cfg 未 os 时为 None，否则为 (cap, q, ttl)；wait_queue
    # 为 FIFO 有序映射 cid -> (cid, flow, c, s, key, bc, cc, sc, enqueue_now)，
    # 容量上限 q；bc/cc/sc 为入队请求的三项令牌成本。OrderedDict 即哈希表加
    # 双向链表：键集同时是排队成员索引，入队/接纳/过期/取消/ci 单点维护即同步，
    # oa 查重与 oc 任意位置删除均 O(1) 且其余项 FIFO 相对次序不变。
    queue_cfg = None
    wait_queue = OrderedDict()
    # 每后端接纳容量覆盖（pc/pg 与配置 capacities）：backend_id -> cap，
    # 仅在显式配置时存在；oa/ot 接纳与 oi/od/oq 投影判定 capacity 时，已配
    # 覆盖用其 cap，否则用 os.cap。pc 同值幂等、异值覆盖（即使活动连接数
    # 超过新值也不关连接，连接释放后自然恢复）；remove 删除覆盖且同 id 重加
    # 不继承。ci/cb 按配置 capacities 原子替换：v10 显式覆盖按后端加入序
    # 恢复，旧版（v1..v9）或 [] 清空；ce 经末置 capacities 按后端加入序
    # 导出当前覆盖，提交快照仅存 ci/cb 当时值，其后 pc 只改当前 ce。dict
    # 查找/写入 O(1)，额外空间 O(B)。
    cap_overrides = {}
    # 过载分钟历史（oh）：window=now//60 -> [immediate, queued, dequeued,
    # expired, peak]，全池一份（不按后端分）。oa 返回 A/Q 在其 now 窗记
    # immediate/queued，ot 的接纳/过期项在其 now 窗记 dequeued/expired，
    # peak 为该窗入队后队长峰值；计数封顶 10^18，只保留最近 60 窗，空窗
    # 不预建。ci/cb 成功清空；oh 只读。
    overload_hist = {}
    # 排队等待历史（wh）：window=离队时刻//60 -> {"admitted": [0]*5,
    # "expired": [0]*5, "cancelled": [0]*5, "evicted": [0]*5}，全池一份。
    # 等待项成功离队时按 d=离队时刻-入队 now 落入 ≤0、≤1、≤10、≤100、
    # >100 五桶：ot 接纳/过期按 ot.now，oc 取消按当前逻辑时钟，H 模式 oa
    # 头淘汰按被淘汰项的 oa.now；立即接纳、失败批次不记。计数封顶
    # 10^18，只保留最近 60 窗，空窗不预建；ci/cb/ca 成功清空。wh 除时钟
    # 外只读。额外空间 O(60)。
    wait_hist = {}
    # 排队等待分位告警（wa）：以 kind ∈ A/E/C/V（对应
    # admitted/expired/cancelled/evicted）为键，各 kind 独立维护、未首评为
    # 缺键，否则为 {"p","hi","lo","n","state","run","w","result"}——
    # (p,hi,lo,n) 为首评固化的参数（p 1..100、hi 1..4、lo 0..3、lo<hi、
    # n 1..60），state ∈ N/A，run 为当前连续计数，w 为最近已评窗，result
    # 为该窗结果（同窗同参原样返回，不推进状态机）。窗值取 wait_hist 中
    # 该 kind 在 [w,w] 单窗五桶按 wp 规则计算的 samples/bucket/upper（无
    # 样本 0/null/null）。全池一份（不按后端分）；ci/cb/ca 成功整体清空。
    # dict 查找/写入 O(1)，额外空间 O(1)（至多 4 个 kind）。
    wait_alerts = {}
    # 多维限流历史（lh）：以 (scope, id) 唯一（scope ∈ B/C/S），window=
    # now//60 -> [admitted, units, token, quota]。oa/ot 检查在配桶或配额
    # 的维时记账：接纳 admitted+1、units+该维成本；未接纳且令牌不足记
    # token，配额不足记 quota（可同增），仅容量阻塞不记；计数封顶
    # 10^18，只保留最近 60 窗，空窗不预建。ls/qs 重配不清；remove 删除
    # 该 id（B），同 id 重加不继承，ci/cb 成功整体清空。lh 除时钟外只读。
    # 额外空间 O(60K)。
    limit_hist = {}
    # 确定性滞回背压：bp_cfg 未 bp 时为 None，否则为 (low, high)；bp_state
    # 为 N/P。首配或异参重配按当前队长 >=high 置 P，否则 N；同参幂等不改
    # 状态。low < high <= queue_cfg[1]（os.q）；登记值随 ce/ci 导出导入，
    # ci 携带时置 N、未携带时取消。
    bp_cfg = None
    bp_state = "N"
    # 队列出队策略（qp）：queue_mode 为 ot 当前生效策略，F 为遇首个阻塞项
    # 即停（默认），S 为 ot 将其余各项按 FIFO 各检查一次、阻塞项移至队尾
    # 并保持相对次序，P 为老化优先（先按 score 降序、再按 FIFO 各尝试一
    # 次）。dequeue_policy 为最近登记的 F/S：P 不持久化——进入 P 时保留
    # 最近 F/S，P 时 ce/cl 的 queue.dequeue 仍导出该值；qp 切 F/S 会更新
    # 该值并退出 P；ci/cb/ca 成功采用所载 F/S。同值幂等；ci/cb 按
    # queue.dequeue 载入（v1..v8 默认 F）；登记值随 ce/ci 经 v9+
    # queue.dequeue 导出导入；rp/qp 提交后的后续修改仅影响当前 ce，不改
    # 提交。
    queue_mode = "F"
    dequeue_policy = "F"
    # 老化优先级登记（qa）：aging_cfg 未 qa 时为 None，否则为
    # (step, {s: p})——step 为老化步长，dict 为各服务类基础优先级 p，未
    # 登记服务类按 p=0。qa 原子替换整份登记；qp 选 P 时无登记报 STATE。
    # 登记不持久化：不随 ce/cl 导出，ci/cb/ca 成功即清空。额外空间 O(P)，
    # P 式 ot 查 p 为 O(1)。
    aging_cfg = None
    # FIFO 满载策略（rp）：T 为队满尾拒绝（默认），H 为队满头淘汰——删除
    # 队首并把新项放队尾。evict_count 为累计淘汰数（封顶 10^18），
    # evict_last 为最近淘汰的 cid（无则 None）；旧项不耗或返还令牌、配额，
    # 入队路由已产生的粘性映射保留，淘汰 cid 可立即复用。同值幂等，
    # ci/cb 按 queue.full 载入（v1..v8 默认 T）并清零 0、None；登记值随
    # ce/ci 经 v9 queue.full 导出导入（evicted、last 不导出）。
    full_mode = "T"
    evict_count = 0
    evict_last = None
    # 连接空闲超时：ttl_cfg 未 ts 时为 None，否则为登记的全局空闲时限；
    # 异值重配报 STATE，登记值随 ce/ci 导出导入（ci 后作用于新连接）。
    ttl_cfg = None
    # 连接硬时限：hard_ttl_cfg 未 tm 且当前配置 lifetime=null 时为 None，
    # 否则为登记的全局硬时限；同值幂等、异值覆盖（覆盖立即作用于既有与
    # 后续连接，硬截止恒为 opened_at+当前登记值，tk 不延长）。v11 起随
    # ce/ci 经末置 lifetime 导出导入：ci/cb/ca 成功按快照载入，null 清除；
    # 载入后的 tm 后续修改只影响当前 ce/ct，不改已有提交与预约快照。
    hard_ttl_cfg = None
    # pick 调度策略：W 为既有平滑加权（默认），R 为轮询，L 为最少连接；
    # 登记值随 ce/ci 导出导入（v1/v2 等价于 W，v3 仅 W/R，v4 收 W/R/L）。
    # rr_ticket 为 R 模式的轮询游标，仅 ci 成功重建为 0，add/remove 或
    # 状态迁移均不重置；L 无游标。
    pick_mode = "W"
    rr_ticket = 0
    last_now = None
    # 全池增量快照（mo）游标：mo_seq 为下一次成功 mo 应有的 seq（首项
    # 必须为 1，成功后递增；ci/cb 成功重置为 1）；mo_cache 为最近一次
    # 成功 mo 的 (seq, now, 结果对象)，同 seq、now 重报原样返回且不推进
    # 时钟、不重存基线。每后端的 mo_base 为上次成功 mo 时该后端当前窗
    # mr 累计快照（首元素为基线窗，None 为初始），随 add/make_record
    # 初始化为零（remove 后重加即从零），仅在与本次同窗时作为基线，故
    # 无需额外的全局窗游标。
    mo_seq = 1
    mo_cache = None
    # 全池故障阈值告警（fe）：未首评为 None，否则为 {"hi","lo","n",
    # "state","run","w","result"}——(hi,lo,n) 为首评固化的阈值，state ∈
    # N/A，run 为当前连续计数，w 为最近已评窗，result 为该窗结果（同窗
    # 重报原样返回，不推进状态机）。ci 成功即清除。
    alert = None
    # 告警转换历史（ah）：fe 每次状态转换追加一个事件，键序
    # window,from,to,v,hi,lo,n（from/to 为转换前后状态，仅 N/A；v 为该次
    # 评估值，hi/lo/n 为固化阈值）；评估后删除早于 w-59 的事件，w 严格
    # 递增故队列按 window 升序且至多 60 项，追加与前端裁剪均摊还 O(1)。
    # 同窗重报不推进状态机，自然不重复追加；remove 及同 id 重加不影响
    # （仅改变后续 fe 的 v），ci 成功清空。
    alert_events = deque()
    # 后端错误率告警（ea）：以后端 id 为键，未首评为缺键，否则为
    # {"hi","lo","n","state","run","w","result"}——(hi,lo,n) 为首评固化的
    # 阈值（hi 1..10000、lo 0..9999、lo<hi、n 1..60），state ∈ N/A，run 为
    # 当前连续计数，w 为最近已评窗，result 为该窗结果（同窗同阈值原样返回，
    # 不推进状态机）。每后端独立；remove 删除其告警，同 id 重加回到未首评，
    # ci/cb 成功整体清空。dict 查找/写入 O(1)，额外空间 O(B)。
    err_alerts = {}
    # ea 告警转换历史：以后端 id 为键的 deque，仅在该后端首次发生 N/A 转换
    # 时惰性创建；每个事件键序 window,from,to,rate,hi,lo,n（window 为触发
    # 窗，from/to 仅 N/A，rate 沿用该次 ea 的两位定点串，hi/lo/n 为固化
    # 阈值）。同窗重报与未转换不追加；每次 ea 成功后删除 window<w-59 的事
    # 件，w 严格递增故每后端至多 60 项、队列按 window 升序，追加与前端裁
    # 剪均摊还 O(1)。remove 及同 id 重加删除其历史，ci/cb 成功整体清空；
    # eh 只读，不推进告警也不清理历史。额外空间 O(60B)。
    err_events = {}
    # 后端延迟分位告警（pa）：以后端 id 为键，未首评为缺键，否则为
    # {"p","hi","lo","n","state","run","w","result"}——(p,hi,lo,n) 为首评
    # 固化的参数（p 1..100、hi 1..4、lo 0..3、lo<hi、n 1..60），state ∈
    # N/A，run 为当前连续计数，w 为最近已评窗，result 为该窗结果（同窗同
    # 参原样返回，不推进状态机）。窗值取该后端 w 窗五 latency 桶按 lp 规则
    # 计算的 samples/bucket/upper（无样本 0/null/null）。每后端独立；
    # remove 删除其告警，同 id 重加回到未首评，ci/cb 成功整体清空。dict
    # 查找/写入 O(1)，额外空间 O(B)。
    percent_alerts = {}
    # pa 告警转换历史：以后端 id 为键的 deque，仅在该后端首次发生 N/A 转换
    # 时惰性创建；每个事件键序 window,from,to,p,samples,bucket,upper,hi,lo,n
    # （window 为触发评估窗，from/to 仅 N/A，p 与 bucket/upper 沿用该次 pa
    # 的评估值，samples/hi/lo/n 为整数）。同窗重报与未转换不追加；每次推进
    # 状态机的 pa 成功后删除 window<w-59 的事件，w 严格递增故每后端至多 60
    # 项、队列按 window 升序，追加与前端裁剪均摊还 O(1)。remove 及同 id 重
    # 加删除其历史，ci/cb 成功整体清空；ph 只读，不推进告警也不清理历史。
    # 额外空间 O(60B)。
    percent_events = {}
    # 每后端重试/重映射告警（xa）：以 (id, k) 为键（k ∈ R/M，分别取该后端
    # w 窗 mh 口径的 retries/remaps 累计值），各 (id,k) 独立、未首评为缺键，
    # 否则为 {"hi","lo","n","state","run","w","result"}——(hi,lo,n) 为首评
    # 固化的阈值（hi 1..10^18、lo 0..(10^18-1)、lo<hi、n 1..60），state ∈
    # N/A，run 为当前连续计数，w 为最近已评窗，result 为该窗结果（同窗同
    # 参原样返回，不推进状态机）。remove 删除该后端全部键，同 id 重加回到
    # 未首评，ci/cb/ca 成功整体清空。dict 查找/写入 O(1)，每后端至多两键，
    # 额外空间 O(B)。
    retry_alerts = {}
    # xa 告警转换历史：以 (id,k) 为键的 deque，仅在该键首次发生 N/A 转换时
    # 惰性创建；每个事件键序 window,from,to,value,hi,lo,n（window 为触发
    # 评估窗，from/to 仅 N/A，value 沿用该次 xa 的评估值，hi/lo/n 为固化
    # 阈值，五个数值字段皆为整数）。同窗重报与未转换不追加；每次推进状态
    # 机的 xa 成功后删除 window<w-59 的事件，w 严格递增故每键至多 60 项、
    # 队列按 window 升序，追加与前端裁剪均摊还 O(1)。remove 及同 id 重加
    # 删除该后端 R/M 两键历史，ci/cb/ca 成功整体清空；xh 只读，不推进告
    # 警也不清理历史。额外空间 O(60B)。
    retry_events = {}
    # 每后端并发告警（na）：以后端 id 为键，各 id 独立、未首评为缺键，否则
    # 为 {"hi","lo","n","state","run","w","result"}——(hi,lo,n) 为首评固化
    # 的阈值（hi 1..10^9、lo 0..(10^9-1)、lo<hi、n 1..60），state ∈ N/A，
    # run 为当前连续计数，w 为最近已评窗，result 为该窗结果（同窗同参原样
    # 返回，不推进状态机）。窗值取该后端 w 窗 mx 口径的 samples/peak（空窗
    # 均为 0）。remove 删除该后端键，同 id 重加回到未首评，ci/cb/ca/cu 成
    # 功整体清空。dict 查找/写入 O(1)，每后端至多一键，额外空间 O(B)。
    conc_alerts = {}
    # 限流告警（le）：以 (scope, id) 唯一（scope ∈ B/C/S），各标识独立、
    # 未首评为缺键，否则为 {"hi","lo","n","state","run","w","result"}——
    # (hi,lo,n) 为首评固化的阈值（hi 1..10^18、lo 0..(10^18-1)、lo<hi、
    # n 1..60），state ∈ N/A，run 为当前连续计数，w 为最近已评窗，result
    # 为该窗结果（同窗同参原样返回，不推进状态机）。窗值取该标识 lh 的
    # w 窗 token/quota（空窗均为 0），value 为二者之和并封顶 10^18。
    # ls/qs 重配保留；remove 删除该后端的 B 键（同 id 重加回到未首评），
    # ci/cb/ca/cu 成功整体清空。dict 查找/写入 O(1)，额外空间 O(K)，K 为
    # 标识数。
    limit_alerts = {}
    # 连续不可用时长告警（ua）：以 (id, reason) 唯一（reason ∈ drain/health/
    # circuit/fault），各组合独立、未首评为缺键，否则为
    # {"threshold","state","now","result"}——threshold 为首评固化阈值
    # （1..10^9），state ∈ N/A 为上次成功评估后的告警态，now 为上次成功评
    # 估时刻（同 id,reason,threshold,now 重报原样返回首评结果、不推进状
    # 态），result 为该次结果。active/since/duration 只读 ru 同刻事实口径，
    # 不随存储固化；active 且 duration>=threshold 为 A，首次以前态 N 起评
    # （首次已超阈值即 changed=true）。remove 删除该 id 全部原因键，同 id
    # 重加回到未首评，ci/cb/ca/cu 成功整体清空。dict 查找/写入 O(1)，每后
    # 端至多四键，额外空间 O(B)。
    unavail_alerts = {}
    # 配置提交历史（cl/cb）：(rev, 规范化 version=11 配置快照) 按 rev 升序，
    # 仅保留最近 16 条；rev 由 next_rev 从 1 起递增分配，只增不复用。ci/cb
    # 成功才分配并追加，失败不分配、不改历史；初始无提交。快照为
    # export_config 产出的全新结构（含 faults 登记时间线与 queue 策略），
    # 不随后续运行态变化（提交后的 qp/rp 修改仅影响当前 ce）。额外时空
    # O(16(B+M+T+Q))。
    commit_history = []
    next_rev = 1
    # 配置预约（cp/cq/ca/cx/cy）：无预约为 None，否则为
    # (snapshot, at, digest)——snapshot 为 cp 当时规范化 version=11 配置的
    # 全新导出结构（不随后续运行态变化），at 为触发时刻（只表示时刻、不推进
    # 时钟），digest 为快照的 ct 摘要。cp 成功即整体替换，cq 只读 O(1)，
    # ca 成功（含四键条件形态 ca_cond：digest、at 同时匹配才继续）、
    # ci/cb/cu 成功均清除；cx 在 digest、at 同时匹配时原子清除、
    # 无预约时幂等空操作、有预约但任一不匹配报 STATE 且保留预约；cy 按
    # 旧值/新值条件替换；其余操作不影响预约。额外空间 O(N)，N 为规范化
    # 配置大小。
    reservation = None
    # 全池唯一端点切换预约（es/en/eu/ei）：无预约为 None，否则为
    # (items, before, at, digest)——items 为 es 当时规范化的
    # (id, base, target) 元组列表（保持请求顺序、id 互不重复；端点 None
    # 表示未登记），before 为旧连接关闭的 opened 上界，at 为最早生效时
    # 刻（只表示时刻、不推进时钟），digest 为固定键序对象
    # {"items":...,"before":...} 紧凑 UTF-8 JSON 的小写 SHA-256。es 成功
    # 即整体替换（同身份重报幂等、不重写），en 只读 O(N)，eu 在 digest、
    # at 同时匹配时原子清除（无预约幂等空操作、不匹配报 STATE 且保留），
    # ei 仅在身份匹配且 now>=at 时按操作开始快照执行 ej 同式切换与清理、
    # 成功后删除；ci/cb/cu/ca 成功一并清除；其余操作不影响预约。额外空间
    # O(N)，N 为预约项数。
    ep_switch = None
    # 配置变更审计（al）：deque(maxlen=64) 按 rev 升序保留最近 64 条，追加
    # O(1) 且超额自动淘汰最旧项；初始为空，独立于 commit_history 的 16 条
    # 提交，成功配置变更（ci/cb/cu/ca 分配新 rev 时）不清空旧事件。每事件
    # 为全新 dict，精确键序 rev,now,kind,section,before,after：rev 为本次
    # 新 rev，now 为该操作显式时钟，kind ∈ ci/cb/cu/ca，section 仅 cu 取被
    # 替换顶层字段、其余 None，before/after 为操作前后规范化 version=11 配
    # 置的 ct 摘要（值未变两摘要相同仍记录）。额外空间 O(A)，A≤64。
    audit_events = deque(maxlen=64)
    # 段级审计（ad）：与 audit_events 平行、同 maxlen=64、同序追加与淘汰，
    # 每项与同下标审计事件一一对应，为
    # (section, before_fingerprint, after_fingerprint) 三元组的元组，仅列
    # 该次变更前后值不同的顶层段（按 AD_SECTIONS 序），只存段指纹、不复制
    # 整份配置；值未变的成功事件对应空元组。ci/cb/cu/ca 成功分配新 rev 的
    # 同一事务内与事件一起固化，失败不追加。额外空间 O(64S)，S 为规范化
    # 顶层段数。
    audit_sections = deque(maxlen=64)
    # 端点切换预约生命周期审计（ek）：独立、不复用的 seq 自 1 递增（ep_audit
    # seq 为下一个待分配 seq），事件按 seq 升序以 deque(maxlen=64) 仅保留最
    # 近 64 条，超额淘汰最旧项；ci/cb/cu/ca 清除预约不清空审计窗口。每事件
    # 为 (seq, now, action, before, after)：now 为造成变更的显式时钟，
    # action ∈ SET/REPLACE/CANCEL/APPLY/CLEAR，before/after 为 None 或
    # (digest, at) 预约身份。仅真正改变预约状态的成功操作追加：es 从空建立
    # 记 SET、以不同身份覆盖记 REPLACE（同身份重报不记）；eo 旧身份匹配的
    # 首次替换记 REPLACE（已等于新身份的幂等重报不记）；eu 成功取消记
    # CANCEL（无预约空取消不记）；ei 成功生效与 ed APPLIED 记 APPLY（ed 的
    # EMPTY/WAITING/MISSING/CONFLICT 不记）；ci/cb/cu/ca 成功清除现存预约记
    # CLEAR（本无预约不记）。追加 O(1)，额外空间 O(64)。
    ep_audit_events = deque(maxlen=64)
    ep_audit_seq = 1
    results = []

    def backend_routable(record, drain_strict=False):
        """粘性沿用条件：现存、healthy、熔断 C 且未摘除到 X（D 态原粘性仍
        命中，故只排除 X；新映射建环时另要求 A 态）。drain_strict=True 时
        （H 模式 pick）排空 D 同样失格，仅 A 态可沿用。"""
        if record is None or not record["healthy"] or not circuit_closed(record):
            return False
        if drain_strict:
            return record["drain"]["state"] == "A"
        return record["drain"]["state"] != "X"

    def select_route(key, now=None, fatal=True, drain_strict=False):
        """按 route 语义选后端，返回
        (backend_id, sticky, remapped, expired, expires)。

        now=None 为旧二键语义：映射项 [b,e] 的 e 不参与判定，b 可用即沿用
        且 e 不变；无项或重选写 e=None。now 为整数时限时语义（三键 route 与
        ss 后的 la/oa/ot）：须已 ss，e=None 时仅判 b 可用性（沿用 b 但补写
        e=now+ttl），e 非 None 时仅 now<e 且 b 可用才沿用、e 不变，否则重选；
        无项、e=None 或重选均写 e=now+sticky_ttl。expired
        为原 e 非 None 且 now>=原 e（重选回同一 b 仍为 True）。未配环或无
        可选后端报 STATE；fatal=False 时不退出而返回 None（供排队重试把该
        情形视为阻塞）。drain_strict=True（H 模式 pick）时粘性保留另要求 A
        态：D/X 均按失格依环迁移；建环本就只含 A 态。"""
        if ring_vnodes is None or (now is not None and sticky_ttl is None):
            # 三键（now 非 None）还要求已 ss。
            if fatal:
                fail(EXIT_STATE, "STATE")
            return None
        entry = sticky_map.get(key)
        old_b = entry[0] if entry is not None else None
        old_expires = entry[1] if entry is not None else None
        keep = False
        expired = False
        if entry is not None:
            if now is None or old_expires is None:
                # 二键语义不判到期（e 原样保留）；三键遇 e=null 也只判 b
                # 可用性，但沿用时仍要写 e=now+ttl（下方处理）。
                keep = backend_routable(
                    backends.get(old_b), drain_strict=drain_strict
                )
            else:
                # 三键且 e 非 null：到期标志先于可用性判定。
                expired = now >= old_expires
                keep = not expired and backend_routable(
                    backends.get(old_b), drain_strict=drain_strict
                )
        if keep:
            if now is not None and old_expires is None:
                # 三键沿用 e=null 项的可用 b：b 不变，但补写 e=now+ttl。
                new_expires = now + sticky_ttl
                sticky_map[key] = [old_b, new_expires]
                return old_b, True, False, False, new_expires
            # 其余沿用一律 e 不变。
            return old_b, True, False, False, old_expires
        tokens = build_ring(backends, ring_vnodes)
        if not tokens:
            if fatal:
                fail(EXIT_STATE, "STATE")
            return None
        digests = [token[0] for token in tokens]
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0  # 越界回绕到环首
        chosen_id = tokens[index][3]
        remapped = entry is not None and chosen_id != old_b
        new_expires = None if now is None else now + sticky_ttl
        sticky_map[key] = [chosen_id, new_expires]
        return chosen_id, False, remapped, expired, new_expires

    def project_route(key, now=None, drain_strict=False, ring_tokens=None,
                      ring_digests=None):
        """select_route 的只读投影（供 oq）：按完全相同的三键/二键粘性与
        环规则计算此刻将路由到的后端，但绝不新建或改写粘性映射；未配环、
        三键未 ss 或环内无合格后端返回 None。排空沿用口径由 drain_strict
        决定（oq 传 True：D 态粘性目标同样失格并沿环迁移到环上 A 态候选，
        环上无候选即 R）。ring_tokens/ring_digests 为调用方预先构建并全项
        共用的当前环与令牌摘要升序列表（oq 各项 O(log(BV)) 二分），为
        None 时按需自建。"""
        if ring_vnodes is None or (now is not None and sticky_ttl is None):
            return None
        entry = sticky_map.get(key)
        old_b = entry[0] if entry is not None else None
        old_expires = entry[1] if entry is not None else None
        if entry is not None:
            if now is None or old_expires is None:
                keep = backend_routable(
                    backends.get(old_b), drain_strict=drain_strict
                )
            else:
                # 三键且 e 非 null：到期即失格（与 select_route 同口径，但
                # 只读投影不改写映射）。
                keep = now < old_expires and backend_routable(
                    backends.get(old_b), drain_strict=drain_strict
                )
            if keep:
                return old_b
        if ring_tokens is None:
            ring_tokens = build_ring(backends, ring_vnodes)
        if not ring_tokens:
            return None
        if ring_digests is None:
            ring_digests = [token[0] for token in ring_tokens]
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(ring_digests, key_hash)
        if index == len(ring_tokens):
            index = 0  # 越界回绕到环首
        return ring_tokens[index][3]

    def refill(bucket, now):
        # 先按时间差补充至容量上限，再推进时钟。
        bucket["t"] = min(
            bucket["b"], bucket["t"] + (now - bucket["at"]) * bucket["r"]
        )
        bucket["at"] = now

    def roll_quota(quota, now):
        # 固定窗口随时钟推进：跨窗（now//span 越过登记窗口）才置
        # window=now//span 并清 used；时钟非递减，窗口只前进不后退。
        window = now // quota["span"]
        if window != quota["window"]:
            quota["window"] = window
            quota["used"] = 0

    def record_limit_admit(scope, bucket_id, cost, now):
        """多维限流历史的接纳记账（O(1)）：window=now//60 窗 admitted 加
        1、units 加该维成本，封顶 10^18。仅对在配桶或配额的维调用（维
        存在才有历史行）。"""
        history = limit_hist.get((scope, bucket_id))
        if history is None:
            history = {}
            limit_hist[(scope, bucket_id)] = history
        window = now // 60
        row = history.get(window)
        if row is None:
            row = [0, 0, 0, 0]
            history[window] = row
            # 时钟非递减，新建窗时顺带丢弃 60 窗前的旧窗。
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        row[0] = min(METRIC_CAP, row[0] + 1)
        row[1] = min(METRIC_CAP, row[1] + cost)

    def record_limit_block(scope, bucket_id, cost, token_short, quota_short,
                           now):
        """多维限流历史的未接纳记账（O(1)）：令牌不足 token 加 1、配额不
        足 quota 加 1（可同增）；仅容量阻塞（两者皆 False）不调用。行的
        units/admitted 保持不动。"""
        if not token_short and not quota_short:
            return
        history = limit_hist.get((scope, bucket_id))
        if history is None:
            history = {}
            limit_hist[(scope, bucket_id)] = history
        window = now // 60
        row = history.get(window)
        if row is None:
            row = [0, 0, 0, 0]
            history[window] = row
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        if token_short:
            row[2] = min(METRIC_CAP, row[2] + 1)
        if quota_short:
            row[3] = min(METRIC_CAP, row[3] + 1)

    def establish_connection(cid, backend_id, flow, now):
        """成功建连（open/oa/ot/fx/fr 共用）：后端并发加一、登记连接
        （opened_at=now）；后端当时已 ep 登记 endpoint 时快照其
        (host, port)，未配置仍建连（无快照，fw 报 STATE）。"""
        record = backends[backend_id]
        record["conns"] += 1
        connections[cid] = [backend_id, flow, now, now]
        endpoint = record["endpoint"]
        if endpoint is not None:
            conn_endpoints[cid] = endpoint

    def conn_deadlines(connection):
        """返回连接的 (idle, hard, deadline)：idle 为 last+ts.ttl（未 ts 为
        None），hard 为 opened_at+硬时限（硬时限经 tm 或 ci/cb/ca 的 v11
        lifetime 载入，二者均无为 None），deadline 为非
        None 二者最小值（均未配为 None）。硬截止只认 opened_at 与当前登记
        值，tk 刷新 last 不影响它。均 O(1)。"""
        idle = None if ttl_cfg is None else connection[3] + ttl_cfg
        hard = (
            None if hard_ttl_cfg is None else connection[2] + hard_ttl_cfg
        )
        candidates = [value for value in (idle, hard) if value is not None]
        deadline = min(candidates) if candidates else None
        return idle, hard, deadline

    def effective_cap(backend_id):
        """该后端当前接纳容量上限：已配 pc 覆盖用其 cap，否则用 os.cap。
        os 必已配置（仅 oa/ot/oi/od/oq 在 queue_cfg 非空路径调用）。O(1)。"""
        override = cap_overrides.get(backend_id)
        return queue_cfg[0] if override is None else override

    def evaluate_admit(backend_id, cid, flow, c, s, costs, now):
        """对已路由的后端按 la 规则补充检查但不消费：先补充在配桶、推进在配
        固定窗（均不回写扣减），令牌不足、配额 used+成本>limit、目标非 A 或
        连接数达 cap 时返回 ("block", id)，全部满足才按成本原子耗令牌、配额
        used 加成本并建连接（opened_at=now），返回 ("admit", id)。costs 为
        (bc, cc, sc)。配额不足只导致排队阻塞，从不由 oa/ot 报 RATE。

        多维限流历史（lh）：仅对在配桶或配额的维记账——接纳则该窗该维
        admitted 加 1、units 加该维成本；未接纳且该维令牌不足记 token、
        配额不足记 quota（可同增），仅容量阻塞不记。接纳记账在全部扣减
        与建连之后（其间无 fail 路径），失败批次由调用方整体回滚。"""
        chosen = []
        chosen_quotas = []
        # 各维 (scope, id, cost)，与在配桶/配额列表平行，用于记账。
        dimensions = (
            ("B", backend_id, costs[0]),
            ("C", c, costs[1]),
            ("S", s, costs[2]),
        )
        tracked = []
        for scope, bucket_id, cost in dimensions:
            has_bucket = (scope, bucket_id) in buckets
            has_quota = (scope, bucket_id) in quotas
            if has_bucket or has_quota:
                tracked.append((scope, bucket_id, cost, has_bucket, has_quota))
            bucket = buckets.get((scope, bucket_id))
            if bucket is not None:
                chosen.append((bucket, cost))
            quota = quotas.get((scope, bucket_id))
            if quota is not None:
                chosen_quotas.append((quota, cost))
        # 先补充再判定：检查阶段只推进补充时刻与窗口，不扣令牌、不增 used；
        # 阻塞时这些推进随批次成功保留（与 la 同口径），失败批整体回滚。
        for bucket, _ in chosen:
            refill(bucket, now)
        for quota, _ in chosen_quotas:
            roll_quota(quota, now)
        record = backends[backend_id]
        # 各维分别判定令牌/配额不足：多维限流历史按维记账，故逐维记录
        # 其补充/推进后的不足事实，与整体是否接纳无关。
        token_short = {}
        quota_short = {}
        for scope, bucket_id, cost, has_bucket, has_quota in tracked:
            if has_bucket:
                token_short[(scope, bucket_id)] = (
                    buckets[(scope, bucket_id)]["t"] < cost
                )
            if has_quota:
                quota = quotas[(scope, bucket_id)]
                quota_short[(scope, bucket_id)] = (
                    quota["used"] + cost > quota["limit"]
                )
        if (
            not all(bucket["t"] >= cost for bucket, cost in chosen)
            or not all(
                quota["used"] + cost <= quota["limit"]
                for quota, cost in chosen_quotas
            )
            or record["drain"]["state"] != "A"
            or record["conns"] >= effective_cap(backend_id)
        ):
            for scope, bucket_id, cost, _, _ in tracked:
                # 未接纳：令牌不足与配额不足按维各记（可同增）；两者皆
                # 无（仅目标非 A 或连接达 cap 的容量阻塞）不记。
                record_limit_block(
                    scope, bucket_id, cost,
                    token_short.get((scope, bucket_id), False),
                    quota_short.get((scope, bucket_id), False),
                    now,
                )
            return "block", backend_id
        # 接纳才按成本耗令牌、增配额 used 并按 open 建连接；全部满足后统一
        # 扣减，天然原子（其间无 fail 路径）。
        for bucket, cost in chosen:
            bucket["t"] -= cost
        for quota, cost in chosen_quotas:
            quota["used"] += cost
        establish_connection(cid, backend_id, flow, now)
        for scope, bucket_id, cost, _, _ in tracked:
            # 接纳记账：admitted 加 1、units 加该维成本。
            record_limit_admit(scope, bucket_id, cost, now)
        return "admit", backend_id

    def try_admit(cid, flow, c, s, key, costs, now):
        """按 oa/ot 规则尝试一次接纳：先路由再评估。路由不可用（未配环或无
        可选后端）返回 ("route", None)——oa 据此报 STATE，ot 视为队首阻塞即
        停；其余返回 evaluate_admit 的结果。ss 后按三键限时粘性（以本次
        now），未 ss 沿用旧二键语义。"""
        routed = select_route(
            key, now if sticky_ttl is not None else None, fatal=False
        )
        if routed is None:
            return "route", None
        backend_id = routed[0]
        return evaluate_admit(backend_id, cid, flow, c, s, costs, now)

    def record_metric(backend_id, ok, ms, retries, remaps, now):
        """按 mr 语义累加一条度量：写入 window=now//60 所属窗，每后端只保留
        最近 60 窗，五延迟桶 [≤1,≤10,≤100,≤1000,>1000]，各计数封顶 10^18。
        mr 与 fx/fr 完成后的自动度量按操作顺序共用此入口。"""
        record = backends[backend_id]
        window = now // 60
        history = record["metrics"]
        metrics = history.get(window)
        if metrics is None:
            # 首次报告该窗：新建计数；时钟非递减，顺带丢弃 60 窗前的旧窗。
            metrics = [0, 0, 0, 0, [0, 0, 0, 0, 0]]
            history[window] = metrics
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        metrics[0] = min(METRIC_CAP, metrics[0] + 1)
        if not ok:
            metrics[1] = min(METRIC_CAP, metrics[1] + 1)
        metrics[2] = min(METRIC_CAP, metrics[2] + retries)
        metrics[3] = min(METRIC_CAP, metrics[3] + remaps)
        # 上界 [1,10,100,1000]：桶依次为 ≤1、≤10、≤100、≤1000、>1000。
        bucket = bisect.bisect_left((1, 10, 100, 1000), ms)
        metrics[4][bucket] = min(METRIC_CAP, metrics[4][bucket] + 1)
        # 重试/重映射时刻历史（rr）：与 mr 同窗同入口写入，仅正数记账，
        # 零值不建窗；count 增量即 retries/remaps，故同窗 count 恒等于
        # mh 的 retries/remaps 累计（封顶规则一致）。
        record_retries(record, window, retries, remaps, now)

    def record_retries(record, window, retries, remaps, now):
        """重试/重映射时刻记账（O(1)）：window=now//60 窗的 retries/remaps
        两项，每项布局 [count,first,last]。count 增加写入的正数并封顶
        10^18，first 仅本窗首次写入时记 now，last 每次写入都更新（封顶后
        亦然）；零值不记。每后端只保留最近 60 窗，空窗不预建；remove 后
        重加与 ci/cb 成功随新记录清空。"""
        if retries <= 0 and remaps <= 0:
            return
        history = record["retry_hist"]
        entry = history.get(window)
        if entry is None:
            # 首次记账该窗：新建两项 [count,first,last]；时钟非递减，顺带
            # 丢弃 60 窗前旧窗。
            entry = {
                "retries": [0, None, None],
                "remaps": [0, None, None],
            }
            history[window] = entry
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        for amount, item in (
            (retries, entry["retries"]),
            (remaps, entry["remaps"]),
        ):
            if amount <= 0:
                # 零值不记：不触碰 count/first/last。
                continue
            item[0] = min(METRIC_CAP, item[0] + amount)
            # first 仅首次写入时记录；last 每次都更新（封顶后亦然）。
            if item[1] is None:
                item[1] = now
            item[2] = now

    def record_reason(backend_id, reason, now):
        """不可用原因分钟历史记账（O(1)）：reason ∈ health/drain/circuit/
        overload，归属后端 backend_id 的 window=now//60 窗，对应计数加 1 并
        封顶 10^18；first 为该窗该原因首次事件的 now（封顶不再变），last 为
        最近事件的 now，计数封顶后 last 仍更新。仅在确有状态转换（probe
        h→u、dr A→D/X、cr C/H→O）或 oa 因连接达 cap 入队 Q 时由调用方调
        用；重报、无转换与 oa 的 OVERLOAD 回滚均不调用。每后端只保留最近
        60 窗，空窗不预建。"""
        history = backends[backend_id]["reason_hist"]
        window = now // 60
        counts = history.get(window)
        if counts is None:
            # 首次记账该窗：新建四项 [count,first,last]；时钟非递减，顺带
            # 丢弃 60 窗前旧窗。
            counts = {
                "health": [0, None, None],
                "drain": [0, None, None],
                "circuit": [0, None, None],
                "overload": [0, None, None],
            }
            history[window] = counts
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        entry = counts[reason]
        entry[0] = min(METRIC_CAP, entry[0] + 1)
        # first 仅在首次事件时记录；last 每次事件都更新（封顶后亦然）。
        if entry[1] is None:
            entry[1] = now
        entry[2] = now

    def overload_window(now):
        """取 now//60 窗的过载分钟历史计数行（惰性建窗，空窗不预建），只保留
        最近 60 窗：时钟非递减，新建窗时丢弃下界之前的旧窗。行布局
        [immediate, queued, dequeued, expired, peak]。记账 O(1)。"""
        window = now // 60
        row = overload_hist.get(window)
        if row is None:
            row = [0, 0, 0, 0, 0]
            overload_hist[window] = row
            cutoff = window - 59
            for old in [w for w in overload_hist if w < cutoff]:
                del overload_hist[old]
        return row

    def record_wait(kind, leave_now, enqueue_now):
        """排队等待历史记账（O(1)）：kind ∈ admitted/expired/cancelled/
        evicted，等待项成功离队时按 d=离队时刻-入队 now 落入 ≤0、≤1、≤10、
        ≤100、>100 五桶（0/1/2/3/4），事件归离队时刻 window=leave_now//60
        窗，对应桶计数加 1 并封顶 10^18。只保留最近 60 窗，空窗不预建；
        时钟非递减，新建窗时丢弃下界之前的旧窗。立即接纳（从未入队）、
        失败批次（未成功离队）不调用。"""
        window = leave_now // 60
        row = wait_hist.get(window)
        if row is None:
            row = {
                "admitted": [0, 0, 0, 0, 0],
                "expired": [0, 0, 0, 0, 0],
                "cancelled": [0, 0, 0, 0, 0],
                "evicted": [0, 0, 0, 0, 0],
            }
            wait_hist[window] = row
            cutoff = window - 59
            for old in [w for w in wait_hist if w < cutoff]:
                del wait_hist[old]
        d = leave_now - enqueue_now
        if d <= 0:
            bucket = 0
        elif d <= 1:
            bucket = 1
        elif d <= 10:
            bucket = 2
        elif d <= 100:
            bucket = 3
        else:
            bucket = 4
        counts = row[kind]
        counts[bucket] = min(METRIC_CAP, counts[bucket] + 1)

    def active_segment(faults, a_values, now):
        """在段列表（a 升序且 [a,z) 互不重叠）与其平行 a 列表中取唯一活动
        段（O(log T_b)）；未登记或处于段间隙时返回 None。"""
        if not a_values:
            return None
        idx = bisect.bisect_right(a_values, now) - 1
        if idx < 0:
            return None
        segment = faults[idx]
        if now < segment[2]:
            return segment
        return None

    def active_fault(record, now):
        """按 now 在故障时间线中取唯一活动段：段按 a 升序且 [a,z) 互不
        重叠，故至多一段满足 a<=now<z（O(log T_b)，fault_a 为与 faults
        平行的 a 列表，随替换原子更新）；未登记或处于段间隙时返回 None。"""
        return active_segment(record["faults"], record["fault_a"], now)

    def fault_active(record, now):
        """mg 的 removed=fault 判定：活动段且 now ∈ [a,z) 窗口内时，D 恒为
        故障，F 仅 ((now-a)//v)%2=0 相位为故障；S（仅变慢）、F 非故障相位、
        段间隙与未登记均不算故障。"""
        segment = active_fault(record, now)
        if segment is None:
            return False
        k, a, _, v = segment
        if k == "D":
            return True
        if k == "F":
            return ((now - a) // v) % 2 == 0
        return False

    def removed_reason(record, now):
        """ms/mg 共用的 removed 优先级：drain（D/X）> health（unhealthy）>
        circuit（熔断非 C）> fault（D 窗口内或 F 故障相位），否则 null。"""
        if record["drain"]["state"] in ("D", "X"):
            return "drain"
        if not record["healthy"]:
            return "health"
        if not circuit_closed(record):
            return "circuit"
        if fault_active(record, now):
            return "fault"
        return None

    def fault_effect(segment, now):
        """fq 同款 effect：无活动段（未登记或段间隙）为 N；活动段 D 为 D、
        F 按 ((now-a)//v)%2=0 相位取 D 否则 N、S 为 S。"""
        if segment is None:
            return "N"
        k, a, _, v = segment
        if k == "D":
            return "D"
        if k == "F":
            return "D" if ((now - a) // v) % 2 == 0 else "N"
        return "S"

    def unavail_reason_fact(record, reason, now):
        """ru/ua 共用的单原因连续不可用事实：返回 (active, since)。不生效为
        (False, None)；生效时 since 为本次连续区间起点：health/drain/circuit
        取各转换点维护的连续生效起点，不因更高优先级原因存在而遮蔽；fault
        只读时间线——D 段 since=a，F 下线相位 [a+2jv,a+(2j+1)v) 的 since
        对齐到 a+2jv，F 上线相位、S（仅变慢）、段间隙与未登记均不生效。
        活动段查找 O(log T_b)，其余三因 dict 取键 O(1)。"""
        if reason == "fault":
            segment = active_fault(record, now)
            if segment is None:
                return False, None
            kind, seg_a, _, seg_v = segment
            if kind == "D":
                # 整个 D 段均不可用。
                return True, seg_a
            if kind == "F" and ((now - seg_a) // seg_v) % 2 == 0:
                # F 下线相位：[a+2jv, a+(2j+1)v)，相位起点对齐到偶数 v。
                return True, seg_a + 2 * seg_v * (
                    (now - seg_a) // (2 * seg_v)
                )
            # F 上线相位与 S（仅变慢）不阻断。
            return False, None
        reason_since = record["unavail_since"][reason]
        return reason_since is not None, reason_since

    def simulate_fr(tokens, digests, key, timeout, max_attempts, now,
                    fault_view=None):
        """只读模拟一次 fr 的哈希遍历与 D/F/S 规则，返回
        (state, backend, attempts, latency)：自 key 哈希点按 fx 顺序遍历
        不同后端至多 max_attempts 个，D/故障相位 F 失败耗时 0、S 的
        v>timeout 失败耗时 timeout，否则成功耗时 v 或 0 并终止；不建连、
        不记度量与故障统计。tokens 非空（空环由调用方先报 STATE）。
        fault_view 非 None 时为 {backend_id: (faults, fault_a)} 的只读
        候选视图（fd 差异预演），活动段改自视图查找、不触碰运行态。"""
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0  # 越界回绕到环首
        attempts = 0
        latency = 0
        chosen_id = None
        state = "R"
        seen = set()
        for offset in range(len(tokens)):
            if attempts >= max_attempts:
                break
            backend_id = tokens[(index + offset) % len(tokens)][3]
            if backend_id in seen:
                continue
            seen.add(backend_id)
            if fault_view is None:
                segment = active_fault(backends[backend_id], now)
            else:
                view_faults, view_a = fault_view[backend_id]
                segment = active_segment(view_faults, view_a, now)
            effect = fault_effect(segment, now)
            attempts += 1
            if effect == "D":
                # D/故障相位 F：本尝试失败、耗时 0。
                continue
            cost = segment[3] if effect == "S" else 0
            if cost > timeout:
                # S 且 v>timeout：本尝试失败，耗时按 timeout 计后重试。
                latency += timeout
                continue
            # 首个成功尝试即终止：耗时 v 或 0。
            latency += cost
            chosen_id = backend_id
            state = "A"
            break
        return state, chosen_id, attempts, latency

    def simulate_fc(tokens, digests, key, timeout, max_attempts, at,
                    fault_view=None):
        """fc 只读模拟：同 simulate_fr 的哈希遍历与 D/F/S 规则，但在 at 时刻
        求值并按尝试序产出 trace。返回
        (state, backend, attempts, latency, trace)：state ∈ A/R，backend 仅
        A 为 id 否则 null，attempts 为尝试后端数，latency 为各次计入耗时之
        和（与 simulate_fr 同口径）。trace 每项键序 id,kind,cost,result：
        kind 为该候选 at 时刻活动段的登记种类（D/F/S），段间隙或未登记为
        null（F 非故障相位仍记 "F"）；cost 为计入 latency 的本次耗时；
        result ∈ D/T/A，依次表示下线（D 或 F 故障相位，cost=0）、慢超时
        （S 且 v>timeout，cost=timeout）、接纳（cost 为 v 或 0，末项且
        state=A）。fault_view 语义同 simulate_fr（候选时间线视图）。
        不建连、不记度量与故障统计；tokens 非空（空环由调用方先报 STATE）。"""
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0  # 越界回绕到环首
        attempts = 0
        latency = 0
        chosen_id = None
        state = "R"
        seen = set()
        trace = []
        for offset in range(len(tokens)):
            if attempts >= max_attempts:
                break
            backend_id = tokens[(index + offset) % len(tokens)][3]
            if backend_id in seen:
                continue
            seen.add(backend_id)
            if fault_view is None:
                segment = active_fault(backends[backend_id], at)
            else:
                view_faults, view_a = fault_view[backend_id]
                segment = active_segment(view_faults, view_a, at)
            effect = fault_effect(segment, at)
            kind = None if segment is None else segment[0]
            attempts += 1
            if effect == "D":
                # D/故障相位 F：本尝试失败、耗时 0。
                trace.append(
                    {"id": backend_id, "kind": kind, "cost": 0, "result": "D"}
                )
                continue
            cost = segment[3] if effect == "S" else 0
            if cost > timeout:
                # S 且 v>timeout：本尝试失败，耗时按 timeout 计后重试。
                latency += timeout
                trace.append(
                    {
                        "id": backend_id, "kind": kind,
                        "cost": timeout, "result": "T",
                    }
                )
                continue
            # 首个成功尝试即终止：耗时 v 或 0。
            latency += cost
            trace.append(
                {"id": backend_id, "kind": kind, "cost": cost, "result": "A"}
            )
            chosen_id = backend_id
            state = "A"
            break
        return state, chosen_id, attempts, latency, trace

    def simulate_oi(tokens, digests, key, c, s, costs, timeout,
                    max_attempts, now):
        """只读模拟一次 oi 预演，返回
        (state, backend, attempts, latency, counts)：state ∈ A/R，backend
        仅 A 为 id 否则 None，attempts 为尝试后端数，latency 为各次耗时
        之和，counts 为各失败原因的失败尝试数 (fault, slow, capacity,
        quota)。自 key 哈希点按 fr 顺序遍历环上不同后端至多 max_attempts
        个（环同 route，仅健康、熔断 C、排空 A），每个候选即一次尝试，
        按 fault、slow、capacity、quota 优先判失败：D 或 F 故障相位失败
        耗时 0，S 且 v>timeout 失败耗时 timeout，conns≥cap 失败（耗时随
        S 规则），以 now 只读补充令牌并推进固定窗后任一 B/C/S 桶或配额
        不足失败（耗时同前）；到达 slow 之后的尝试一律按 S=v、其余 0 计
        耗时（即 S 尝试延迟 min(v,timeout)）。首个全部通过的后端即 A 并
        停止，耗尽为 R。只读：桶的补充结果只在本地投影上推进（每次尝试
        独立从当前真实桶投影一份），固定窗推进与配额余量只在局部变量上
        计算，绝不回写；不读写粘性、不建连、不记 mr/fm/fh。tokens 非空
        （空环由调用方先报 STATE）。"""
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0  # 越界回绕到环首
        bc, cc, sc = costs
        counts = {"fault": 0, "slow": 0, "capacity": 0, "quota": 0}
        attempts = 0
        latency = 0
        chosen_id = None
        state = "R"
        seen = set()
        for offset in range(len(tokens)):
            if attempts >= max_attempts:
                break
            backend_id = tokens[(index + offset) % len(tokens)][3]
            if backend_id in seen:
                continue
            seen.add(backend_id)
            attempts += 1
            record = backends[backend_id]
            segment = active_fault(record, now)
            effect = fault_effect(segment, now)
            if effect == "D":
                # D/故障相位 F：本尝试失败、耗时 0。
                counts["fault"] += 1
                continue
            # N（段间隙、未登记或 F 非故障相位）耗时 0；S 耗时 v。
            cost = segment[3] if effect == "S" else 0
            if cost > timeout:
                # S 且 v>timeout：本尝试失败，耗时按 timeout 计后重试。
                counts["slow"] += 1
                latency += timeout
                continue
            # 到达此后的尝试延迟：S 为 v（即 min(v,timeout)，v≤timeout），
            # 其余为 0；capacity/quota 失败与成功都按此计尝试耗时。
            latency += cost
            if record["conns"] >= effective_cap(backend_id):
                # 连接数达该后端有效容量（已配 pc 用覆盖，否则 os.cap）：
                # capacity 失败。
                counts["capacity"] += 1
                continue
            # 三桶三配额：以 now 只读补充/推进（每次尝试独立投影，不回写
            # 真实桶/配额）；B 桶与 B 配额挂在本候选后端，C/S 与后端无关。
            enough = True
            for scope, bucket_id, demand in (
                ("B", backend_id, bc),
                ("C", c, cc),
                ("S", s, sc),
            ):
                bucket = buckets.get((scope, bucket_id))
                if bucket is not None:
                    projected = min(
                        bucket["b"],
                        bucket["t"] + (now - bucket["at"]) * bucket["r"],
                    )
                    if projected < demand:
                        enough = False
                        break
                quota = quotas.get((scope, bucket_id))
                if quota is not None:
                    window = now // quota["span"]
                    used = 0 if window != quota["window"] else quota["used"]
                    if used + demand > quota["limit"]:
                        enough = False
                        break
            if not enough:
                # 任一桶或配额不足：quota 失败。
                counts["quota"] += 1
                continue
            # 全部通过：A 并停止，不补充、不扣减、不建连。
            chosen_id = backend_id
            state = "A"
            break
        return state, chosen_id, attempts, latency, counts

    def simulate_od(tokens, digests, key, c, s, costs, timeout,
                    max_attempts, now):
        """只读模拟一次 od 接纳明细，返回 (state, backend, trace)：遍历与
        fault、slow、capacity、quota 优先判定同 oi，首个全部通过的后端即
        A 并停止，耗尽为 R。trace 按尝试序，项键序
        id,effect,latency,result,blocked：effect 为该候选 now 时刻的
        N/D/S；latency 为 S 时 min(v,timeout)、否则 0；result 为
        F（故障）/S（超时）/C（满载）/Q（限额）/A（接纳）；blocked 仅 Q
        时按 BT,BQ,CT,CQ,ST,SQ 序列出不足项（T 为按 now 只读补充后的令
        牌桶，Q 为推进窗口后的固定配额），否则为空数组。只读投影同 oi：
        不补充、不扣减、不回写，不读写粘性、不建连、不记 mr/fm/fh。
        tokens 非空（空环由调用方先报 STATE）。"""
        key_hash = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest(), "big"
        )
        index = bisect.bisect_left(digests, key_hash)
        if index == len(tokens):
            index = 0  # 越界回绕到环首
        bc, cc, sc = costs
        trace = []
        chosen_id = None
        state = "R"
        seen = set()
        attempts = 0
        for offset in range(len(tokens)):
            if attempts >= max_attempts:
                break
            backend_id = tokens[(index + offset) % len(tokens)][3]
            if backend_id in seen:
                continue
            seen.add(backend_id)
            attempts += 1
            record = backends[backend_id]
            segment = active_fault(record, now)
            effect = fault_effect(segment, now)
            # N（段间隙、未登记或 F 非故障相位）耗时 0；S 耗时
            # min(v,timeout)；D 耗时 0。
            cost = segment[3] if effect == "S" else 0
            latency = min(cost, timeout)
            entry = {
                "id": backend_id,
                "effect": effect,
                "latency": latency,
                "result": None,
                "blocked": [],
            }
            trace.append(entry)
            if effect == "D":
                # D/故障相位 F：本尝试失败。
                entry["result"] = "F"
                continue
            if cost > timeout:
                # S 且 v>timeout：本尝试失败。
                entry["result"] = "S"
                continue
            if record["conns"] >= effective_cap(backend_id):
                # 连接数达该后端有效容量（已配 pc 用覆盖，否则 os.cap）：
                # capacity 失败。
                entry["result"] = "C"
                continue
            # 三桶三配额：以 now 只读补充/推进（每次尝试独立投影，不回写
            # 真实桶/配额）；按 BT,BQ,CT,CQ,ST,SQ 序收集全部不足项。
            blocked = entry["blocked"]
            for scope, bucket_id, demand, t_label, q_label in (
                ("B", backend_id, bc, "BT", "BQ"),
                ("C", c, cc, "CT", "CQ"),
                ("S", s, sc, "ST", "SQ"),
            ):
                bucket = buckets.get((scope, bucket_id))
                if bucket is not None:
                    projected = min(
                        bucket["b"],
                        bucket["t"] + (now - bucket["at"]) * bucket["r"],
                    )
                    if projected < demand:
                        blocked.append(t_label)
                quota = quotas.get((scope, bucket_id))
                if quota is not None:
                    window = now // quota["span"]
                    used = 0 if window != quota["window"] else quota["used"]
                    if used + demand > quota["limit"]:
                        blocked.append(q_label)
            if blocked:
                # 任一桶或配额不足：quota 失败。
                entry["result"] = "Q"
                continue
            # 全部通过：A 并停止，不补充、不扣减、不建连。
            entry["result"] = "A"
            chosen_id = backend_id
            state = "A"
            break
        return state, chosen_id, trace

    def fault_window_stats(record, now):
        """取 now//60 窗的故障记账计数组（惰性建窗），并只保留最近 60 窗。
        时钟非递减，新建窗时丢弃 cutoff 之前的旧窗；空窗不预建。fm 累计与
        各窗计数共用 new_fault_stats 结构，由调用方同序双写。"""
        window = now // 60
        history = record["fault_hist"]
        stats = history.get(window)
        if stats is None:
            stats = new_fault_stats()
            history[window] = stats
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        return stats

    def settle_fault_baseline(record, keep, now):
        """换段或落入段间隙时结算旧基线：fault_base 中除 keep 外的已受影响
        段各计一次 recovered（归该段自身种类 k 与当前观察窗 now//60），随后
        移除；keep 为本次活动段时其基线保留。时间线段互不重叠、时钟非递减，
        实际至多一个旧段在基线中。各计数封顶 10^18。"""
        base = record["fault_base"]
        if not base:
            return
        for old_segment in list(base):
            if old_segment == keep:
                continue
            kind = old_segment[0]
            totals = record["fault_stats"][kind]
            totals["recovered"] = min(
                METRIC_CAP, totals["recovered"] + 1
            )
            # 恢复归当前观察窗（首次观察到 N 的请求窗）；空窗不预建。
            window_stats = fault_window_stats(record, now)[kind]
            window_stats["recovered"] = min(
                METRIC_CAP, window_stats["recovered"] + 1
            )
            base.discard(old_segment)

    def observe_fault(record, segment, effect, rejected, now):
        """fx/fr 访问后端的一次记账（O(1)）。segment 为按 now 取到的唯一
        活动段（间隙为 None）；增量归活动段自身种类：effect 为 D/S 即受影响
        （affected+1 并以段元组置恢复基线），D 失败或 S 耗时超 timeout 另
        rejected+1。上次受影响段首次变 N——F 同段非故障相位、换段或落入
        间隙——recovered 归上段种类（当前观察窗）并清其基线；同次新段照常
        记账；连续 N 不重复。fm 累计与 window=now//60 分钟窗同序双写，各
        计数封顶 10^18。"""
        if segment is None:
            # 段间隙：无活动段可记账，仅结算可能存在的上段恢复。
            settle_fault_baseline(record, None, now)
            return
        kind = segment[0]
        totals = record["fault_stats"][kind]
        if effect == "N":
            # F 非故障相位：先结算其它段（换段）的恢复，本段仅在已置基线时
            # 才计 recovered；连续 N 不重复。
            settle_fault_baseline(record, segment, now)
            if segment in record["fault_base"]:
                totals["recovered"] = min(
                    METRIC_CAP, totals["recovered"] + 1
                )
                window_stats = fault_window_stats(record, now)[kind]
                window_stats["recovered"] = min(
                    METRIC_CAP, window_stats["recovered"] + 1
                )
                record["fault_base"].discard(segment)
            return
        # 受影响（D/S）：先结算其它已置基线段（换段时 recovered 归上段 k），
        # 同次新段照常记 affected。
        settle_fault_baseline(record, segment, now)
        window_stats = fault_window_stats(record, now)[kind]
        totals["affected"] = min(METRIC_CAP, totals["affected"] + 1)
        window_stats["affected"] = min(
            METRIC_CAP, window_stats["affected"] + 1
        )
        record["fault_base"].add(segment)
        if rejected:
            totals["rejected"] = min(METRIC_CAP, totals["rejected"] + 1)
            window_stats["rejected"] = min(
                METRIC_CAP, window_stats["rejected"] + 1
            )

    def bump_fault(record, segment, field, now):
        """fx 跳过（remaps）与 fr 重试（retries/remaps）的计数：归失败后端
        该次活动段的登记种类，fm 累计与 now//60 窗各加 1，封顶 10^18。仅在
        确有活动段（effect 为 D 或超时 S）时被调用。"""
        kind = segment[0]
        totals = record["fault_stats"][kind]
        totals[field] = min(METRIC_CAP, totals[field] + 1)
        window_stats = fault_window_stats(record, now)[kind]
        window_stats[field] = min(METRIC_CAP, window_stats[field] + 1)

    def record_h_pick(chosen_id, old_entry, now):
        """H 模式 pick 成功后记一次账，归属返回 id chosen_id：仅在成功项调用
        一次。分类（重叠按 expired>removed>health>circuit>drain）：
        无旧映射 first；三键旧 e 非 null 且 now>=e 为 expired（重选回原 id
        亦然）；否则旧目标不存在 removed、unhealthy 为 health、熔断非 C 为
        circuit、排空非 A 为 drain；旧目标合格且未判到期为 sticky。total 与
        对应项各 +1，封顶 10^18。"""
        if old_entry is None:
            category = "first"
        else:
            old_b = old_entry[0]
            old_expires = old_entry[1]
            if (
                now is not None
                and old_expires is not None
                and now >= old_expires
            ):
                category = "expired"
            else:
                old_record = backends.get(old_b)
                if old_record is None:
                    category = "removed"
                elif not old_record["healthy"]:
                    category = "health"
                elif not circuit_closed(old_record):
                    category = "circuit"
                elif old_record["drain"]["state"] != "A":
                    category = "drain"
                else:
                    category = "sticky"
        counts = backends[chosen_id]["pick_counts"]
        counts["total"] = min(METRIC_CAP, counts["total"] + 1)
        counts[category] = min(METRIC_CAP, counts[category] + 1)

    def export_config():
        """ce 与提交快照共用的配置导出：纯登记值、不含任何运行态，逐层键序
        固定（version=11；backends 按加入序，项 id,weight,d,fail,success,
        circuit,drain,endpoint；limits 按 scope 的 B/C/S 序、id 的 UTF-8
        字节升序；overload/sticky/idle/backpressure 为 null 或登记值；
        scheduler 精确为 {"pick":...}；faults 按后端加入序、段 a 升序，
        项键序 id,k,a,z,v，空计划为 []，不含 effect 或运行态；quotas，
        项键序 scope,id,limit,span，按 scope 的 B/C/S 序、id 的 UTF-8
        字节升序，不含 window、used；queue 键序 dequeue,full，仅登记
        出队与满载策略（F/S、T/H），不含等待项、evicted 或 last；
        capacities 按后端加入序仅列显式接纳容量覆盖，项键序 id,cap，空
        为 []；lifetime 末置，为 null 或 {"ttl":当前硬时限登记值}）。
        返回全新结构，调用方可安全存为快照（不随后续运行态变化；ci/cb/ca
        后的 tm 修改只改当前导出，不影响已存提交与预约快照）。"""
        exported_backends = []
        for backend_id, record in backends.items():
            circuit = record["circuit"]
            endpoint = record["endpoint"]
            exported_backends.append(
                {
                    "id": backend_id,
                    "weight": record["weight"],
                    "d": record["warm_d"],
                    "fail": record["fail"],
                    "success": record["success"],
                    "circuit": (
                        None
                        if circuit is None
                        else {
                            "n": circuit["params"][0],
                            "m": circuit["params"][1],
                            "r": circuit["params"][2],
                            "w": circuit["params"][3],
                            "q": circuit["params"][4],
                        }
                    ),
                    "drain": record["drain"]["t"],
                    # 既有七键后追加 endpoint：null 或键序 host,port。
                    "endpoint": (
                        None
                        if endpoint is None
                        else {"host": endpoint[0], "port": endpoint[1]}
                    ),
                }
            )
        exported_limits = [
            {"scope": scope, "id": bucket_id, "r": bucket["r"], "b": bucket["b"]}
            # 仅遍历不消费：按 (scope 秩, id UTF-8 字节) 升序输出。
            for (scope, bucket_id), bucket in sorted(
                buckets.items(),
                key=lambda item: (
                    {"B": 0, "C": 1, "S": 2}[item[0][0]],
                    item[0][1].encode("utf-8"),
                ),
            )
        ]
        exported_overload = (
            None
            if queue_cfg is None
            else {"cap": queue_cfg[0], "q": queue_cfg[1], "ttl": queue_cfg[2]}
        )
        # 三项均只导出登记值：sticky/idle 为 null 或 {"ttl":整数}，
        # backpressure 为 null 或 {low,high}（不含 bp_state 运行态）；
        # scheduler 精确为 {"pick":"W"/"R"/"L"/"H"}（不含 ticket 运行态）。
        # faults 按后端加入序、同后端段 a 升序（record["faults"] 已为该序），
        # 纯登记段、不含 effect 或运行态；空计划为 []。
        exported_faults = []
        for backend_id, record in backends.items():
            for k, a, z, v in record["faults"]:
                exported_faults.append(
                    {"id": backend_id, "k": k, "a": a, "z": z, "v": v}
                )
        # quotas 末置：仅登记值（limit/span），不含 window、used 运行态；
        # 按 (scope 秩, id UTF-8 字节) 升序输出，同 limits。
        exported_quotas = [
            {
                "scope": scope,
                "id": quota_id,
                "limit": quota["limit"],
                "span": quota["span"],
            }
            for (scope, quota_id), quota in sorted(
                quotas.items(),
                key=lambda item: (
                    {"B": 0, "C": 1, "S": 2}[item[0][0]],
                    item[0][1].encode("utf-8"),
                ),
            )
        ]
        # capacities：当前配置的每后端接纳容量覆盖，按后端加入序仅列
        # 显式项，项键序 id,cap，空为 []。ci/cb 时随配置原子替换，此后 pc
        # 只改当前 ce（此处实时反映），不改已存提交快照（快照为全新结构）。
        exported_capacities = [
            {"id": backend_id, "cap": cap_overrides[backend_id]}
            for backend_id in backends
            if backend_id in cap_overrides
        ]
        # lifetime 末置：导出当前硬时限登记（ci/cb/ca 按快照载入，tm 后续
        # 修改只改当前 ce/ct）；未 tm 且快照未携带（规范化 null）时为 null。
        return {
            "version": 11,
            "backends": exported_backends,
            "vnodes": ring_vnodes,
            "limits": exported_limits,
            "overload": exported_overload,
            "sticky": None if sticky_ttl is None else {"ttl": sticky_ttl},
            "idle": None if ttl_cfg is None else {"ttl": ttl_cfg},
            "backpressure": (
                None
                if bp_cfg is None
                else {"low": bp_cfg[0], "high": bp_cfg[1]}
            ),
            "scheduler": {"pick": pick_mode},
            "faults": exported_faults,
            "quotas": exported_quotas,
            # queue：仅登记出队（F/S）与满载（T/H）策略，键序
            # dequeue,full；不含等待项、evicted、last 与 qa 老化登记等运行
            # 态。P 不持久化：queue_mode 为 P 时导出进入 P 前保留的 F/S。
            "queue": {"dequeue": dequeue_policy, "full": full_mode},
            "capacities": exported_capacities,
            "lifetime": (
                None if hard_ttl_cfg is None else {"ttl": hard_ttl_cfg}
            ),
        }

    def apply_config(config, now):
        """ci/cb/ca 共用的原子替换：以 now 重建默认运行态（全部 healthy、d>0
        自 now 起算预热、熔断 C 空窗、排空 A、桶满、配额按 quotas 重建
        （window=now//span、used=0，v1..v7 为空即清空）、队空、粘性清空、
        度量归零、平滑 current 与轮询 ticket=0；sticky/idle 取登记值作用于新
        连接，backpressure 携带时置 N、未携带时取消；队列策略按配置 queue
        载入（v1..v8 规范化为 F/T，v9+ 为登记的 dequeue/full），队空且
        FIFO 满载淘汰计数与最近淘汰 cid 清零；每后端接纳容量覆盖按
        capacities 原子替换（v10+ 显式覆盖，旧版或 [] 清空）；硬时限按
        lifetime 原子载入（v11 携带 ttl 即登记，v1..v10 或 null 清除回到
        未配，作用于此后新建连接）；fe/ah 告警
        状态与历史清除；故障统计、分钟历史与恢复基线重置），并按 v7+
        faults 载入各后端登记时间线（v1..v6 为空计划）。调用方须已完成
        全部校验，本函数自身不再失败。"""
        nonlocal backends, buckets, quotas, ring_vnodes, queue_cfg, wait_queue
        nonlocal sticky_ttl, ttl_cfg, bp_cfg, bp_state, pick_mode, rr_ticket
        nonlocal hard_ttl_cfg
        nonlocal sticky_map, alert, alert_events, overload_hist, wait_hist, err_alerts
        nonlocal err_events, percent_alerts, percent_events, wait_alerts, retry_alerts, retry_events
        nonlocal conc_alerts, limit_alerts, unavail_alerts
        nonlocal mo_seq, mo_cache, queue_mode, dequeue_policy
        nonlocal full_mode, evict_count, evict_last, cap_overrides
        nonlocal limit_hist, aging_cfg

        def make_record(weight, d, fail_threshold, success_threshold,
                        circuit_params, drain_t, endpoint, fault_segments):
            if d == 0:
                stage = "steady"
                warm_from = warm_start = warm_end = None
            else:
                # 预热自本次 now 起算。
                stage = "warm"
                warm_from = 100
                warm_start = now
                warm_end = now + d
            return {
                "weight": weight,
                "current": 0,
                "conns": 0,
                "healthy": True,
                "fail": fail_threshold,
                "success": success_threshold,
                "failures": 0,
                "successes": 0,
                "probe_now": None,
                "probe_ok": None,
                "stage": stage,
                "warm_d": d,
                "warm_from": warm_from,
                "warm_start": warm_start,
                "warm_end": warm_end,
                "circuit": (
                    None
                    if circuit_params is None
                    else {
                        "params": circuit_params,
                        "state": "C",
                        "window": deque(maxlen=circuit_params[0]),
                        "next": None,
                        "used": 0,
                        "cr_now": None,
                        "cr_ok": None,
                    }
                ),
                "drain": {
                    "t": drain_t,
                    "state": "A",
                    "start": None,
                    "end": None,
                    "deadline": None,
                    "forced": 0,
                },
                # ci/cb 重建默认运行态：ru 的 health/drain/circuit 连续生效
                # 起点全部清除（fault 只读时间线）。
                "unavail_since": {
                    "health": None, "drain": None, "circuit": None,
                },
                # 热加载不携带历史写操作形状。
                "last_op": None,
                # 后端 IP 端点登记值（v1..v5 已规范化为 None）。
                "endpoint": endpoint,
                "metrics": {},
                # ci/cb 重建默认运行态：mo 增量基线清零。
                "mo_base": [None, 0, 0, 0, 0, [0, 0, 0, 0, 0]],
                # ci 成功清零 H pick 记账。
                "pick_counts": {
                    "total": 0, "first": 0, "sticky": 0, "expired": 0,
                    "removed": 0, "health": 0, "circuit": 0, "drain": 0,
                },
                # ci 成功清空采样历史，默认运行态为空。
                "samples": {},
                # 故障时间线：ci version=7 载入登记段（按 a 升序、互不
                # 重叠），v1..v6 为空计划；fault_a 为平行的段起点 a 列表，
                # 供 bisect O(log T_b) 取唯一活动段。
                "faults": [segment for segment in fault_segments],
                "fault_a": [segment[1] for segment in fault_segments],
                # ci 成功清零故障演练统计与恢复判定基线。
                "fault_stats": new_fault_stats(),
                # 恢复判定基线：已被观察为受影响（D/S）的段元组集合；该段
                # 首次再观察为 N（F 非故障相位、换段或落入间隙）时按段种类
                # 结算 recovered 并移除。fs/fb/fp 异参替换只清基线不清计数。
                "fault_base": set(),
                # ci 成功清空故障统计分钟历史。
                "fault_hist": {},
                # ci 成功清空不可用原因分钟历史。
                "reason_hist": {},
                # ci/cb 成功清空重试/重映射时刻历史。
                "retry_hist": {},
            }

        new_backends = {}
        for (backend_id, weight, d, fail_threshold,
             success_threshold, circuit_params, drain_t,
             endpoint) in config["backends"]:
            new_backends[backend_id] = make_record(
                weight, d, fail_threshold, success_threshold,
                circuit_params, drain_t, endpoint,
                config["faults"].get(backend_id, []),
            )
        # 新桶满令牌起步，at=now。
        new_buckets = {}
        for scope, bucket_id, r, b in config["limits"]:
            new_buckets[(scope, bucket_id)] = {
                "r": r,
                "b": b,
                "t": b,
                "at": now,
                "last": None,
            }
        backends = new_backends
        buckets = new_buckets
        # 固定窗口配额随配置原子替换：v8+ 按 quotas 载入，各配额置
        # window=now//span、used=0（last 置空，qs 精确重报豁免不跨热加载
        # 保留）；v1..v7 已规范化为空，即清空全部配额。
        new_quotas = {}
        for scope, quota_id, limit, span in config["quotas"]:
            new_quotas[(scope, quota_id)] = {
                "limit": limit,
                "span": span,
                "window": now // span,
                "used": 0,
                "last": None,
            }
        quotas = new_quotas
        ring_vnodes = config["vnodes"]
        queue_cfg = config["overload"]
        wait_queue = OrderedDict()
        # 每后端接纳容量覆盖随配置原子替换：v10 按 capacities 仅列显式覆盖
        # （规范化列表已按后端加入序、id 均为本配置后端）；旧版或 [] 即清空
        # （全部使用 overload.cap）。ci 成功后 pc 只改当前 ce，不改已存提交。
        cap_overrides = dict(config["capacities"])
        # ci/cb 成功清空过载分钟历史。
        overload_hist = {}
        # ci/cb/ca 成功清空排队等待历史（等待项随队空一并清空，无遗留事件）。
        wait_hist = {}
        # ci/cb/ca 成功清空全部排队等待分位告警（wa 各 kind 均回到未首评）。
        wait_alerts = {}
        # ci/cb 成功清空多维限流历史（ls/qs 重配不清，但热加载/回滚整体
        # 重建运行态，同 id 历史不跨配置保留）。
        limit_hist = {}
        # 热加载三项：sticky/idle 取登记值（null 即未登记，idle 作用于
        # 此后新建连接）；backpressure 携带时置 N，未携带（含 v1）即取消。
        sticky_ttl = config["sticky"]
        ttl_cfg = config["idle"]
        # 硬时限随配置快照原子载入（ci/cb/ca 与 v11 导出导入语义一致）：
        # config["lifetime"] 为 None（v1..v10 或 v11 null）即清除登记回到
        # 未配，否则为登记的 ttl 整数，作用于此后新建连接；活动连接已被
        # ci/cb/ca 前置清空，无既有连接受影响。
        hard_ttl_cfg = config["lifetime"]
        bp_cfg = config["backpressure"]
        bp_state = "N"
        # 队列策略按配置 queue 原子载入：dequeue 仅 F/S（v1..v8 规范化为
        # F），full 仅 T/H（v1..v8 规范化为 T）；同时清空排队项，满载淘汰
        # 计数与最近淘汰 cid 清零。P 不持久化：ci/cb/ca 成功即采用所载
        # F/S（queue_mode 与 dequeue_policy 同置该值，不可能为 P）并清空
        # qa 老化登记。
        queue_mode = config["queue"][0]
        dequeue_policy = config["queue"][0]
        aging_cfg = None
        full_mode = config["queue"][1]
        evict_count = 0
        evict_last = None
        # 调度策略随配置原子替换（v1/v2 已规范化为 W），轮询游标复位。
        pick_mode = config["scheduler"]
        rr_ticket = 0
        sticky_map = {}
        # ci 成功清除全池故障告警状态（fe 回到未首评）与转换历史。
        alert = None
        alert_events = deque()
        # ci/cb 成功清空全部后端错误率告警（ea 各 id 均回到未首评）。
        err_alerts = {}
        # ci/cb 成功同时清空全部后端 ea 告警转换历史。
        err_events = {}
        # ci/cb 成功清空全部后端延迟分位告警（pa 各 id 均回到未首评）。
        percent_alerts = {}
        # ci/cb 成功同时清空全部后端 pa 告警转换历史。
        percent_events = {}
        # ci/cb/ca 成功清空全部后端重试/重映射告警（xa 各 (id,k) 均回到
        # 未首评）。
        retry_alerts = {}
        # ci/cb/ca 成功同时清空全部后端 xa 告警转换历史。
        retry_events = {}
        # ci/cb/ca/cu 成功清空全部后端并发告警（na 各 id 均回到未首评）。
        conc_alerts = {}
        # ci/cb/ca/cu 成功清空全部限流告警（le 各标识均回到未首评）。
        limit_alerts = {}
        # ci/cb/ca/cu 成功清空全部连续不可用时长告警（ua 各 (id,reason)
        # 均回到未首评）。
        unavail_alerts = {}
        # ci/cb 成功清 mo 游标与缓存、seq 重置为 1（各后端基线随新记录
        # 清零）；失败时调用方根本不会进入本函数，天然回滚。
        mo_seq = 1
        mo_cache = None

    # -- 运行态检查点（se/si）---------------------------------------------
    # state 为逐层固定键序的规范化 JSON 对象：有业务顺序的集合（后端加入
    # 序、连接建连序、FIFO 队列、提交/审计 rev 序、告警事件窗序、分钟窗
    # 时序）保持原序；无业务顺序的集合（粘性键、桶/配额 (scope,id)、池级
    # 告警标识、老化服务类等）统一按 UTF-8 字节（scope 先按 B/C/S）排序。
    # 摘要为紧凑 UTF-8 编码的 {"version":1,"state":...} 的小写 SHA-256。
    def enc_fault_stats(stats):
        return {kind: [
            stats[kind]["affected"], stats[kind]["rejected"],
            stats[kind]["retries"], stats[kind]["remaps"],
            stats[kind]["recovered"],
        ] for kind in "DFS"}

    def export_bundle(b):
        """把一个完整运行态束 b（dict，键见 BUNDLE_KEYS）规范化为新 JSON
        结构。se 直接传当前闭包变量构成的束；si 先在局部构造候选束并据此
        校验、计算摘要，全部通过后才整体替换闭包变量（原子）。O(N)，N 为
        编码字节数的同阶。只读，不推进时钟、不改束内状态。"""
        backends = b["backends"]
        connections = b["connections"]
        conn_endpoints = b["conn_endpoints"]
        sticky_map = b["sticky_map"]
        buckets = b["buckets"]
        quotas = b["quotas"]
        wait_queue = b["wait_queue"]
        cap_overrides = b["cap_overrides"]
        overload_hist = b["overload_hist"]
        wait_hist = b["wait_hist"]
        limit_hist = b["limit_hist"]
        err_alerts = b["err_alerts"]
        err_events = b["err_events"]
        percent_alerts = b["percent_alerts"]
        percent_events = b["percent_events"]
        retry_alerts = b["retry_alerts"]
        retry_events = b["retry_events"]
        conc_alerts = b["conc_alerts"]
        limit_alerts = b["limit_alerts"]
        wait_alerts = b["wait_alerts"]
        unavail_alerts = b["unavail_alerts"]
        alert = b["alert"]
        alert_events = b["alert_events"]
        commit_history = b["commit_history"]
        audit_events = b["audit_events"]
        audit_sections = b["audit_sections"]
        reservation = b["reservation"]
        ep_switch = b["ep_switch"]
        mo_cache = b["mo_cache"]
        last_now = b["last_now"]
        ring_vnodes = b["ring_vnodes"]
        sticky_ttl = b["sticky_ttl"]
        ttl_cfg = b["idle_ttl"]
        hard_ttl_cfg = b["hard_ttl"]
        pick_mode = b["pick_mode"]
        rr_ticket = b["rr_ticket"]
        queue_cfg = b["queue_cfg"]
        bp_cfg = b["bp_cfg"]
        bp_state = b["bp_state"]
        dequeue_policy = b["dequeue_policy"]
        queue_mode = b["queue_mode"]
        full_mode = b["full_mode"]
        evict_count = b["evict_count"]
        evict_last = b["evict_last"]
        aging_cfg = b["aging_cfg"]
        mo_seq = b["mo_seq"]
        next_rev = b["next_rev"]
        backend_states = []
        for backend_id, record in backends.items():
            metrics = [
                {
                    "w": window,
                    "v": [
                        row[0], row[1], row[2], row[3], list(row[4]),
                    ],
                }
                for window, row in sorted(record["metrics"].items())
            ]
            mobase = record["mo_base"]
            samples = [
                {
                    "w": window,
                    "samples": snapshot["samples"],
                    "peak": snapshot["peak"],
                    "points": [
                        {"t": point_now, "c": conns, "r": reason}
                        for point_now, (conns, reason)
                        in sorted(snapshot["points"].items())
                    ],
                }
                for window, snapshot in sorted(record["samples"].items())
            ]
            fault_hist = [
                {"w": window, "v": enc_fault_stats(stats)}
                for window, stats in sorted(record["fault_hist"].items())
            ]
            reason_hist = []
            for window, counts in sorted(record["reason_hist"].items()):
                reason_hist.append(
                    {
                        "w": window,
                        "health": list(counts["health"]),
                        "drain": list(counts["drain"]),
                        "circuit": list(counts["circuit"]),
                        "overload": list(counts["overload"]),
                    }
                )
            retry_hist = [
                {
                    "w": window,
                    "retries": list(entry["retries"]),
                    "remaps": list(entry["remaps"]),
                }
                for window, entry in sorted(record["retry_hist"].items())
            ]
            # fault_base 为段集合，无独立业务顺序：按 (a,k,z,v) 排序。
            fault_base = [
                {"k": seg[0], "a": seg[1], "z": seg[2], "v": seg[3]}
                for seg in sorted(
                    record["fault_base"],
                    key=lambda seg: (seg[1], seg[0], seg[2], seg[3]),
                )
            ]
            faults = [
                {"k": seg[0], "a": seg[1], "z": seg[2], "v": seg[3]}
                for seg in record["faults"]
            ]
            circuit = record["circuit"]
            if circuit is None:
                circuit_state = None
            else:
                cr = circuit["cr_now"]
                circuit_state = {
                    "n": circuit["params"][0],
                    "m": circuit["params"][1],
                    "r": circuit["params"][2],
                    "w": circuit["params"][3],
                    "q": circuit["params"][4],
                    "state": circuit["state"],
                    "win": list(circuit["window"]),
                    "next": circuit["next"],
                    "used": circuit["used"],
                    "cr": (
                        None if cr is None
                        else {"t": cr, "ok": circuit["cr_ok"]}
                    ),
                }
            drain = record["drain"]
            last_op = record["last_op"]
            if last_op is None:
                last_op_state = None
            elif last_op[0] == "add3":
                last_op_state = {"k": "add3", "w": last_op[1],
                                 "d": None, "t": None}
            else:
                last_op_state = {"k": last_op[0], "w": last_op[1],
                                 "d": last_op[2], "t": last_op[3]}
            endpoint = record["endpoint"]
            # 每后端告警：ea/eh、pa/ph、xa/xh（R/M）、na、ua 四因。
            err_entry = err_alerts.get(backend_id)
            pa_entry = percent_alerts.get(backend_id)
            alerts_state = {
                "err": (
                    None if err_entry is None
                    else [
                        err_entry["hi"], err_entry["lo"], err_entry["n"],
                        err_entry["state"], err_entry["run"], err_entry["w"],
                        err_entry["result"]["requests"],
                        err_entry["result"]["errors"],
                        err_entry["result"]["rate"],
                        err_entry["result"]["changed"],
                    ]
                ),
                "err_events": [
                    [event["window"], event["from"], event["to"],
                     event["rate"], event["hi"], event["lo"], event["n"]]
                    for event in err_events.get(backend_id, ())
                ],
                "pa": (
                    None if pa_entry is None
                    else [
                        pa_entry["p"], pa_entry["hi"], pa_entry["lo"],
                        pa_entry["n"], pa_entry["state"], pa_entry["run"],
                        pa_entry["w"], pa_entry["result"]["samples"],
                        pa_entry["result"]["bucket"],
                        pa_entry["result"]["upper"],
                        pa_entry["result"]["changed"],
                    ]
                ),
                "pa_events": [
                    [event["window"], event["from"], event["to"],
                     event["p"], event["samples"], event["bucket"],
                     event["upper"], event["hi"], event["lo"], event["n"]]
                    for event in percent_events.get(backend_id, ())
                ],
                "xa": {},
                "xa_events": {},
                "na": None,
                "ua": {},
            }
            for kind_code in ("R", "M"):
                entry = retry_alerts.get((backend_id, kind_code))
                alerts_state["xa"][kind_code] = (
                    None if entry is None
                    else [
                        entry["hi"], entry["lo"], entry["n"],
                        entry["state"], entry["run"], entry["w"],
                        entry["result"]["value"], entry["result"]["changed"],
                    ]
                )
                alerts_state["xa_events"][kind_code] = [
                    [event["window"], event["from"], event["to"],
                     event["value"], event["hi"], event["lo"], event["n"]]
                    for event in retry_events.get((backend_id, kind_code), ())
                ]
            na_entry = conc_alerts.get(backend_id)
            alerts_state["na"] = (
                None if na_entry is None
                else [
                    na_entry["hi"], na_entry["lo"], na_entry["n"],
                    na_entry["state"], na_entry["run"], na_entry["w"],
                    na_entry["result"]["samples"],
                    na_entry["result"]["peak"],
                    na_entry["result"]["changed"],
                ]
            )
            for reason_code in ("drain", "health", "circuit", "fault"):
                entry = unavail_alerts.get((backend_id, reason_code))
                alerts_state["ua"][reason_code] = (
                    None if entry is None
                    else [
                        entry["threshold"], entry["state"], entry["now"],
                        entry["result"]["active"], entry["result"]["since"],
                        entry["result"]["duration"],
                        entry["result"]["changed"],
                    ]
                )
            backend_states.append(
                {
                    "id": backend_id,
                    "weight": record["weight"],
                    "current": record["current"],
                    "conns": record["conns"],
                    "healthy": record["healthy"],
                    "fail": record["fail"],
                    "success": record["success"],
                    "failures": record["failures"],
                    "successes": record["successes"],
                    "probe": (
                        None if record["probe_now"] is None
                        else {"t": record["probe_now"], "ok": record["probe_ok"]}
                    ),
                    "warm": {
                        "stage": record["stage"],
                        "d": record["warm_d"],
                        "from": record["warm_from"],
                        "start": record["warm_start"],
                        "end": record["warm_end"],
                    },
                    "circuit": circuit_state,
                    "drain": {
                        "t": drain["t"],
                        "state": drain["state"],
                        "start": drain["start"],
                        "end": drain["end"],
                        "deadline": drain["deadline"],
                        "forced": drain["forced"],
                    },
                    "since": {
                        "health": record["unavail_since"]["health"],
                        "drain": record["unavail_since"]["drain"],
                        "circuit": record["unavail_since"]["circuit"],
                    },
                    "lastop": last_op_state,
                    "endpoint": (
                        None if endpoint is None
                        else {"host": endpoint[0], "port": endpoint[1]}
                    ),
                    "metrics": metrics,
                    "mobase": {
                        "w": mobase[0],
                        "v": [
                            mobase[1], mobase[2], mobase[3], mobase[4],
                            list(mobase[5]),
                        ],
                    },
                    "pickcounts": {
                        "total": record["pick_counts"]["total"],
                        "first": record["pick_counts"]["first"],
                        "sticky": record["pick_counts"]["sticky"],
                        "expired": record["pick_counts"]["expired"],
                        "removed": record["pick_counts"]["removed"],
                        "health": record["pick_counts"]["health"],
                        "circuit": record["pick_counts"]["circuit"],
                        "drain": record["pick_counts"]["drain"],
                    },
                    "samples": samples,
                    "faults": faults,
                    "faultbase": fault_base,
                    "faultstats": enc_fault_stats(record["fault_stats"]),
                    "faulthist": fault_hist,
                    "reasonhist": reason_hist,
                    "retryhist": retry_hist,
                    "alerts": alerts_state,
                }
            )

        # 连接按建连序（dict 保序）；端点快照内联（键恒为连接 cid 子集）。
        connection_states = []
        for cid, connection in connections.items():
            endpoint = conn_endpoints.get(cid)
            connection_states.append(
                {
                    "cid": cid,
                    "b": connection[0],
                    "flow": list(connection[1]),
                    "opened": connection[2],
                    "last": connection[3],
                    "ep": (
                        None if endpoint is None
                        else {"host": endpoint[0], "port": endpoint[1]}
                    ),
                }
            )

        # 粘性映射无业务顺序：按 key 的 UTF-8 字节升序。
        sticky_states = [
            {"key": key, "b": sticky_map[key][0], "e": sticky_map[key][1]}
            for key in sorted(sticky_map, key=lambda k: k.encode("utf-8"))
        ]

        scope_rank = {"B": 0, "C": 1, "S": 2}

        def pair_sort_key(pair):
            return (scope_rank[pair[0]], pair[1].encode("utf-8"))

        bucket_states = [
            {
                "scope": scope,
                "id": bucket_id,
                "r": bucket["r"],
                "b": bucket["b"],
                "t": bucket["t"],
                "at": bucket["at"],
                "last": (
                    None if bucket["last"] is None
                    else [bucket["last"][0], bucket["last"][1],
                          bucket["last"][2]]
                ),
            }
            for (scope, bucket_id), bucket in sorted(
                buckets.items(),
                key=lambda item: pair_sort_key(item[0]),
            )
        ]
        quota_states = [
            {
                "scope": scope,
                "id": quota_id,
                "limit": quota["limit"],
                "span": quota["span"],
                "window": quota["window"],
                "used": quota["used"],
                "last": (
                    None if quota["last"] is None
                    else [quota["last"][0], quota["last"][1],
                          quota["last"][2]]
                ),
            }
            for (scope, quota_id), quota in sorted(
                quotas.items(),
                key=lambda item: pair_sort_key(item[0]),
            )
        ]

        # 等待队列按 FIFO（OrderedDict 保序）。
        wait_states = [
            {
                "cid": item[0],
                "flow": list(item[1]),
                "c": item[2],
                "s": item[3],
                "key": item[4],
                "cost": [item[5], item[6], item[7]],
                "now": item[8],
            }
            for item in wait_queue.values()
        ]

        capacity_states = [
            {"id": backend_id, "cap": cap_overrides[backend_id]}
            for backend_id in backends
            if backend_id in cap_overrides
        ]

        overload_hist_states = [
            {
                "w": window,
                "v": list(row),
            }
            for window, row in sorted(overload_hist.items())
        ]
        wait_hist_states = [
            {
                "w": window,
                "a": list(row["admitted"]),
                "e": list(row["expired"]),
                "c": list(row["cancelled"]),
                "v": list(row["evicted"]),
            }
            for window, row in sorted(wait_hist.items())
        ]
        limit_hist_states = []
        for scope, hist_id in sorted(limit_hist, key=pair_sort_key):
            history = limit_hist[(scope, hist_id)]
            limit_hist_states.append(
                {
                    "scope": scope,
                    "id": hist_id,
                    "rows": [
                        {"w": window, "v": list(history[window])}
                        for window in sorted(history)
                    ],
                }
            )

        # 池级告警：fe/ah 一份；wa 按 kind UTF-8 排序；le 按 (scope,id)
        # UTF-8 排序。
        pool_alerts = {
            "fault": (
                None if alert is None
                else [
                    alert["hi"], alert["lo"], alert["n"], alert["state"],
                    alert["run"], alert["w"], alert["result"]["v"],
                    alert["result"]["changed"],
                ]
            ),
            "fault_events": [
                [event["window"], event["from"], event["to"], event["v"],
                 event["hi"], event["lo"], event["n"]]
                for event in alert_events
            ],
            "wait": [
                {
                    "kind": kind_code,
                    "e": (
                        lambda entry: [
                            entry["p"], entry["hi"], entry["lo"], entry["n"],
                            entry["state"], entry["run"], entry["w"],
                            entry["result"]["samples"],
                            entry["result"]["bucket"],
                            entry["result"]["upper"],
                            entry["result"]["changed"],
                        ]
                    )(wait_alerts[kind_code]),
                }
                for kind_code in sorted(wait_alerts)
            ],
            "limit": [
                {
                    "scope": scope,
                    "id": alert_id,
                    "e": [
                        entry["hi"], entry["lo"], entry["n"], entry["state"],
                        entry["run"], entry["w"],
                        entry["result"]["token"], entry["result"]["quota"],
                        entry["result"]["value"], entry["result"]["changed"],
                    ],
                }
                for (scope, alert_id), entry in sorted(
                    limit_alerts.items(),
                    key=lambda item: pair_sort_key(item[0]),
                )
            ],
        }

        commit_states = [
            {"rev": rev, "config": snapshot}
            for rev, snapshot in commit_history
        ]
        audit_states = []
        for index, event in enumerate(audit_events):
            changes = audit_sections[index]
            audit_states.append(
                {
                    "rev": event["rev"],
                    "now": event["now"],
                    "kind": event["kind"],
                    "section": event["section"],
                    "before": event["before"],
                    "after": event["after"],
                    "changes": [
                        {"section": section, "before": before_digest,
                         "after": after_digest}
                        for section, before_digest, after_digest in changes
                    ],
                }
            )
        reservation_state = None
        if reservation is not None:
            snapshot, at, digest = reservation
            reservation_state = {"at": at, "digest": digest,
                                 "config": snapshot}

        ep_switch_state = None
        if ep_switch is not None:
            switch_items, switch_before, switch_at, switch_digest = ep_switch
            ep_switch_state = {
                "digest": switch_digest,
                "at": switch_at,
                "before": switch_before,
                "items": [
                    {
                        "id": item_id,
                        "base": endpoint_json(base),
                        "target": endpoint_json(target),
                    }
                    for item_id, base, target in switch_items
                ],
            }

        # 端点切换预约生命周期审计：顶层段 ep_audit 固定键序
        # next_seq,events；事件按 seq 升序仅保留最近 64 条，逐项固定键序
        # seq,now,action,before,after，before/after 为 null 或固定键序
        # digest,at 的预约身份。
        ep_audit_state = {
            "next_seq": b["ep_audit_seq"],
            "events": [
                {
                    "seq": seq,
                    "now": event_now,
                    "action": action,
                    "before": ep_identity_json(before),
                    "after": ep_identity_json(after_identity),
                }
                for seq, event_now, action, before, after_identity
                in b["ep_audit_events"]
            ],
        }

        return {
            "now": last_now,
            "vnodes": ring_vnodes,
            "sticky_ttl": sticky_ttl,
            "idle_ttl": ttl_cfg,
            "hard_ttl": hard_ttl_cfg,
            "pick": {"mode": pick_mode, "ticket": rr_ticket},
            "overload": (
                None if queue_cfg is None
                else {"cap": queue_cfg[0], "q": queue_cfg[1],
                      "ttl": queue_cfg[2]}
            ),
            "backpressure": (
                None if bp_cfg is None
                else {"low": bp_cfg[0], "high": bp_cfg[1],
                      "state": bp_state}
            ),
            "queue": {
                "dequeue": dequeue_policy,
                "mode": queue_mode,
                "full": full_mode,
                "evicted": evict_count,
                "last": evict_last,
            },
            "aging": (
                None if aging_cfg is None
                else {
                    "step": aging_cfg[0],
                    "items": [
                        {"s": s_value, "p": aging_cfg[1][s_value]}
                        for s_value in sorted(
                            aging_cfg[1], key=lambda s: s.encode("utf-8")
                        )
                    ],
                }
            ),
            "backends": backend_states,
            "connections": connection_states,
            "sticky": sticky_states,
            "buckets": bucket_states,
            "quotas": quota_states,
            "wait_queue": wait_states,
            "capacities": capacity_states,
            "overload_hist": overload_hist_states,
            "wait_hist": wait_hist_states,
            "limit_hist": limit_hist_states,
            "alerts": pool_alerts,
            "commits": commit_states,
            "next_rev": next_rev,
            "audit": audit_states,
            "reservation": reservation_state,
            "ep_switch": ep_switch_state,
            "ep_audit": ep_audit_state,
            "mo": {
                "seq": mo_seq,
                "cache": (
                    None if mo_cache is None
                    else {"seq": mo_cache[0], "now": mo_cache[1],
                          "result": mo_cache[2]}
                ),
            },
        }

    BUNDLE_KEYS = (
        "last_now", "ring_vnodes", "sticky_ttl", "idle_ttl", "hard_ttl",
        "pick_mode", "rr_ticket", "queue_cfg", "bp_cfg", "bp_state",
        "dequeue_policy", "queue_mode", "full_mode", "evict_count",
        "evict_last", "aging_cfg",
        "backends", "connections", "conn_endpoints", "sticky_map",
        "buckets", "quotas", "wait_queue", "cap_overrides",
        "overload_hist", "wait_hist", "limit_hist", "wait_alerts",
        "alert", "alert_events",
        "err_alerts", "err_events", "percent_alerts", "percent_events",
        "retry_alerts", "retry_events", "conc_alerts", "limit_alerts",
        "unavail_alerts",
        "commit_history", "next_rev", "audit_events", "audit_sections",
        "reservation", "ep_switch", "ep_audit_events", "ep_audit_seq",
        "mo_seq", "mo_cache",
    )

    def current_bundle():
        """引用当前闭包运行态构成一个束（不拷贝）；供 se 规范化。"""
        return {
            "last_now": last_now,
            "ring_vnodes": ring_vnodes,
            "sticky_ttl": sticky_ttl,
            "idle_ttl": ttl_cfg,
            "hard_ttl": hard_ttl_cfg,
            "pick_mode": pick_mode,
            "rr_ticket": rr_ticket,
            "queue_cfg": queue_cfg,
            "bp_cfg": bp_cfg,
            "bp_state": bp_state,
            "dequeue_policy": dequeue_policy,
            "queue_mode": queue_mode,
            "full_mode": full_mode,
            "evict_count": evict_count,
            "evict_last": evict_last,
            "aging_cfg": aging_cfg,
            "backends": backends,
            "connections": connections,
            "conn_endpoints": conn_endpoints,
            "sticky_map": sticky_map,
            "buckets": buckets,
            "quotas": quotas,
            "wait_queue": wait_queue,
            "cap_overrides": cap_overrides,
            "overload_hist": overload_hist,
            "wait_hist": wait_hist,
            "limit_hist": limit_hist,
            "wait_alerts": wait_alerts,
            "alert": alert,
            "alert_events": alert_events,
            "err_alerts": err_alerts,
            "err_events": err_events,
            "percent_alerts": percent_alerts,
            "percent_events": percent_events,
            "retry_alerts": retry_alerts,
            "retry_events": retry_events,
            "conc_alerts": conc_alerts,
            "limit_alerts": limit_alerts,
            "unavail_alerts": unavail_alerts,
            "commit_history": commit_history,
            "next_rev": next_rev,
            "audit_events": audit_events,
            "audit_sections": audit_sections,
            "reservation": reservation,
            "ep_switch": ep_switch,
            "ep_audit_events": ep_audit_events,
            "ep_audit_seq": ep_audit_seq,
            "mo_seq": mo_seq,
            "mo_cache": mo_cache,
        }

    def install_bundle(b):
        """si 全部校验通过后原子替换全部运行态闭包变量。"""
        nonlocal last_now, ring_vnodes, sticky_ttl, ttl_cfg, hard_ttl_cfg
        nonlocal pick_mode, rr_ticket, queue_cfg, bp_cfg, bp_state
        nonlocal dequeue_policy, queue_mode, full_mode, evict_count, evict_last
        nonlocal aging_cfg
        nonlocal backends, connections, conn_endpoints, sticky_map
        nonlocal buckets, quotas, wait_queue, cap_overrides
        nonlocal overload_hist, wait_hist, limit_hist, wait_alerts
        nonlocal alert, alert_events
        nonlocal err_alerts, err_events, percent_alerts, percent_events
        nonlocal retry_alerts, retry_events, conc_alerts, limit_alerts
        nonlocal unavail_alerts
        nonlocal commit_history, next_rev, audit_events, audit_sections
        nonlocal reservation, ep_switch, ep_audit_events, ep_audit_seq
        nonlocal mo_seq, mo_cache
        last_now = b["last_now"]
        ring_vnodes = b["ring_vnodes"]
        sticky_ttl = b["sticky_ttl"]
        ttl_cfg = b["idle_ttl"]
        hard_ttl_cfg = b["hard_ttl"]
        pick_mode = b["pick_mode"]
        rr_ticket = b["rr_ticket"]
        queue_cfg = b["queue_cfg"]
        bp_cfg = b["bp_cfg"]
        bp_state = b["bp_state"]
        dequeue_policy = b["dequeue_policy"]
        queue_mode = b["queue_mode"]
        full_mode = b["full_mode"]
        evict_count = b["evict_count"]
        evict_last = b["evict_last"]
        aging_cfg = b["aging_cfg"]
        backends = b["backends"]
        connections = b["connections"]
        conn_endpoints = b["conn_endpoints"]
        sticky_map = b["sticky_map"]
        buckets = b["buckets"]
        quotas = b["quotas"]
        wait_queue = b["wait_queue"]
        cap_overrides = b["cap_overrides"]
        overload_hist = b["overload_hist"]
        wait_hist = b["wait_hist"]
        limit_hist = b["limit_hist"]
        wait_alerts = b["wait_alerts"]
        alert = b["alert"]
        alert_events = b["alert_events"]
        err_alerts = b["err_alerts"]
        err_events = b["err_events"]
        percent_alerts = b["percent_alerts"]
        percent_events = b["percent_events"]
        retry_alerts = b["retry_alerts"]
        retry_events = b["retry_events"]
        conc_alerts = b["conc_alerts"]
        limit_alerts = b["limit_alerts"]
        unavail_alerts = b["unavail_alerts"]
        commit_history = b["commit_history"]
        next_rev = b["next_rev"]
        audit_events = b["audit_events"]
        audit_sections = b["audit_sections"]
        reservation = b["reservation"]
        ep_switch = b["ep_switch"]
        ep_audit_events = b["ep_audit_events"]
        ep_audit_seq = b["ep_audit_seq"]
        mo_seq = b["mo_seq"]
        mo_cache = b["mo_cache"]

    # ------------------------------------------------------------------
    # si 导入：把规范化 state 严格解析回内部运行态束。
    # 结构/键集/键序/类型/范围/UTF-8 非法 → INPUT/2；重复标识、悬空引用、
    # 矛盾计数或非法状态组合 → STATE/4。调用方须已确认摘要相符。
    def parse_checkpoint(raw):
        if not isinstance(raw, dict):
            fail(EXIT_INPUT, "INPUT")

        def o(value, keys):
            # 对象键集与键序都须与规范化形态逐字一致（键序偏差属 INPUT）。
            if not isinstance(value, dict) or list(value) != list(keys):
                fail(EXIT_INPUT, "INPUT")
            return value

        def ni(value):
            return cp_nonneg(value)

        def ti(value):
            return cp_time(value)

        def si2(value, lo, hi):
            # 指定闭区间的非 bool 整数。
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not lo <= value <= hi
            ):
                fail(EXIT_INPUT, "INPUT")
            return value

        def enum(value, choices):
            if not isinstance(value, str) or value not in choices:
                fail(EXIT_INPUT, "INPUT")
            return value

        def abstime(value):
            # 派生绝对时刻（now+ttl/w/d 等）：now 与登记时长各 ≤10^9，故上
            # 界为 2·10^9；非 bool 整数。
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not 0 <= value <= 2 * 10 ** 9
            ):
                fail(EXIT_INPUT, "INPUT")
            return value

        def ident(value):
            return parse_key(value)

        def alert_chain(events, state):
            """校验滞回告警转换事件链（仅含 60 窗保留窗口，更早事件可能已被
            前端裁剪，故不假设首事件 from=N）：from/to 在 N/A 间交替、相邻
            事件 to==下一 from、窗严格递增、末事件 to 等于当前状态（无事件
            时当前态须为 N）。"""
            previous_window = None
            previous_to = None
            for event in events:
                if event["to"] == event["from"]:
                    cp_state_int()
                window = event["window"]
                if previous_window is not None:
                    if window <= previous_window:
                        cp_state_int()
                    if event["from"] != previous_to:
                        cp_state_int()
                previous_window = window
                previous_to = event["to"]
            if events:
                if events[-1]["to"] != state:
                    cp_state_int()
            elif state == "A":
                cp_state_int()

        def five(value):
            if (
                not isinstance(value, list) or len(value) != 5
                or any(
                    not isinstance(x, int) or isinstance(x, bool)
                    or not 0 <= x <= METRIC_CAP
                    for x in value
                )
            ):
                fail(EXIT_INPUT, "INPUT")
            return [cp_nonneg(x) for x in value]

        def bools(value, limit=None):
            if not isinstance(value, list):
                fail(EXIT_INPUT, "INPUT")
            if limit is not None and len(value) > limit:
                cp_state_int()
            for x in value:
                cp_bool(x)
            return deque(value, maxlen=limit)
        def cfl(value):
            # [count, first, last]：count=0 须 first=last=null；count>0 须
            # 两者为非负整数且 first<=last。
            if not isinstance(value, list) or len(value) != 3:
                fail(EXIT_INPUT, "INPUT")
            count, first, last = value
            si2(count, 0, METRIC_CAP)
            if count == 0:
                if first is not None or last is not None:
                    cp_state_int()
                return [0, None, None]
            if first is None or last is None:
                cp_state_int()
            ti(first)
            ti(last)
            if first > last:
                cp_state_int()
            return [count, first, last]

        root = o(raw, (
            "now", "vnodes", "sticky_ttl", "idle_ttl", "hard_ttl", "pick",
            "overload", "backpressure", "queue", "aging",
            "backends", "connections", "sticky", "buckets", "quotas",
            "wait_queue", "capacities", "overload_hist", "wait_hist",
            "limit_hist", "alerts", "commits", "next_rev", "audit",
            "reservation", "ep_switch", "ep_audit", "mo",
        ))

        last_now = root["now"]
        if last_now is not None:
            ti(last_now)
        ring_vnodes = root["vnodes"]
        if ring_vnodes is not None:
            si2(ring_vnodes, 1, 1024)
        sticky_ttl = root["sticky_ttl"]
        if sticky_ttl is not None:
            si2(sticky_ttl, 1, 10 ** 9)
        idle_ttl = root["idle_ttl"]
        if idle_ttl is not None:
            si2(idle_ttl, 1, 10 ** 9)
        hard_ttl = root["hard_ttl"]
        if hard_ttl is not None:
            si2(hard_ttl, 1, 10 ** 9)
        pick_obj = o(root["pick"], ("mode", "ticket"))
        pick_mode = enum(pick_obj["mode"], ("W", "R", "L", "H"))
        rr_ticket = ni(pick_obj["ticket"])
        if pick_mode == "H" and ring_vnodes is None:
            cp_state_int()

        overload_raw = root["overload"]
        if overload_raw is None:
            queue_cfg = None
        else:
            ov = o(overload_raw, ("cap", "q", "ttl"))
            queue_cfg = (
                si2(ov["cap"], 1, 10 ** 6),
                si2(ov["q"], 1, 10 ** 6),
                si2(ov["ttl"], 1, 10 ** 6),
            )
        bp_raw = root["backpressure"]
        if bp_raw is None:
            bp_cfg = None
            bp_state = "N"
        else:
            bp = o(bp_raw, ("low", "high", "state"))
            low = si2(bp["low"], 0, 10 ** 6)
            high = si2(bp["high"], 0, 10 ** 6)
            bp_state = enum(bp["state"], ("N", "P"))
            if not low < high:
                fail(EXIT_INPUT, "INPUT")
            if queue_cfg is None:
                cp_state_int()
            if high > queue_cfg[1]:
                fail(EXIT_INPUT, "INPUT")
            bp_cfg = (low, high)

        qobj = o(root["queue"], ("dequeue", "mode", "full", "evicted", "last"))
        dequeue_policy = enum(qobj["dequeue"], ("F", "S"))
        queue_mode = enum(qobj["mode"], ("F", "S", "P"))
        full_mode = enum(qobj["full"], ("T", "H"))
        evict_count = si2(qobj["evicted"], 0, METRIC_CAP)
        evict_last = qobj["last"]
        if evict_last is not None:
            ident(evict_last)

        aging_raw = root["aging"]
        if aging_raw is None:
            aging_cfg = None
        else:
            ag = o(aging_raw, ("step", "items"))
            step = si2(ag["step"], 1, 10 ** 9)
            aging_items = cp_list(ag["items"])
            aging_map = {}
            for item in aging_items:
                it = o(item, ("s", "p"))
                s_value = ident(it["s"])
                if s_value in aging_map:
                    cp_state_int()
                aging_map[s_value] = si2(it["p"], 0, 10 ** 9)
            aging_cfg = (step, aging_map)
        if queue_mode == "P" and aging_cfg is None:
            cp_state_int()

        # ---- 后端记录 ----
        backends = OrderedDict()
        err_alerts = {}
        err_events = {}
        percent_alerts = {}
        percent_events = {}
        retry_alerts = {}
        retry_events = {}
        conc_alerts = {}
        unavail_alerts = {}

        def fault_segments(value):
            arr = cp_list(value)
            segments = []
            for seg in arr:
                s = o(seg, ("k", "a", "z", "v"))
                kind = enum(s["k"], ("D", "F", "S"))
                a = ti(s["a"])
                z = ti(s["z"])
                v = ti(s["v"])
                if not a < z:
                    fail(EXIT_INPUT, "INPUT")
                if kind == "D":
                    if v != 0:
                        fail(EXIT_INPUT, "INPUT")
                elif v == 0:
                    fail(EXIT_INPUT, "INPUT")
                if segments and segments[-1][2] > a:
                    # 规范化段须按 a 升序且半开区间不重叠（相邻端点可接）。
                    cp_state_int()
                segments.append((kind, a, z, v))
            return segments

        def fstats(value):
            objv = o(value, ("D", "F", "S"))
            result = {}
            for kind in "DFS":
                arr = objv[kind]
                nums = five(arr)
                result[kind] = {
                    "affected": nums[0], "rejected": nums[1],
                    "retries": nums[2], "remaps": nums[3],
                    "recovered": nums[4],
                }
            return result

        backend_list = cp_list(root["backends"])
        for bentry in backend_list:
            be = o(bentry, (
                "id", "weight", "current", "conns", "healthy", "fail",
                "success", "failures", "successes", "probe", "warm",
                "circuit", "drain", "since", "lastop", "endpoint",
                "metrics", "mobase", "pickcounts", "samples", "faults",
                "faultbase", "faultstats", "faulthist", "reasonhist",
                "retryhist", "alerts",
            ))
            backend_id = parse_backend_id(be["id"])
            if backend_id in backends:
                cp_state_int()
            weight = si2(be["weight"], 1, 100)
            current = ni(be["current"])
            conns = ni(be["conns"])
            healthy = cp_bool(be["healthy"])
            fail_t = si2(be["fail"], 1, 100)
            succ_t = si2(be["success"], 1, 100)
            failures = si2(be["failures"], 0, fail_t)
            successes = si2(be["successes"], 0, succ_t)
            probe_raw = be["probe"]
            if probe_raw is None:
                probe_now = probe_ok = None
            else:
                pr = o(probe_raw, ("t", "ok"))
                probe_now = ti(pr["t"])
                probe_ok = cp_bool(pr["ok"])

            warm = o(be["warm"], ("stage", "d", "from", "start", "end"))
            stage = enum(warm["stage"], ("warm", "steady"))
            warm_d = si2(warm["d"], 0, 10 ** 9)
            warm_from = warm["from"]
            warm_start = warm["start"]
            warm_end = warm["end"]
            if stage == "warm":
                if warm_d == 0 or warm_from is None or warm_start is None \
                        or warm_end is None:
                    cp_state_int()
                ni(warm_from)
                ti(warm_start)
                abstime(warm_end)
                if not warm_start < warm_end or warm_end - warm_start != warm_d:
                    cp_state_int()
            else:
                if warm_from is not None or warm_start is not None \
                        or warm_end is not None:
                    cp_state_int()

            circuit_raw = be["circuit"]
            if circuit_raw is None:
                circuit = None
            else:
                c = o(circuit_raw, (
                    "n", "m", "r", "w", "q", "state", "win", "next",
                    "used", "cr",
                ))
                n = si2(c["n"], 1, 100)
                m = si2(c["m"], 1, n)
                r = si2(c["r"], 1, 100)
                w = si2(c["w"], 1, 10 ** 9)
                q = si2(c["q"], 1, 100)
                cstate = enum(c["state"], ("C", "O", "H"))
                window = bools(c["win"], n)
                cnext = c["next"]
                used = ni(c["used"])
                if cstate == "O":
                    if cnext is None or not (isinstance(cnext, int)
                                             and not isinstance(cnext, bool)):
                        cp_state_int()
                    abstime(cnext)
                    if used != 0:
                        cp_state_int()
                else:
                    if cnext is not None:
                        cp_state_int()
                if cstate == "H":
                    if not 0 <= used <= q:
                        cp_state_int()
                elif used != 0:
                    cp_state_int()
                cr_raw = c["cr"]
                cr_now = cr_ok = None
                if cr_raw is not None:
                    cr = o(cr_raw, ("t", "ok"))
                    cr_now = ti(cr["t"])
                    cr_ok = cp_bool(cr["ok"])
                circuit = {
                    "params": (n, m, r, w, q),
                    "state": cstate,
                    "window": window,
                    "next": cnext,
                    "used": used,
                    "cr_now": cr_now,
                    "cr_ok": cr_ok,
                }

            drain_raw = o(be["drain"], (
                "t", "state", "start", "end", "deadline", "forced",
            ))
            drain_t = drain_raw["t"]
            if drain_t is not None:
                si2(drain_t, 1, 10 ** 9)
            dstate = enum(drain_raw["state"], ("A", "D", "X"))
            dstart = drain_raw["start"]
            dend = drain_raw["end"]
            ddeadline = drain_raw["deadline"]
            dforced = ni(drain_raw["forced"])
            for x in (dstart, dend):
                if x is not None:
                    ti(x)
            if ddeadline is not None:
                abstime(ddeadline)
            if dstate == "A":
                if dstart is not None or dend is not None \
                        or ddeadline is not None or dforced != 0:
                    cp_state_int()
            elif dstate == "D":
                if drain_t is None or dstart is None or ddeadline is None \
                        or dend is not None or dforced != 0:
                    cp_state_int()
            else:  # X
                if drain_t is None or dstart is None or dend is None \
                        or ddeadline is None:
                    cp_state_int()

            since_raw = o(be["since"], ("health", "drain", "circuit"))
            since_health = since_raw["health"]
            since_drain = since_raw["drain"]
            since_circuit = since_raw["circuit"]
            for x in (since_health, since_drain, since_circuit):
                if x is not None:
                    ti(x)
            # ru 不变量：health 起点 ⇔ unhealthy；drain 起点 ⇔ D/X；
            # circuit 起点 ⇔ O/H（未配熔断器须无起点）。矛盾即非法状态组合。
            if (since_health is None) != healthy:
                cp_state_int()
            if (since_drain is not None) != (dstate in ("D", "X")):
                cp_state_int()
            if circuit is not None:
                if (since_circuit is not None) != (
                    circuit["state"] in ("O", "H")
                ):
                    cp_state_int()
            elif since_circuit is not None:
                cp_state_int()

            lastop_raw = be["lastop"]
            if lastop_raw is None:
                last_op = None
            else:
                lo = o(lastop_raw, ("k", "w", "d", "t"))
                lok = enum(lo["k"], ("add3", "add5", "ws"))
                low_ = si2(lo["w"], 1, 100)
                if lok == "add3":
                    if lo["d"] is not None or lo["t"] is not None:
                        cp_state_int()
                    last_op = ("add3", low_)
                else:
                    lod = ti(lo["d"])
                    lot = ti(lo["t"])
                    last_op = (lok, low_, lod, lot)

            endpoint_raw = be["endpoint"]
            if endpoint_raw is None:
                endpoint = None
            else:
                ep = o(endpoint_raw, ("host", "port"))
                endpoint = (
                    parse_endpoint_host(ep["host"]),
                    parse_endpoint_port(ep["port"]),
                )

            # 分钟度量历史。
            metrics = {}
            for wentry in cp_list(be["metrics"]):
                wv = o(wentry, ("w", "v"))
                window = ti(wv["w"])
                vals = cp_list(wv["v"])
                if len(vals) != 5:
                    fail(EXIT_INPUT, "INPUT")
                req = si2(vals[0], 0, METRIC_CAP)
                errc = si2(vals[1], 0, METRIC_CAP)
                retries = si2(vals[2], 0, METRIC_CAP)
                remaps = si2(vals[3], 0, METRIC_CAP)
                lat = five(vals[4])
                # errors 不可能超过 requests（每条 mr 至多一个 error）。
                if errc > req:
                    cp_state_int()
                if window in metrics:
                    cp_state_int()
                metrics[window] = [req, errc, retries, remaps, lat]

            mobase_raw = o(be["mobase"], ("w", "v"))
            mbw = mobase_raw["w"]
            if mbw is not None:
                ti(mbw)
            mbvals = cp_list(mobase_raw["v"])
            if len(mbvals) != 5:
                fail(EXIT_INPUT, "INPUT")
            mo_base = [
                mbw,
                si2(mbvals[0], 0, METRIC_CAP), si2(mbvals[1], 0, METRIC_CAP),
                si2(mbvals[2], 0, METRIC_CAP), si2(mbvals[3], 0, METRIC_CAP),
                five(mbvals[4]),
            ]

            pc_raw = o(be["pickcounts"], (
                "total", "first", "sticky", "expired", "removed",
                "health", "circuit", "drain",
            ))
            pick_counts = {
                key: si2(pc_raw[key], 0, METRIC_CAP)
                for key in (
                    "total", "first", "sticky", "expired", "removed",
                    "health", "circuit", "drain",
                )
            }

            samples = {}
            for sentry in cp_list(be["samples"]):
                sv = o(sentry, ("w", "samples", "peak", "points"))
                swindow = ti(sv["w"])
                scount = ni(sv["samples"])
                speak = ni(sv["peak"])
                points = {}
                for p in cp_list(sv["points"]):
                    pv = o(p, ("t", "c", "r"))
                    pt = ti(pv["t"])
                    pc = ni(pv["c"])
                    reason = pv["r"]
                    if reason is not None:
                        enum(reason, ("drain", "health", "circuit", "fault"))
                    # ms 按 now//60 分窗：采样时刻必须落在所属窗。
                    if pt // 60 != swindow:
                        cp_state_int()
                    if pt in points:
                        cp_state_int()
                    points[pt] = (pc, reason)
                if scount != len(points):
                    cp_state_int()
                expected_peak = 0
                for _, (pc, _) in points.items():
                    if pc > expected_peak:
                        expected_peak = pc
                if speak != expected_peak:
                    cp_state_int()
                if swindow in samples:
                    cp_state_int()
                samples[swindow] = {
                    "points": points, "samples": scount, "peak": speak,
                }

            faults = fault_segments(be["faults"])
            fault_a = [seg[1] for seg in faults]
            fault_base = set()
            for fbentry in cp_list(be["faultbase"]):
                seg = fault_segments([fbentry])[0]
                if seg not in faults:
                    cp_state_int()
                fault_base.add(seg)
            fault_stats = fstats(be["faultstats"])

            fault_hist = {}
            for fentry in cp_list(be["faulthist"]):
                fv = o(fentry, ("w", "v"))
                fwindow = ti(fv["w"])
                if fwindow in fault_hist:
                    cp_state_int()
                fault_hist[fwindow] = fstats(fv["v"])

            reason_hist = {}
            for rentry in cp_list(be["reasonhist"]):
                rv = o(rentry, (
                    "w", "health", "drain", "circuit", "overload",
                ))
                rwindow = ti(rv["w"])
                if rwindow in reason_hist:
                    cp_state_int()
                reason_hist[rwindow] = {
                    name: cfl(rv[name])
                    for name in ("health", "drain", "circuit", "overload")
                }

            retry_hist = {}
            for xentry in cp_list(be["retryhist"]):
                xv = o(xentry, ("w", "retries", "remaps"))
                xwindow = ti(xv["w"])
                if xwindow in retry_hist:
                    cp_state_int()
                retry_hist[xwindow] = {
                    "retries": cfl(xv["retries"]),
                    "remaps": cfl(xv["remaps"]),
                }

            # ---- 每后端告警 ----
            al = o(be["alerts"], (
                "err", "err_events", "pa", "pa_events", "xa", "xa_events",
                "na", "ua",
            ))

            def rate_str(value):
                if (
                    not isinstance(value, str)
                    or len(value) < 4
                    or value[-3] != "."
                ):
                    fail(EXIT_INPUT, "INPUT")
                head, frac = value[:-3], value[-2:]
                if not head.isdigit() or not frac.isdigit():
                    fail(EXIT_INPUT, "INPUT")
                return value

            err_entry = None
            if al["err"] is not None:
                arr = cp_list(al["err"])
                if len(arr) != 10:
                    fail(EXIT_INPUT, "INPUT")
                hi = si2(arr[0], 1, 10000)
                lo = si2(arr[1], 0, 9999)
                n = si2(arr[2], 1, 60)
                if not lo < hi:
                    fail(EXIT_INPUT, "INPUT")
                estate = enum(arr[3], ("N", "A"))
                run = ni(arr[4])
                ew = ti(arr[5])
                requests = ni(arr[6])
                errors = ni(arr[7])
                if errors > requests:
                    cp_state_int()
                rate_str(arr[8])
                cp_bool(arr[9])
                err_entry = {
                    "hi": hi, "lo": lo, "n": n, "state": estate,
                    "run": run, "w": ew,
                    "result": {
                        "op": "ea", "id": backend_id, "w": ew,
                        "state": estate, "requests": requests,
                        "errors": errors, "rate": arr[8], "run": run,
                        "changed": arr[9],
                    },
                }
            err_alerts[backend_id] = err_entry
            ehist = deque()
            for ev in cp_list(al["err_events"]):
                vals = cp_list(ev)
                if len(vals) != 7:
                    fail(EXIT_INPUT, "INPUT")
                ehist.append({
                    "window": ti(vals[0]),
                    "from": enum(vals[1], ("N", "A")),
                    "to": enum(vals[2], ("N", "A")),
                    "rate": rate_str(vals[3]),
                    "hi": si2(vals[4], 1, 10000),
                    "lo": si2(vals[5], 0, 9999),
                    "n": si2(vals[6], 1, 60),
                })
            if ehist:
                err_events[backend_id] = ehist
            if err_entry is not None:
                alert_chain(ehist, err_entry["state"])

            pa_entry = None
            if al["pa"] is not None:
                arr = cp_list(al["pa"])
                if len(arr) != 11:
                    fail(EXIT_INPUT, "INPUT")
                p = si2(arr[0], 1, 100)
                hi = si2(arr[1], 0, 4)
                lo = si2(arr[2], 0, 4)
                n = si2(arr[3], 1, 60)
                if not lo < hi:
                    fail(EXIT_INPUT, "INPUT")
                pastate = enum(arr[4], ("N", "A"))
                run = ni(arr[5])
                pw = ti(arr[6])
                samples_v = ni(arr[7])
                bucket = arr[8]
                if bucket is not None:
                    si2(bucket, 0, 4)
                upper = arr[9]
                if upper is not None and upper not in (1, 10, 100, 1000):
                    fail(EXIT_INPUT, "INPUT")
                cp_bool(arr[10])
                pa_entry = {
                    "p": p, "hi": hi, "lo": lo, "n": n, "state": pastate,
                    "run": run, "w": pw,
                    "result": {
                        "op": "pa", "id": backend_id, "w": pw, "p": p,
                        "state": pastate, "samples": samples_v,
                        "bucket": bucket, "upper": upper, "run": run,
                        "changed": arr[10],
                    },
                }
            percent_alerts[backend_id] = pa_entry
            phist = deque()
            for ev in cp_list(al["pa_events"]):
                vals = cp_list(ev)
                if len(vals) != 10:
                    fail(EXIT_INPUT, "INPUT")
                pbucket = vals[5]
                if pbucket is not None:
                    si2(pbucket, 0, 4)
                pupper = vals[6]
                if pupper is not None and pupper not in (1, 10, 100, 1000):
                    fail(EXIT_INPUT, "INPUT")
                phist.append({
                    "window": ti(vals[0]),
                    "from": enum(vals[1], ("N", "A")),
                    "to": enum(vals[2], ("N", "A")),
                    "p": si2(vals[3], 1, 100),
                    "samples": ni(vals[4]),
                    "bucket": pbucket,
                    "upper": pupper,
                    "hi": si2(vals[7], 0, 4),
                    "lo": si2(vals[8], 0, 4),
                    "n": si2(vals[9], 1, 60),
                })
            if phist:
                percent_events[backend_id] = phist
            if pa_entry is not None:
                alert_chain(phist, pa_entry["state"])

            xa_obj = o(al["xa"], ("R", "M"))
            xae_obj = o(al["xa_events"], ("R", "M"))
            for kcode in ("R", "M"):
                if xa_obj[kcode] is not None:
                    arr = cp_list(xa_obj[kcode])
                    if len(arr) != 8:
                        fail(EXIT_INPUT, "INPUT")
                    hi = si2(arr[0], 1, METRIC_CAP)
                    lo = si2(arr[1], 0, METRIC_CAP - 1)
                    n = si2(arr[2], 1, 60)
                    if not lo < hi:
                        fail(EXIT_INPUT, "INPUT")
                    xstate = enum(arr[3], ("N", "A"))
                    run = ni(arr[4])
                    xw = ti(arr[5])
                    value = ni(arr[6])
                    cp_bool(arr[7])
                    retry_alerts[(backend_id, kcode)] = {
                        "hi": hi, "lo": lo, "n": n, "state": xstate,
                        "run": run, "w": xw,
                        "result": {
                            "op": "xa", "id": backend_id, "k": kcode,
                            "w": xw, "state": xstate, "value": value,
                            "run": run, "changed": arr[7],
                        },
                    }
                xhist = deque()
                for ev in cp_list(xae_obj[kcode]):
                    vals = cp_list(ev)
                    if len(vals) != 7:
                        fail(EXIT_INPUT, "INPUT")
                    xhist.append({
                        "window": ti(vals[0]),
                        "from": enum(vals[1], ("N", "A")),
                        "to": enum(vals[2], ("N", "A")),
                        "value": ni(vals[3]),
                        "hi": si2(vals[4], 1, METRIC_CAP),
                        "lo": si2(vals[5], 0, METRIC_CAP - 1),
                        "n": si2(vals[6], 1, 60),
                    })
                xentry = retry_alerts.get((backend_id, kcode))
                if xhist:
                    retry_events[(backend_id, kcode)] = xhist
                if xentry is not None:
                    alert_chain(xhist, xentry["state"])

            na_entry = None
            if al["na"] is not None:
                arr = cp_list(al["na"])
                if len(arr) != 9:
                    fail(EXIT_INPUT, "INPUT")
                hi = si2(arr[0], 1, 10 ** 9)
                lo = si2(arr[1], 0, 10 ** 9 - 1)
                n = si2(arr[2], 1, 60)
                if not lo < hi:
                    fail(EXIT_INPUT, "INPUT")
                nastate = enum(arr[3], ("N", "A"))
                run = ni(arr[4])
                naw = ti(arr[5])
                nasamples = ni(arr[6])
                napeak = ni(arr[7])
                cp_bool(arr[8])
                na_entry = {
                    "hi": hi, "lo": lo, "n": n, "state": nastate,
                    "run": run, "w": naw,
                    "result": {
                        "op": "na", "id": backend_id, "w": naw,
                        "state": nastate, "samples": nasamples,
                        "peak": napeak, "run": run, "changed": arr[8],
                    },
                }
            conc_alerts[backend_id] = na_entry

            ua_obj = o(al["ua"], ("drain", "health", "circuit", "fault"))
            for reason in ("drain", "health", "circuit", "fault"):
                if ua_obj[reason] is not None:
                    arr = cp_list(ua_obj[reason])
                    if len(arr) != 7:
                        fail(EXIT_INPUT, "INPUT")
                    threshold = si2(arr[0], 1, 10 ** 9)
                    ustate = enum(arr[1], ("N", "A"))
                    unow = ti(arr[2])
                    active = cp_bool(arr[3])
                    since = arr[4]
                    duration = ni(arr[5])
                    cp_bool(arr[6])
                    if active:
                        if since is None:
                            cp_state_int()
                        ti(since)
                        if duration != unow - since:
                            cp_state_int()
                    else:
                        if since is not None or duration != 0:
                            cp_state_int()
                    unavail_alerts[(backend_id, reason)] = {
                        "threshold": threshold, "state": ustate,
                        "now": unow,
                        "result": {
                            "op": "ua", "id": backend_id, "reason": reason,
                            "threshold": threshold, "state": ustate,
                            "active": active, "since": since,
                            "duration": duration, "changed": arr[6],
                        },
                    }

            backends[backend_id] = {
                "weight": weight,
                "current": current,
                "conns": conns,
                "healthy": healthy,
                "fail": fail_t,
                "success": succ_t,
                "failures": failures,
                "successes": successes,
                "probe_now": probe_now,
                "probe_ok": probe_ok,
                "stage": stage,
                "warm_d": warm_d,
                "warm_from": warm_from,
                "warm_start": warm_start,
                "warm_end": warm_end,
                "circuit": circuit,
                "drain": {
                    "t": drain_t, "state": dstate, "start": dstart,
                    "end": dend, "deadline": ddeadline, "forced": dforced,
                },
                "unavail_since": {
                    "health": since_health, "drain": since_drain,
                    "circuit": since_circuit,
                },
                "last_op": last_op,
                "endpoint": endpoint,
                "metrics": metrics,
                "mo_base": mo_base,
                "pick_counts": pick_counts,
                "samples": samples,
                "faults": faults,
                "fault_a": fault_a,
                "fault_stats": fault_stats,
                "fault_base": fault_base,
                "fault_hist": fault_hist,
                "reason_hist": reason_hist,
                "retry_hist": retry_hist,
            }

        # ---- 连接（建连序）与端点快照 ----
        connections = OrderedDict()
        conn_endpoints = {}
        conn_count = {}
        for centry in cp_list(root["connections"]):
            ce = o(centry, ("cid", "b", "flow", "opened", "last", "ep"))
            cid = parse_cid(ce["cid"])
            if cid in connections:
                cp_state_int()
            target = parse_backend_id(ce["b"])
            if target not in backends:
                cp_state_int()
            flow = parse_flow(ce["flow"])
            opened = ti(ce["opened"])
            lastv = ti(ce["last"])
            if lastv < opened:
                cp_state_int()
            connections[cid] = [target, flow, opened, lastv]
            conn_count[target] = conn_count.get(target, 0) + 1
            ep_raw = ce["ep"]
            if ep_raw is not None:
                ep = o(ep_raw, ("host", "port"))
                conn_endpoints[cid] = (
                    parse_endpoint_host(ep["host"]),
                    parse_endpoint_port(ep["port"]),
                )
        # 矛盾计数：每后端 conns 须等于引用它的活动连接数；D 态须有连接、
        # X 态须无连接。
        for backend_id, record in backends.items():
            actual = conn_count.get(backend_id, 0)
            if record["conns"] != actual:
                cp_state_int()
            dstate = record["drain"]["state"]
            if dstate == "D" and actual == 0:
                cp_state_int()
            if dstate == "X" and actual != 0:
                cp_state_int()
            # mo 增量基线须不超过同窗 mr 累计（基线是该窗某时刻的累计快照，
            # 各计数与延迟桶逐项 ≤ 现值）；跨窗基线仅用于被判定为零，无约束。
            mbw = record["mo_base"][0]
            if mbw is not None and mbw in record["metrics"]:
                row = record["metrics"][mbw]
                base_counts = record["mo_base"]
                # mo_base 布局 [w, requests, errors, retries, remaps, 五桶]，
                # metrics 布局 [requests, errors, retries, remaps, 五桶]。
                if any(base_counts[i + 1] > row[i] for i in range(4)):
                    cp_state_int()
                if any(
                    base_counts[5][i] > row[4][i] for i in range(5)
                ):
                    cp_state_int()

        # ---- 粘性映射（按 key UTF-8 排序）----
        sticky_map = {}
        prev_key = None
        for sentry in cp_list(root["sticky"]):
            sk = o(sentry, ("key", "b", "e"))
            key = parse_key(sk["key"])
            target = parse_backend_id(sk["b"])
            if target not in backends:
                cp_state_int()
            expires = sk["e"]
            if expires is not None:
                ni(expires)
            if key in sticky_map:
                cp_state_int()
            encoded = key.encode("utf-8")
            if prev_key is not None and not prev_key < encoded:
                fail(EXIT_INPUT, "INPUT")
            prev_key = encoded
            sticky_map[key] = [target, expires]

        scope_rank = {"B": 0, "C": 1, "S": 2}

        def check_pair_order(items):
            previous = None
            for scope, identv in items:
                key = (scope_rank[scope], identv.encode("utf-8"))
                if previous is not None and not previous < key:
                    fail(EXIT_INPUT, "INPUT")
                previous = key

        # ---- 令牌桶 ----
        buckets = {}
        pair_order = []
        for bentry in cp_list(root["buckets"]):
            bk = o(bentry, ("scope", "id", "r", "b", "t", "at", "last"))
            scope = enum(bk["scope"], ("B", "C", "S"))
            bucket_id = ident(bk["id"])
            r = si2(bk["r"], 1, 10 ** 9)
            bcap = si2(bk["b"], 1, 10 ** 9)
            t = ni(bk["t"])
            at = ti(bk["at"])
            if t > bcap:
                cp_state_int()
            lastv = bk["last"]
            if lastv is not None:
                lvals = cp_list(lastv)
                if len(lvals) != 3 or lvals[0] != r or lvals[1] != bcap:
                    cp_state_int()
                ti(lvals[2])
                lastv = (r, bcap, lvals[2])
            if scope == "B" and bucket_id not in backends:
                cp_state_int()
            pair = (scope, bucket_id)
            if pair in buckets:
                cp_state_int()
            buckets[pair] = {
                "r": r, "b": bcap, "t": t, "at": at, "last": lastv,
            }
            pair_order.append(pair)
        check_pair_order(pair_order)

        # ---- 固定窗口配额 ----
        quotas = {}
        pair_order = []
        for qentry in cp_list(root["quotas"]):
            qk = o(qentry, (
                "scope", "id", "limit", "span", "window", "used", "last",
            ))
            scope = enum(qk["scope"], ("B", "C", "S"))
            quota_id = ident(qk["id"])
            limit = si2(qk["limit"], 1, 10 ** 18)
            span = si2(qk["span"], 1, 10 ** 9)
            window = ni(qk["window"])
            used = ni(qk["used"])
            if used > limit:
                cp_state_int()
            lastv = qk["last"]
            if lastv is not None:
                lvals = cp_list(lastv)
                if len(lvals) != 3 or lvals[0] != limit or lvals[1] != span:
                    cp_state_int()
                ti(lvals[2])
                lastv = (limit, span, lvals[2])
            if scope == "B" and quota_id not in backends:
                cp_state_int()
            pair = (scope, quota_id)
            if pair in quotas:
                cp_state_int()
            quotas[pair] = {
                "limit": limit, "span": span, "window": window,
                "used": used, "last": lastv,
            }
            pair_order.append(pair)
        check_pair_order(pair_order)

        # ---- 等待队列（FIFO）----
        wait_queue = OrderedDict()
        for wq in cp_list(root["wait_queue"]):
            item = o(wq, ("cid", "flow", "c", "s", "key", "cost", "now"))
            cid = parse_cid(item["cid"])
            if cid in wait_queue or cid in connections:
                cp_state_int()
            flow = parse_flow(item["flow"])
            cval = ident(item["c"])
            sval = ident(item["s"])
            key = parse_key(item["key"])
            costs = cp_list(item["cost"])
            if len(costs) != 3:
                fail(EXIT_INPUT, "INPUT")
            bc, cc, sc = (si2(costs[i], 0, 10 ** 9) for i in range(3))
            if bc == 0 and cc == 0 and sc == 0:
                fail(EXIT_INPUT, "INPUT")
            enqueue_now = ti(item["now"])
            wait_queue[cid] = (
                cid, flow, cval, sval, key, bc, cc, sc, enqueue_now,
            )
        if queue_cfg is not None and len(wait_queue) > queue_cfg[1]:
            cp_state_int()
        if queue_cfg is None and wait_queue:
            cp_state_int()

        # ---- 接纳容量覆盖（按后端加入序）----
        cap_overrides = {}
        prev_index = -1
        backend_order = {bid: i for i, bid in enumerate(backends)}
        for centry in cp_list(root["capacities"]):
            ck = o(centry, ("id", "cap"))
            cap_id = parse_backend_id(ck["id"])
            if cap_id not in backends:
                cp_state_int()
            cap = si2(ck["cap"], 1, 10 ** 6)
            if cap_id in cap_overrides:
                cp_state_int()
            index = backend_order[cap_id]
            if index <= prev_index:
                fail(EXIT_INPUT, "INPUT")
            prev_index = index
            cap_overrides[cap_id] = cap

        # ---- 全池分钟历史 ----
        overload_hist = {}
        for oentry in cp_list(root["overload_hist"]):
            ov = o(oentry, ("w", "v"))
            window = ti(ov["w"])
            vals = cp_list(ov["v"])
            if len(vals) != 5:
                fail(EXIT_INPUT, "INPUT")
            vals = [si2(x, 0, METRIC_CAP) for x in vals]
            if window in overload_hist:
                cp_state_int()
            overload_hist[window] = vals

        wait_hist = {}
        for wentry in cp_list(root["wait_hist"]):
            wh = o(wentry, ("w", "a", "e", "c", "v"))
            window = ti(wh["w"])
            row = {
                "admitted": five(wh["a"]),
                "expired": five(wh["e"]),
                "cancelled": five(wh["c"]),
                "evicted": five(wh["v"]),
            }
            if window in wait_hist:
                cp_state_int()
            wait_hist[window] = row

        limit_hist = {}
        pair_order = []
        for lentry in cp_list(root["limit_hist"]):
            lk = o(lentry, ("scope", "id", "rows"))
            scope = enum(lk["scope"], ("B", "C", "S"))
            hist_id = ident(lk["id"])
            if scope == "B" and hist_id not in backends:
                cp_state_int()
            history = {}
            for row_entry in cp_list(lk["rows"]):
                rw = o(row_entry, ("w", "v"))
                window = ti(rw["w"])
                vals = cp_list(rw["v"])
                if len(vals) != 4:
                    fail(EXIT_INPUT, "INPUT")
                vals = [si2(x, 0, METRIC_CAP) for x in vals]
                if window in history:
                    cp_state_int()
                history[window] = vals
            pair = (scope, hist_id)
            if pair in limit_hist:
                cp_state_int()
            limit_hist[pair] = history
            pair_order.append(pair)
        check_pair_order(pair_order)

        # ---- 池级告警 ----
        alerts = o(root["alerts"], (
            "fault", "fault_events", "wait", "limit",
        ))
        if alerts["fault"] is None:
            pool_alert = None
        else:
            arr = cp_list(alerts["fault"])
            if len(arr) != 8:
                fail(EXIT_INPUT, "INPUT")
            hi = si2(arr[0], 1, METRIC_CAP)
            lo = si2(arr[1], 0, METRIC_CAP - 1)
            n = si2(arr[2], 1, 60)
            if not lo < hi:
                fail(EXIT_INPUT, "INPUT")
            astate = enum(arr[3], ("N", "A"))
            run = ni(arr[4])
            aw = ti(arr[5])
            vval = si2(arr[6], 0, METRIC_CAP)
            cp_bool(arr[7])
            pool_alert = {
                "hi": hi, "lo": lo, "n": n, "state": astate,
                "run": run, "w": aw,
                "result": {
                    "op": "fe", "w": aw, "state": astate, "v": vval,
                    "run": run, "changed": arr[7],
                },
            }
        alert_events = deque()
        for ev in cp_list(alerts["fault_events"]):
            vals = cp_list(ev)
            if len(vals) != 7:
                fail(EXIT_INPUT, "INPUT")
            alert_events.append({
                "window": ti(vals[0]),
                "from": enum(vals[1], ("N", "A")),
                "to": enum(vals[2], ("N", "A")),
                "v": si2(vals[3], 0, METRIC_CAP),
                "hi": si2(vals[4], 1, METRIC_CAP),
                "lo": si2(vals[5], 0, METRIC_CAP - 1),
                "n": si2(vals[6], 1, 60),
            })
        if pool_alert is not None:
            alert_chain(alert_events, pool_alert["state"])

        wait_alerts = {}
        prev_kind = None
        for wentry in cp_list(alerts["wait"]):
            wk = o(wentry, ("kind", "e"))
            kind = enum(wk["kind"], ("A", "E", "C", "V"))
            if prev_kind is not None and not prev_kind < kind:
                fail(EXIT_INPUT, "INPUT")
            prev_kind = kind
            arr = cp_list(wk["e"])
            if len(arr) != 11:
                fail(EXIT_INPUT, "INPUT")
            p = si2(arr[0], 1, 100)
            hi = si2(arr[1], 0, 4)
            lo = si2(arr[2], 0, 4)
            n = si2(arr[3], 1, 60)
            if not lo < hi:
                fail(EXIT_INPUT, "INPUT")
            wstate = enum(arr[4], ("N", "A"))
            run = ni(arr[5])
            ww = ti(arr[6])
            samples_v = ni(arr[7])
            bucket = arr[8]
            if bucket is not None:
                si2(bucket, 0, 4)
            upper = arr[9]
            if upper is not None and upper not in (0, 1, 10, 100):
                fail(EXIT_INPUT, "INPUT")
            cp_bool(arr[10])
            wait_alerts[kind] = {
                "p": p, "hi": hi, "lo": lo, "n": n, "state": wstate,
                "run": run, "w": ww,
                "result": {
                    "op": "wa", "kind": kind, "w": ww, "p": p,
                    "state": wstate, "samples": samples_v,
                    "bucket": bucket, "upper": upper, "run": run,
                    "changed": arr[10],
                },
            }

        limit_alerts = {}
        pair_order = []
        for lentry in cp_list(alerts["limit"]):
            lk = o(lentry, ("scope", "id", "e"))
            scope = enum(lk["scope"], ("B", "C", "S"))
            alert_id = ident(lk["id"])
            if scope == "B" and alert_id not in backends:
                cp_state_int()
            arr = cp_list(lk["e"])
            if len(arr) != 10:
                fail(EXIT_INPUT, "INPUT")
            hi = si2(arr[0], 1, METRIC_CAP)
            lo = si2(arr[1], 0, METRIC_CAP - 1)
            n = si2(arr[2], 1, 60)
            if not lo < hi:
                fail(EXIT_INPUT, "INPUT")
            lstate = enum(arr[3], ("N", "A"))
            run = ni(arr[4])
            lw = ti(arr[5])
            token = si2(arr[6], 0, METRIC_CAP)
            quota = si2(arr[7], 0, METRIC_CAP)
            value = si2(arr[8], 0, METRIC_CAP)
            cp_bool(arr[9])
            pair = (scope, alert_id)
            if pair in limit_alerts:
                cp_state_int()
            limit_alerts[pair] = {
                "hi": hi, "lo": lo, "n": n, "state": lstate,
                "run": run, "w": lw,
                "result": {
                    "op": "le", "scope": scope, "id": alert_id, "w": lw,
                    "state": lstate, "token": token, "quota": quota,
                    "value": value, "run": run, "changed": arr[9],
                },
            }
            pair_order.append(pair)
        check_pair_order(pair_order)

        # ---- 提交历史（rev 升序，至多 16）与审计（至多 64）----
        def snapshot_config(value):
            objv = o(value, (
                "version", "backends", "vnodes", "limits", "overload",
                "sticky", "idle", "backpressure", "scheduler", "faults",
                "quotas", "queue", "capacities", "lifetime",
            ))
            if objv["version"] != 11:
                fail(EXIT_INPUT, "INPUT")
            # 复用 ci 同款全量校验（结构/键序/类型/范围/排序/交叉约束）。
            parsed = parse_config(objv)
            ids = {entry[0] for entry in parsed["backends"]}
            for scope, bucket_id, _, _ in parsed["limits"]:
                if scope == "B" and bucket_id not in ids:
                    cp_state_int()
            for scope, quota_id, _, _ in parsed["quotas"]:
                if scope == "B" and quota_id not in ids:
                    cp_state_int()
            for fault_id in parsed["faults"]:
                if fault_id not in ids:
                    cp_state_int()
            for override_id in parsed["capacities"]:
                if override_id not in ids:
                    cp_state_int()
            # 规范化回显须与快照逐字节同构（整体 canonical 比对再次保证）。
            if export_normalized_config(parsed) != objv:
                fail(EXIT_INPUT, "INPUT")
            return objv

        commit_history = []
        for centry in cp_list(root["commits"]):
            ck = o(centry, ("rev", "config"))
            rev = si2(ck["rev"], 1, 10 ** 18)
            if commit_history and rev <= commit_history[-1][0]:
                cp_state_int()
            commit_history.append((rev, snapshot_config(ck["config"])))
        if len(commit_history) > 16:
            cp_state_int()

        next_rev = si2(root["next_rev"], 1, 10 ** 18 + 1)
        for rev, _ in commit_history:
            if rev >= next_rev:
                cp_state_int()

        audit_events = deque(maxlen=64)
        audit_sections = deque(maxlen=64)
        for aentry in cp_list(root["audit"]):
            ak = o(aentry, (
                "rev", "now", "kind", "section", "before", "after",
                "changes",
            ))
            rev = si2(ak["rev"], 1, 10 ** 18)
            if audit_events and rev <= audit_events[-1]["rev"]:
                cp_state_int()
            if rev >= next_rev:
                cp_state_int()
            anow = ti(ak["now"])
            kind = enum(ak["kind"], ("ci", "cb", "cu", "ca"))
            section = ak["section"]
            if section is not None:
                enum(section, CU_SECTIONS)
                if kind != "cu":
                    cp_state_int()
            elif kind == "cu":
                cp_state_int()
            before_d = cp_hex_digest(ak["before"])
            after_d = cp_hex_digest(ak["after"])
            changes = ()
            seen_sections = set()
            prev_section_index = -1
            for ch in cp_list(ak["changes"]):
                chobj = o(ch, ("section", "before", "after"))
                chsection = enum(chobj["section"], AD_SECTIONS)
                chindex = AD_SECTIONS.index(chsection)
                if chsection in seen_sections or chindex <= prev_section_index:
                    cp_state_int()
                seen_sections.add(chsection)
                prev_section_index = chindex
                changes = changes + (
                    (chsection, cp_hex_digest(chobj["before"]),
                     cp_hex_digest(chobj["after"])),
                )
            audit_events.append({
                "rev": rev, "now": anow, "kind": kind,
                "section": section, "before": before_d, "after": after_d,
            })
            audit_sections.append(tuple(changes))
        if len(audit_events) > 64:
            cp_state_int()

        reservation_raw = root["reservation"]
        if reservation_raw is None:
            reservation = None
        else:
            rs = o(reservation_raw, ("at", "digest", "config"))
            at = ti(rs["at"])
            rdigest = cp_hex_digest(rs["digest"])
            snapshot = snapshot_config(rs["config"])
            if config_digest(snapshot) != rdigest:
                cp_state_int()
            reservation = (snapshot, at, rdigest)

        ep_switch_raw = root["ep_switch"]
        if ep_switch_raw is None:
            ep_switch = None
        else:
            esv = o(ep_switch_raw, ("digest", "at", "before", "items"))
            es_digest = cp_hex_digest(esv["digest"])
            es_at = ti(esv["at"])
            es_before = ti(esv["before"])
            es_items = []
            es_seen = set()
            for eitem in cp_list(esv["items"]):
                eo = o(eitem, ("id", "base", "target"))
                eid = ident(eo["id"])
                if eid in es_seen:
                    cp_state_int()
                es_seen.add(eid)
                # 注意：不做后端悬空引用校验——remove 可在预约生效前删除
                # 后端，生效时由 ei 判 BACKEND/3。
                ebase_raw = eo["base"]
                if ebase_raw is None:
                    ebase = None
                else:
                    eb = o(ebase_raw, ("host", "port"))
                    ebase = (
                        parse_endpoint_host(eb["host"]),
                        parse_endpoint_port(eb["port"]),
                    )
                etarget_raw = eo["target"]
                if etarget_raw is None:
                    etarget = None
                else:
                    et = o(etarget_raw, ("host", "port"))
                    etarget = (
                        parse_endpoint_host(et["host"]),
                        parse_endpoint_port(et["port"]),
                    )
                es_items.append((eid, ebase, etarget))
            # 结构域校验先于语义：项数 1..1000 同 es/ej 形状约束。
            if not 1 <= len(es_items) <= 1000:
                fail(EXIT_INPUT, "INPUT")
            # 语义校验：建立时强制 before<=at。
            if es_before > es_at:
                cp_state_int()
            # 身份摘要须与按规范化快照重算值逐字一致，否则为非法状态组合。
            if endpoint_switch_digest(es_items, es_before) != es_digest:
                cp_state_int()
            ep_switch = (es_items, es_before, es_at, es_digest)

        # 端点切换预约生命周期审计：顶层段 ep_audit 固定键序
        # next_seq,events。seq 独立自 1 递增、不复用；事件按 seq 升序仅
        # 保留最近 64 条，故恢复后窗口须与 next_seq 严格一致（连续、末项
        # 为 next_seq-1、长度为 min(64, next_seq-1)），矛盾计数或缺口为
        # 非法状态组合（STATE）。
        ep_audit_raw = o(root["ep_audit"], ("next_seq", "events"))
        ep_audit_seq = si2(ep_audit_raw["next_seq"], 1, 10 ** 18)
        ep_audit_events = deque(maxlen=64)
        ep_event_list = cp_list(ep_audit_raw["events"])
        if len(ep_event_list) > 64:
            cp_state_int()
        expected_len = min(64, ep_audit_seq - 1)
        if len(ep_event_list) != expected_len:
            cp_state_int()
        def ep_audit_identity(value):
            if value is None:
                return None
            iv = o(value, ("digest", "at"))
            return (cp_hex_digest(iv["digest"]), ti(iv["at"]))

        previous_seq = None
        previous_after = None
        previous_now = None
        for eentry in ep_event_list:
            ekobj = o(eentry, ("seq", "now", "action", "before", "after"))
            ese_seq = si2(ekobj["seq"], 1, 10 ** 18)
            if previous_seq is not None and ese_seq != previous_seq + 1:
                cp_state_int()
            previous_seq = ese_seq
            ese_now = ti(ekobj["now"])
            # 事件 now 取自共用非递减显式时钟，故事件间不得递减（允许同
            # 刻多个变更）。
            if previous_now is not None and ese_now < previous_now:
                cp_state_int()
            previous_now = ese_now
            ese_action = enum(
                ekobj["action"],
                ("SET", "REPLACE", "CANCEL", "APPLY", "CLEAR"),
            )
            ese_before = ep_audit_identity(ekobj["before"])
            ese_after = ep_audit_identity(ekobj["after"])
            # 窗口内相邻事件身份须首尾相接（上一条 after 即下一条
            # before）；最旧保留项之前可能已有事件随窗口淘汰，不校验其
            # before。
            if previous_after is not None and ese_before != previous_after:
                cp_state_int()
            previous_after = ese_after
            # 动作与前后身份组合须与写入路径一致：SET 从空建立（before 为
            # null、after 非空）；REPLACE 以不同身份覆盖（两侧均非空）；
            # CANCEL/APPLY/CLEAR 清除现存预约（before 非空、after 为
            # null）。矛盾组合为非法状态。
            if ese_action == "SET":
                if ese_before is not None or ese_after is None:
                    cp_state_int()
            elif ese_action == "REPLACE":
                # 写入路径仅在身份确实不同时记 REPLACE（同身份为幂等重
                # 报、不记事件）。
                if ese_before is None or ese_after is None \
                        or ese_before == ese_after:
                    cp_state_int()
            else:
                if ese_before is None or ese_after is not None:
                    cp_state_int()
            ep_audit_events.append(
                (ese_seq, ese_now, ese_action, ese_before, ese_after)
            )
        if ep_audit_events and ep_audit_events[-1][0] != ep_audit_seq - 1:
            cp_state_int()
        # 末条事件的 after 必须等于当前预约身份：最近一次真正改变预约的
        # 操作决定现状。窗口为空时（从未分配 seq）不得存在预约。
        if ep_audit_events:
            if ep_audit_events[-1][4] != ep_identity(ep_switch):
                cp_state_int()
            # 最后事件时钟不得晚于当前逻辑时钟。
            if last_now is not None and ep_audit_events[-1][1] > last_now:
                cp_state_int()
        elif ep_switch is not None:
            cp_state_int()

        mo_raw = o(root["mo"], ("seq", "cache"))
        mo_seq = si2(mo_raw["seq"], 1, 10 ** 18)
        mo_cache = None
        if mo_raw["cache"] is not None:
            mc = o(mo_raw["cache"], ("seq", "now", "result"))
            cseq = si2(mc["seq"], 1, 10 ** 18)
            cnow = ti(mc["now"])
            if cseq != mo_seq - 1:
                cp_state_int()
            result = mc["result"]
            if not isinstance(result, dict) or result.get("op") != "mo" \
                    or result.get("seq") != cseq or result.get("window") \
                    != cnow // 60:
                cp_state_int()
            mitems = result.get("backends")
            if not isinstance(mitems, list):
                cp_state_int()
            for item in mitems:
                if not isinstance(item, dict) or item.get("id") not in backends:
                    cp_state_int()
            mo_cache = (cseq, cnow, result)

        return {
            "last_now": last_now,
            "ring_vnodes": ring_vnodes,
            "sticky_ttl": sticky_ttl,
            "idle_ttl": idle_ttl,
            "hard_ttl": hard_ttl,
            "pick_mode": pick_mode,
            "rr_ticket": rr_ticket,
            "queue_cfg": queue_cfg,
            "bp_cfg": bp_cfg,
            "bp_state": bp_state,
            "dequeue_policy": dequeue_policy,
            "queue_mode": queue_mode,
            "full_mode": full_mode,
            "evict_count": evict_count,
            "evict_last": evict_last,
            "aging_cfg": aging_cfg,
            "backends": backends,
            "connections": connections,
            "conn_endpoints": conn_endpoints,
            "sticky_map": sticky_map,
            "buckets": buckets,
            "quotas": quotas,
            "wait_queue": wait_queue,
            "cap_overrides": cap_overrides,
            "overload_hist": overload_hist,
            "wait_hist": wait_hist,
            "limit_hist": limit_hist,
            "wait_alerts": wait_alerts,
            "alert": pool_alert,
            "alert_events": alert_events,
            "err_alerts": err_alerts,
            "err_events": err_events,
            "percent_alerts": percent_alerts,
            "percent_events": percent_events,
            "retry_alerts": retry_alerts,
            "retry_events": retry_events,
            "conc_alerts": conc_alerts,
            "limit_alerts": limit_alerts,
            "unavail_alerts": unavail_alerts,
            "commit_history": commit_history,
            "next_rev": next_rev,
            "audit_events": audit_events,
            "audit_sections": audit_sections,
            "reservation": reservation,
            "ep_switch": ep_switch,
            "ep_audit_events": ep_audit_events,
            "ep_audit_seq": ep_audit_seq,
            "mo_seq": mo_seq,
            "mo_cache": mo_cache,
        }

    def validate_checkpoint_object(raw_cp):
        # sx/sm 共用：校验一份按 version,digest,state 排列且不含 op 的检查点
        # 对象，返回 (digest, 规范化 state, 规范化 state 紧凑字节)。形状/键
        # 序/version/digest 格式报 INPUT；紧凑编码超 8MiB 报 OVERLOAD；摘要
        # 匹配先于语义（不符 INPUT）；完整结构/类型/范围/UTF-8 解析报 INPUT，
        # 悬空引用/重复标识/矛盾计数/非法组合及规范化往返偏差报 STATE。不
        # 安装候选束。
        if (
            not isinstance(raw_cp, dict)
            or list(raw_cp) != ["version", "digest", "state"]
        ):
            fail(EXIT_INPUT, "INPUT")
        cp_version = raw_cp["version"]
        if (
            not isinstance(cp_version, int)
            or isinstance(cp_version, bool)
            or cp_version != CHECKPOINT_VERSION
        ):
            fail(EXIT_INPUT, "INPUT")
        cp_digest = cp_hex_digest(raw_cp["digest"])
        raw_state = raw_cp["state"]
        try:
            state_bytes = json.dumps(
                raw_state, ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            envelope_bytes = json.dumps(
                {"version": cp_version, "state": raw_state},
                ensure_ascii=False, separators=(",", ":"),
            ).encode("utf-8")
        except UnicodeEncodeError:
            fail(EXIT_INPUT, "INPUT")
        if len(state_bytes) > CHECKPOINT_LIMIT:
            fail(EXIT_OVERLOAD, "OVERLOAD")
        if hashlib.sha256(envelope_bytes).hexdigest() != cp_digest:
            fail(EXIT_INPUT, "INPUT")
        bundle = parse_checkpoint(raw_state)
        # 规范化往返：重新规范化后的紧凑编码须与所给 state 逐字节一致，强制
        # 所给 state 本身即规范化形态。
        norm_state = export_bundle(bundle)
        norm_bytes = json.dumps(
            norm_state, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if norm_bytes != state_bytes:
            fail(EXIT_STATE, "STATE")
        return cp_digest, norm_state, norm_bytes, bundle

    def ep_identity(switch):
        """预约身份 (digest, at)；无预约为 None。"""
        if switch is None:
            return None
        return (switch[3], switch[2])

    def ep_identity_json(identity):
        """把 (digest, at) 身份输出为固定键序 digest,at 的对象；None 为
        null。"""
        if identity is None:
            return None
        identity_digest, identity_at = identity
        return {"digest": identity_digest, "at": identity_at}

    def ep_audit_append(action, now, before, after):
        """分配下一个永不复用的 seq 并追加一条端点预约生命周期事件；deque
        按 64 条窗口自动淘汰最旧项，seq 游标不随淘汰回退或复用。O(1)。"""
        nonlocal ep_audit_seq
        ep_audit_events.append((ep_audit_seq, now, action, before, after))
        ep_audit_seq += 1

    for raw_op in ops:
        op = parse_op(raw_op)

        if op[0] in (
            "open", "close", "probe", "add", "ws", "wg", "cr", "cg",
            "dr", "du", "dg", "dq", "dx", "ls", "la", "lg", "qs", "qg", "oa", "ot",
            "oq", "lh", "lt", "le",
            "mr", "mg", "mh",
            "ms", "mx", "rh", "rt", "rr", "ra", "ma", "lp",
            "ci", "cb", "cb_cond", "cu", "cv", "cd", "pd", "hd", "hb", "fx", "fr", "fi", "ft", "oi", "od", "tk", "te", "tg", "tx", "route", "fq", "pick", "fh",
            "fd", "fc",
            "fa", "fe", "ah", "oh", "wh", "wp", "wa", "br",
            "ru", "ua", "ea", "eh", "pa", "ph", "xa", "xh", "xg", "xp",
            "na",
            "mu",
            "cp", "cq", "ca", "ca_cond", "cx", "cy",
            "eq", "ec",
            "er", "ex",
            "ey", "eb",
            "ez",
            "ej",
            "es", "eu", "ei", "ed", "eo", "ev", "ew",
        ):
            now = op[-1]
            # 三键 add 的 now 占位为 None，不参与时钟。
            if now is not None:
                if last_now is not None and now < last_now:
                    # qs 精确重报（limit/span/now 同上次配置）豁免时钟倒退：
                    # 不回拨时钟，由处理分支幂等返回并保留 window/used；其余
                    # 旧时刻操作（含 qg 与异参 qs）仍报 INPUT。
                    if op[0] == "qs":
                        _, qs_scope, qs_id, qs_limit, qs_span, _ = op
                        quota = quotas.get((qs_scope, qs_id))
                        if quota is None or quota["last"] != (
                            qs_limit, qs_span, now
                        ):
                            fail(EXIT_INPUT, "INPUT")
                    else:
                        fail(EXIT_INPUT, "INPUT")
                else:
                    last_now = now

        if op[0] == "add":
            _, backend_id, weight, d, now = op
            if backend_id in backends:
                if now is None:
                    # 三键旧形式：旧行为不变，重复 add 一律 BACKEND。
                    fail(EXIT_BACKEND, "BACKEND")
                # 五键：同参重报幂等；任何 add5/ws 占用同一 now 而异参，
                # 报 INPUT；其余重复 add 报 BACKEND。
                prev = backends[backend_id]["last_op"]
                if prev == ("add5", weight, d, now):
                    results.append({"op": "add", "ok": True})
                    continue
                if (
                    prev is not None
                    and prev[0] in ("add5", "ws")
                    and prev[3] == now
                ):
                    fail(EXIT_INPUT, "INPUT")
                fail(EXIT_BACKEND, "BACKEND")
            if d == 0:
                stage = "steady"
                warm_start = warm_end = warm_from = None
            else:
                stage = "warm"
                warm_from = 100
                warm_start = now
                warm_end = now + d
            backends[backend_id] = {
                "weight": weight,
                "current": 0,
                "conns": 0,
                "healthy": True,
                "fail": DEFAULT_FAIL,
                "success": DEFAULT_SUCCESS,
                "failures": 0,
                "successes": 0,
                "probe_now": None,
                "probe_ok": None,
                "stage": stage,
                "warm_d": d,
                "warm_from": warm_from,
                "warm_start": warm_start,
                "warm_end": warm_end,
                "circuit": None,
                "drain": {
                    "t": None,
                    "state": "A",
                    "start": None,
                    "end": None,
                    "deadline": None,
                    "forced": 0,
                },
                # ru 不可用连续生效起点：health/drain/circuit 各为 None 或
                # 整数时刻；fault 只读故障时间线，无需存储。新增即全无；
                # 起算与清除点见各状态迁移分支与 apply_config。
                "unavail_since": {
                    "health": None, "drain": None, "circuit": None,
                },
                "last_op": ("add3", weight) if now is None else ("add5", weight, d, now),
                # 后端 IP 端点：ep 登记的 (host, port)，未配置为 None；
                # remove 后重加即回到未配，随 ce/ci 导出导入（version=7；
                # v1..v5 规范化为 None）。
                "endpoint": None,
                # 故障时间线：fp/fs 登记的段列表 [(k,a,z,v),...]，按 a 升序、
                # [a,z) 互不重叠；fs 替换为单段、fb 替换全体（未列入清空），
                # fp 原子替换该 id 全部段，remove 清空，ci 按配置重建
                # （v7 载入 faults、v1..v6 清空）。fault_a 为平行的段起点
                # 列表，供 bisect O(log T_b) 取唯一活动段。
                "faults": [],
                "fault_a": [],
                # 故障演练统计（fm）：按登记种类 D/F/S 各记
                # affected/rejected/retries/remaps/recovered 五计数，封顶
                # 10^18；fault_base 为按段的恢复判定基线集合（段被观察为
                # 受影响即加入，首次再观察为 N——F 非故障相位、换段或落入
                # 间隙——时按上段种类结算 recovered 并移除；fs/fb/fp 异参
                # 替换只清基线不清计数，同参不清）。remove 后重加与 ci
                # 成功随新记录清零。
                "fault_stats": new_fault_stats(),
                "fault_base": set(),
                # 故障统计分钟历史（fh）：window=now//60 -> 当窗
                # new_fault_stats 结构（D/F/S 各五计数），随记账与 fault_stats
                # 同序双写，每后端仅保留最近 60 窗，空窗不预建；fs/fb/fp
                # 重报、替换或移除不清历史，remove 后重加与 ci 成功随新记录
                # 清空。
                "fault_hist": {},
                # 度量历史：window -> [requests, errors, retries, remaps,
                # [五个延迟桶]]，仅保留最近 60 窗；空表示从未 mr。
                "metrics": {},
                # mo 增量基线：[window, requests, errors, retries, remaps,
                # [五延迟桶]]，window 为基线所属窗；上次 mo 与本次同窗时
                # 作为减数，跨窗或初始（window=None）增量按零。add 与
                # remove 后重加均从零起步，ci/cb 重建默认运行态时清零。
                "mo_base": [None, 0, 0, 0, 0, [0, 0, 0, 0, 0]],
                # H pick 记账：total/first/sticky/expired/removed/health/
                # circuit/drain 七计数，各封顶 10^18；remove 后重加即随新记录
                # 清零。hm 只读查询。
                "pick_counts": {
                    "total": 0, "first": 0, "sticky": 0, "expired": 0,
                    "removed": 0, "health": 0, "circuit": 0, "drain": 0,
                },
                # 后端采样历史（ms/mx）：window -> {"points": {now: (活动
                # 连接数, removed)}, "samples": 窗内采样数, "peak": 窗内并发
                # 峰值}；points 按采样先后（now 升序）保序，samples/peak 由
                # ms 同步维护供 na O(1) 取窗值。按 now//60 仅保留最近 60 窗，
                # 每窗至多 60 个不同 now。remove 后重加、ci 成功即清空。
                "samples": {},
                # 不可用原因分钟历史（rh/rt）：window -> 固定键序
                # health,drain,circuit,overload 的对象，每因布局
                # [count,first,last]，仅保留最近 60 窗，空窗不预建；计数
                # 封顶 10^18，first/last 为首次/最近事件的 now（封顶后
                # last 仍更新），仅在状态转换或 oa 因连接达 cap 入队时记账。
                # remove 后重加与 ci 成功即清空。
                "reason_hist": {},
                # 重试/重映射时刻历史（rr）：window -> 固定键序
                # retries,remaps 的对象，每项布局 [count,first,last]，仅
                # 保留最近 60 窗，空窗不预建；仅在 mr、fx/fr 自动度量写入
                # 正数 retries/remaps 时记账，计数封顶 10^18，first/last
                # 为首次/最近写入的 now（封顶后 last 仍更新）。remove 后
                # 重加与 ci/cb 成功即清空。
                "retry_hist": {},
            }
            results.append({"op": "add", "ok": True})

        elif op[0] == "remove":
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if record["conns"] > 0:
                fail(EXIT_STATE, "STATE")
            del backends[backend_id]
            buckets.pop(("B", backend_id), None)
            quotas.pop(("B", backend_id), None)
            # remove 同时删除该后端多维限流历史；同 id 重加不继承（ls/qs
            # 重配不清历史，但 remove 即整体删除）。
            limit_hist.pop(("B", backend_id), None)
            # remove 删除接纳容量覆盖；同 id 重加不继承（cap_overrides 以现存
            # 后端为键，新记录无覆盖）。
            cap_overrides.pop(backend_id, None)
            # remove 删除该后端错误率告警；同 id 重加回到未首评。
            err_alerts.pop(backend_id, None)
            # remove 同时删除该后端的告警转换历史；同 id 重加不继承。
            err_events.pop(backend_id, None)
            # remove 删除该后端延迟分位告警；同 id 重加回到未首评。
            percent_alerts.pop(backend_id, None)
            # remove 同时删除该后端的 pa 告警转换历史；同 id 重加不继承。
            percent_events.pop(backend_id, None)
            # remove 删除该后端重试/重映射告警的 R/M 两键；同 id 重加各自
            # 回到未首评。
            retry_alerts.pop((backend_id, "R"), None)
            retry_alerts.pop((backend_id, "M"), None)
            # remove 同时删除该后端 R/M 两键的 xa 告警转换历史；同 id 重加
            # 不继承。
            retry_events.pop((backend_id, "R"), None)
            retry_events.pop((backend_id, "M"), None)
            # remove 删除该后端并发告警；同 id 重加回到未首评。
            conc_alerts.pop(backend_id, None)
            # remove 删除该后端的 B 维限流告警；同 id 重加回到未首评。
            limit_alerts.pop(("B", backend_id), None)
            # remove 删除该后端全部原因的连续不可用时长告警（四键）；同 id
            # 重加各原因均回到未首评。
            for unavail_reason in ("drain", "health", "circuit", "fault"):
                unavail_alerts.pop((backend_id, unavail_reason), None)
            results.append({"op": "remove", "ok": True})

        elif op[0] == "pick":
            _, key, pick_now = op
            if pick_mode == "H":
                if key is None:
                    # H 仅收 {op,key} 或 {op,key,now}：原 {op} 形状与 H
                    # 模式不符，报 STATE（键集本身非法已在解析期判 INPUT）。
                    fail(EXIT_STATE, "STATE")
                # H 共享既有环与粘性映射，沿用 route 选择语义；粘性保留另
                # 要求 A 态（D 态原粘性也失格，依环迁移且不迁回）。首次按
                # 环选，目标删除或因健康、熔断、排空失格才迁移；改 vnodes
                # 不主动迁移；三键沿用到期规则。pick 不改连接数与 W/R/L
                # 运行态（current/ticket/conns 均不动）。
                # 必须在 select_route 改写映射之前抓取旧项供记账分类；
                # select_route 失败会抛 STATE 错，不会走到下方记账，故失败
                # 项自然不记。
                old_entry = sticky_map.get(key)
                chosen_id, sticky, remapped, expired, expires = select_route(
                    key, pick_now, drain_strict=True
                )
                # 调度与映射不变；成功项仅记一次并归属返回 id。
                record_h_pick(chosen_id, old_entry, pick_now)
                if pick_now is None:
                    # 二键结果键序 op,id,sticky,remapped。
                    results.append(
                        {
                            "op": "pick",
                            "id": chosen_id,
                            "sticky": sticky,
                            "remapped": remapped,
                        }
                    )
                else:
                    # 三键追加 expired,expires，值义同 route。
                    results.append(
                        {
                            "op": "pick",
                            "id": chosen_id,
                            "sticky": sticky,
                            "remapped": remapped,
                            "expired": expired,
                            "expires": expires,
                        }
                    )
                continue
            if key is not None:
                # W/R/L 仅收原 {op} 形状；{op,key} 或 {op,key,now} 与
                # 当前模式不符，报 STATE（now 时钟已先于分支校验）。
                fail(EXIT_STATE, "STATE")
            if pick_mode == "L":
                # 最少连接：仅考虑 healthy、熔断 C、排空 A 的后端，取活动
                # 连接数 conns 最少者，并列取最早加入者（dict 遍历序即加入
                # 序，严格小于才替换）。忽略权重、无游标；pick 本身不改
                # conns/current/ticket，连接操作实时改变 conns，后端增删
                # 及资格迁移仅改变下次候选。O(B) 时间、O(1) 额外空间。
                chosen_id = None
                chosen_conns = None
                for backend_id, record in backends.items():
                    if (
                        not record["healthy"]
                        or not circuit_closed(record)
                        or not drain_available(record)
                    ):
                        continue
                    if chosen_conns is None or record["conns"] < chosen_conns:
                        chosen_conns = record["conns"]
                        chosen_id = backend_id
                if chosen_id is None:
                    fail(EXIT_STATE, "STATE")
                results.append({"op": "pick", "id": chosen_id})
                continue
            if pick_mode == "R":
                # 轮询：按既有健康、熔断闭合、排空 A 条件取加入序可选列表 E；
                # E 空报 STATE，否则取 E[ticket%len(E)] 并将 ticket 加一。
                # 忽略权重、不改平滑 current；两遍扫描保持 O(1) 额外空间。
                eligible = 0
                for record in backends.values():
                    if (
                        record["healthy"]
                        and circuit_closed(record)
                        and drain_available(record)
                    ):
                        eligible += 1
                if eligible == 0:
                    fail(EXIT_STATE, "STATE")
                target = rr_ticket % eligible
                rr_ticket += 1
                index = 0
                chosen_id = None
                for backend_id, record in backends.items():
                    if (
                        record["healthy"]
                        and circuit_closed(record)
                        and drain_available(record)
                    ):
                        if index == target:
                            chosen_id = backend_id
                            break
                        index += 1
                results.append({"op": "pick", "id": chosen_id})
                continue
            # 只在 healthy 且熔断闭合的可用（A）池内平滑加权：以最近时钟时刻
            # 的当前有效权重（百分制整数）累加，累加与总权重扣减都忽略不健
            # 康、熔断非 C 或排空中/已摘除的后端。
            chosen_id = None
            chosen_current = None
            healthy_total = 0
            for backend_id, record in backends.items():
                if (
                    not record["healthy"]
                    or not circuit_closed(record)
                    or not drain_available(record)
                ):
                    continue
                weight = effective_weight(record, last_now)
                record["current"] += weight
                healthy_total += weight
                # 严格大于：并列时保留遍历到的最早者。
                if chosen_current is None or record["current"] > chosen_current:
                    chosen_current = record["current"]
                    chosen_id = backend_id
            if chosen_id is None:
                fail(EXIT_STATE, "STATE")
            backends[chosen_id]["current"] -= healthy_total
            results.append({"op": "pick", "id": chosen_id})

        elif op[0] == "open":
            _, cid, flow, now = op
            chosen_id = None
            chosen_key = None
            for backend_id, record in backends.items():
                if (
                    not record["healthy"]
                    or not circuit_closed(record)
                    or not drain_available(record)
                ):
                    continue
                # 连接数升序、有效权重降序、加入顺序（dict 遍历序）。
                key = (record["conns"], -effective_weight(record, now))
                if chosen_key is None or key < chosen_key:
                    chosen_key = key
                    chosen_id = backend_id
            if chosen_id is None:
                fail(EXIT_STATE, "STATE")
            if cid in connections:
                fail(EXIT_CONNECTION, "CONNECTION")
            establish_connection(cid, chosen_id, flow, now)
            results.append({"op": "open", "cid": cid, "backend": chosen_id})

        elif op[0] == "close":
            _, cid, now = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            record = backends[connection[0]]
            record["conns"] -= 1
            del connections[cid]
            # 删除连接时同步删除其转发快照。
            conn_endpoints.pop(cid, None)
            drain = record["drain"]
            if drain["state"] == "D" and record["conns"] == 0:
                # 排空中最后连接关闭即转 X，end 取本次 close 的 now。
                drain["state"] = "X"
                drain["end"] = now
            results.append({"op": "close", "ok": True})

        elif op[0] == "hset":
            _, backend_id, fail_threshold, success_threshold = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            # 只换阈值并清连续计数，不改健康状态。
            record["fail"] = fail_threshold
            record["success"] = success_threshold
            record["failures"] = 0
            record["successes"] = 0
            results.append({"op": "hset", "ok": True})

        elif op[0] == "probe":
            _, backend_id, ok, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if record["probe_now"] == now:
                # 同一 (id, now) 已生效过：ok 相同则幂等，不同即冲突重报。
                if record["probe_ok"] != ok:
                    fail(EXIT_INPUT, "INPUT")
            else:
                if ok:
                    record["successes"] = min(
                        record["successes"] + 1, record["success"]
                    )
                    record["failures"] = 0
                    if not record["healthy"] and record["successes"] >= record["success"]:
                        record["healthy"] = True
                        record["successes"] = 0
                        record["failures"] = 0
                        record["current"] = 0
                        # ru：恢复 healthy，health 连续生效起点清除。
                        record["unavail_since"]["health"] = None
                        # 恢复：以该 probe 的 now 从 1.00 按登记的 d 重启预热；
                        # d=0 时立即回到目标权重。
                        if record["warm_d"] > 0:
                            record["stage"] = "warm"
                            record["warm_from"] = 100
                            record["warm_start"] = now
                            record["warm_end"] = now + record["warm_d"]
                        else:
                            record["stage"] = "steady"
                else:
                    record["failures"] = min(
                        record["failures"] + 1, record["fail"]
                    )
                    record["successes"] = 0
                    if record["healthy"] and record["failures"] >= record["fail"]:
                        record["healthy"] = False
                        record["successes"] = 0
                        record["failures"] = 0
                        record["current"] = 0
                        # 不可用原因历史：仅 healthy→unhealthy 的转换记 health；
                        # 同 (id,now,ok) 重报在上方已幂等返回，不会至此。
                        record_reason(backend_id, "health", now)
                        # ru：probe 转 unhealthy 起算连续生效起点。
                        record["unavail_since"]["health"] = now
                record["probe_now"] = now
                record["probe_ok"] = ok
            results.append({"op": "probe", "ok": True})

        elif op[0] == "hget":
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            results.append(
                {
                    "op": "hget",
                    "id": backend_id,
                    "state": "healthy" if record["healthy"] else "unhealthy",
                    "successes": record["successes"],
                    "failures": record["failures"],
                    "fail": record["fail"],
                    "success": record["success"],
                }
            )

        elif op[0] == "ws":
            _, backend_id, weight, d, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            prev = record["last_op"]
            if prev == ("ws", weight, d, now):
                # 同参重报幂等，不重复过渡、不清 current。
                results.append({"op": "ws", "ok": True})
                continue
            if (
                prev is not None
                and prev[0] in ("add5", "ws")
                and prev[3] == now
            ):
                # 同 (id, now) 异参（或异类操作）冲突，优先于 unhealthy 判定。
                fail(EXIT_INPUT, "INPUT")
            if not record["healthy"]:
                fail(EXIT_STATE, "STATE")
            # 自当前有效权重线性过渡；必须在改 weight 前按旧调度取有效值。
            start_weight = effective_weight(record, now)
            record["weight"] = weight
            record["current"] = 0  # 调权清零平滑 current。
            record["warm_d"] = d
            if d == 0:
                record["stage"] = "steady"
                record["warm_from"] = None
                record["warm_start"] = None
                record["warm_end"] = None
            else:
                record["stage"] = "warm"
                record["warm_from"] = start_weight
                record["warm_start"] = now
                record["warm_end"] = now + d
            record["last_op"] = ("ws", weight, d, now)
            results.append({"op": "ws", "ok": True})

        elif op[0] == "wg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            if not record["healthy"]:
                stage = "unhealthy"
                start = end = None
            elif record["stage"] == "warm" and now < record["warm_end"]:
                stage = "warm"
                start = record["warm_start"]
                end = record["warm_end"]
            else:
                stage = "steady"
                start = end = None
            cents = effective_weight(record, now) if stage != "unhealthy" else 0
            effective = "%d.%02d" % (cents // 100, cents % 100)
            results.append(
                {
                    "op": "wg",
                    "id": backend_id,
                    "target": record["weight"],
                    "effective": effective,
                    "stage": stage,
                    "start": start,
                    "end": end,
                }
            )

        elif op[0] == "ss":
            _, ttl = op
            if sticky_ttl is None:
                # 首配立即生效；既有粘性项的 expires 仍为 None（二键语义）。
                sticky_ttl = ttl
            elif sticky_ttl != ttl:
                # 异值重配报 STATE；同值幂等。
                fail(EXIT_STATE, "STATE")
            results.append({"op": "ss", "ok": True})

        elif op[0] == "chash":
            _, vnodes = op
            # 同值幂等、异值生效；建环会遇到的 healthy id 必须可编码。
            for record_id, record in backends.items():
                if record["healthy"]:
                    encode_backend_id(record_id)
            ring_vnodes = vnodes
            results.append({"op": "chash", "ok": True})

        elif op[0] == "route":
            _, key, route_now = op
            chosen_id, sticky, remapped, expired, expires = select_route(
                key, route_now
            )
            if route_now is None:
                # 旧二键：结果键序 op,key,backend,sticky,remapped 不变。
                results.append(
                    {
                        "op": "route",
                        "key": key,
                        "backend": chosen_id,
                        "sticky": sticky,
                        "remapped": remapped,
                    }
                )
            else:
                # 三键限时粘性：键序
                # op,key,backend,sticky,remapped,expired,expires。
                results.append(
                    {
                        "op": "route",
                        "key": key,
                        "backend": chosen_id,
                        "sticky": sticky,
                        "remapped": remapped,
                        "expired": expired,
                        "expires": expires,
                    }
                )

        elif op[0] == "cs":
            _, backend_id, n, m, r, w, q = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            params = (n, m, r, w, q)
            circuit = record["circuit"]
            if circuit is not None and circuit["params"] == params:
                # 同参重报幂等，不重置状态与窗口。
                results.append({"op": "cs", "ok": True})
                continue
            # 首配或异参重配：置 C 并清空窗。
            record["circuit"] = {
                "params": params,
                "state": "C",
                "window": deque(maxlen=n),
                "next": None,
                "used": 0,
                "cr_now": None,
                "cr_ok": None,
            }
            # ru：cs 重配清除 circuit 连续生效起点。
            record["unavail_since"]["circuit"] = None
            results.append({"op": "cs", "ok": True})

        elif op[0] == "cr":
            _, backend_id, ok, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            circuit = record["circuit"]
            if circuit is None:
                fail(EXIT_STATE, "STATE")
            if circuit["cr_now"] == now:
                # 同一 (id, now) 已生效过：ok 相同则幂等，不同即冲突重报。
                if circuit["cr_ok"] != ok:
                    fail(EXIT_INPUT, "INPUT")
                results.append({"op": "cr", "ok": True})
                continue
            n, m, r, w, q = circuit["params"]
            if circuit["state"] == "O":
                if now < circuit["next"]:
                    # 熔断恢复窗口未到期，拒绝报告。
                    fail(EXIT_STATE, "STATE")
                # 到期先转 H，本条按 H 处理。
                circuit["state"] = "H"
                circuit["next"] = None
                circuit["used"] = 0
            if circuit["state"] == "C":
                window = circuit["window"]
                window.append(ok)
                samples = len(window)
                failures = sum(1 for value in window if not value)
                if samples >= m and failures * 100 >= r * samples:
                    circuit["state"] = "O"
                    circuit["next"] = now + w
                    circuit["used"] = 0
                    # 不可用原因历史：仅 C→O 的熔断打开记 circuit。
                    record_reason(backend_id, "circuit", now)
                    # ru：自 C 首次转 O 起算；回 C 或 cs 重配已清除，故此刻
                    # 起点必为 None。
                    record["unavail_since"]["circuit"] = now
            else:  # H
                if ok:
                    circuit["used"] += 1
                    if circuit["used"] >= q:
                        # 连续 q 次成功转 C 并清窗。
                        circuit["state"] = "C"
                        circuit["window"].clear()
                        circuit["used"] = 0
                        # ru：回 C 清除 circuit 连续生效起点。
                        record["unavail_since"]["circuit"] = None
                else:
                    # H 中失败立即重开并重算恢复时刻。
                    circuit["state"] = "O"
                    circuit["next"] = now + w
                    circuit["used"] = 0
                    # 不可用原因历史：H→O 的重开同样记 circuit。
                    record_reason(backend_id, "circuit", now)
                    # ru：H 重开 O 不重置起点（沿用首次 C→O 的 now）。
            circuit["cr_now"] = now
            circuit["cr_ok"] = ok
            results.append({"op": "cr", "ok": True})

        elif op[0] == "cg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            circuit = record["circuit"]
            if circuit is None:
                fail(EXIT_STATE, "STATE")
            if circuit["state"] == "O" and now >= circuit["next"]:
                # 恢复窗口到期，触发转 H。
                circuit["state"] = "H"
                circuit["next"] = None
                circuit["used"] = 0
            state = circuit["state"]
            window = circuit["window"]
            results.append(
                {
                    "op": "cg",
                    "id": backend_id,
                    "state": state,
                    "count": len(window),
                    "fail": sum(1 for value in window if not value),
                    "reason": None if state == "C" else "rate",
                    "next": circuit["next"] if state == "O" else None,
                    "used": circuit["used"] if state == "H" else 0,
                }
            )

        elif op[0] == "ds":
            _, backend_id, t = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] == t:
                # 同值幂等，任意状态下都不改配置。
                results.append({"op": "ds", "ok": True})
                continue
            if drain["state"] == "D":
                # 排空中改值报 STATE。
                fail(EXIT_STATE, "STATE")
            drain["t"] = t
            results.append({"op": "ds", "ok": True})

        elif op[0] == "dr":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] is None:
                fail(EXIT_STATE, "STATE")
            if drain["state"] == "A":
                # 开启新一轮摘除：start/deadline/forced 复位，end 待迁移时填。
                drain["start"] = now
                drain["deadline"] = now + drain["t"]
                drain["forced"] = 0
                drain["end"] = None
                if record["conns"] > 0:
                    drain["state"] = "D"
                else:
                    drain["state"] = "X"
                    drain["end"] = now
                # 不可用原因历史：A→D/X 的转换记 drain（一次 dr 至多一次）；
                # D/X 再 dr 幂等返回，不记。
                record_reason(backend_id, "drain", now)
                # ru：dr 使 A 转 D/X 起算连续生效起点；D/X 再 dr 无迁移，
                # 不重置（其后 close/dg/tx 使 D 转 X 同样不触碰起点）。
                record["unavail_since"]["drain"] = now
            # D/X 再 dr 幂等，不改状态。
            results.append({"op": "dr", "ok": True})

        elif op[0] == "du":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] is None:
                fail(EXIT_STATE, "STATE")
            if drain["state"] == "D":
                # 取消排空记 end=now；已强关的连接不恢复，慢启动不重置。
                drain["end"] = now
            drain["state"] = "A"
            # ru：du 清除 drain 连续生效起点（A 态再 du 幂等，本就为 None）。
            record["unavail_since"]["drain"] = None
            results.append({"op": "du", "ok": True})

        elif op[0] == "dg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            drain = record["drain"]
            if drain["t"] is None:
                fail(EXIT_STATE, "STATE")
            if drain["state"] == "D" and now >= drain["deadline"]:
                # 到期强关该后端全部连接（O(C)），转 X，end=deadline。
                forced = 0
                for cid, connection in list(connections.items()):
                    if connection[0] == backend_id:
                        del connections[cid]
                        conn_endpoints.pop(cid, None)
                        forced += 1
                record["conns"] = 0
                drain["forced"] = forced
                drain["state"] = "X"
                drain["end"] = drain["deadline"]
            results.append(
                {
                    "op": "dg",
                    "id": backend_id,
                    "state": drain["state"],
                    "connections": record["conns"],
                    "start": drain["start"],
                    "end": drain["end"],
                    "deadline": drain["deadline"],
                    "forced": drain["forced"],
                }
            )

        elif op[0] == "dq":
            # 全池排空投影（只读）：按后端加入顺序列出 D/X 后端，仅推进
            # 时钟（已在循环开头完成），不改任何状态。due 仅在 D 且
            # now>=deadline 为 true；D 的 remaining=max(deadline-now,0)，
            # X 的 remaining 恒为 0。时间 O(B)、结果空间 O(B)。
            _, now = op
            items = []
            for backend_id, record in backends.items():
                drain = record["drain"]
                state = drain["state"]
                if state not in ("D", "X"):
                    continue
                if state == "D":
                    due = now >= drain["deadline"]
                    remaining = max(drain["deadline"] - now, 0)
                else:
                    due = False
                    remaining = 0
                items.append(
                    {
                        "id": backend_id,
                        "state": state,
                        "connections": record["conns"],
                        "start": drain["start"],
                        "deadline": drain["deadline"],
                        "end": drain["end"],
                        "due": due,
                        "remaining": remaining,
                        "forced": drain["forced"],
                    }
                )
            results.append({"op": "dq", "now": now, "items": items})

        elif op[0] == "dx":
            # 全池批量到期排空：处理集合为 D 且 deadline<=now 的后端，按
            # 加入顺序逐个处理；每个后端的连接按全局建连顺序关闭。关闭沿用
            # dg 强制到期的联动语义（删除活动连接与端点快照、并发归零、转
            # X、end=deadline、forced=本次关闭数），但不消费等待队列、令牌
            # 或配额。先按建连顺序单遍分组（O(C)），再按加入序遍历后端
            # （O(B)），整体 O(B+C)；处理中不存在失败路径，天然原子。
            _, now = op
            due_backends = [
                (backend_id, record)
                for backend_id, record in backends.items()
                if record["drain"]["state"] == "D"
                and record["drain"]["deadline"] <= now
            ]
            # 单遍按后端分组 cids，组内保持全局建连顺序。
            conn_groups = {}
            for cid, connection in connections.items():
                conn_groups.setdefault(connection[0], []).append(cid)
            result_backends = []
            closed_total = 0
            for backend_id, record in due_backends:
                cids = conn_groups.pop(backend_id, [])
                for cid in cids:
                    del connections[cid]
                    conn_endpoints.pop(cid, None)
                record["conns"] = 0
                drain = record["drain"]
                drain["forced"] = len(cids)
                drain["state"] = "X"
                drain["end"] = drain["deadline"]
                result_backends.append(
                    {
                        "id": backend_id,
                        "deadline": drain["deadline"],
                        "cids": cids,
                    }
                )
                closed_total += len(cids)
            results.append(
                {
                    "op": "dx",
                    "now": now,
                    "backends": result_backends,
                    "closed": closed_total,
                }
            )

        elif op[0] == "ls":
            _, scope, bucket_id, r, b, now = op
            # B 桶挂在现存后端上；未知后端优先于其它检查报 BACKEND。
            if scope == "B" and bucket_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            key_pair = (scope, bucket_id)
            bucket = buckets.get(key_pair)
            if bucket is None:
                # 新桶以满令牌起步。
                buckets[key_pair] = {
                    "r": r,
                    "b": b,
                    "t": b,
                    "at": now,
                    "last": (r, b, now),
                }
            else:
                if bucket["last"] == (r, b, now):
                    # 同 (r, b, now) 重报幂等，不补令牌、不推进时钟。
                    results.append({"op": "ls", "ok": True})
                    continue
                # 其余一律按重配置处理：t=b、at=now。
                bucket["r"] = r
                bucket["b"] = b
                bucket["t"] = b
                bucket["at"] = now
                bucket["last"] = (r, b, now)
            results.append({"op": "ls", "ok": True})

        elif op[0] == "la":
            _, c, s, key, bc, cc, sc, now = op
            # 先按 route 选后端（未配环或无可选后端报 STATE），再检查
            # 该后端/客户端/服务类三个桶与对应固定窗口配额；未配置即不
            # 限制也不扣减。ss 后按三键限时粘性规则（以本操作的 now），
            # 未 ss 沿用旧二键语义。
            backend_id, _, _, _, _ = select_route(
                key, now if sticky_ttl is not None else None
            )
            chosen = []
            chosen_quotas = []
            for scope, bucket_id, cost in (
                ("B", backend_id, bc),
                ("C", c, cc),
                ("S", s, sc),
            ):
                bucket = buckets.get((scope, bucket_id))
                if bucket is not None:
                    chosen.append((bucket, cost))
                quota = quotas.get((scope, bucket_id))
                if quota is not None:
                    chosen_quotas.append((quota, cost))
            for bucket, _ in chosen:
                refill(bucket, now)
            for quota, _ in chosen_quotas:
                roll_quota(quota, now)
            # 各在配桶均有 t>=对应成本且各在配配额均有 used+成本<=limit 才
            # 原子扣减；任一不足则 RATE/6：无 stdout、整批原子。
            if (
                not all(bucket["t"] >= cost for bucket, cost in chosen)
                or not all(
                    quota["used"] + cost <= quota["limit"]
                    for quota, cost in chosen_quotas
                )
            ):
                fail(EXIT_RATE, "RATE")
            for bucket, cost in chosen:
                bucket["t"] -= cost
            for quota, cost in chosen_quotas:
                quota["used"] += cost
            results.append({"op": "la", "backend": backend_id, "ok": True})

        elif op[0] == "qs":
            _, scope, quota_id, limit, span, now = op
            # B 配额挂在现存后端上；未知后端优先于其它检查报 BACKEND。
            if scope == "B" and quota_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            key_pair = (scope, quota_id)
            quota = quotas.get(key_pair)
            if quota is None:
                # 新配额自当前窗口起步，已用为零。
                quotas[key_pair] = {
                    "limit": limit,
                    "span": span,
                    "window": now // span,
                    "used": 0,
                    "last": (limit, span, now),
                }
            else:
                if quota["last"] == (limit, span, now):
                    # 同 (limit, span, now) 重报幂等，不推进窗口、不清已用。
                    results.append({"op": "qs", "ok": True})
                    continue
                # 其余一律按重配置处理：window=now//span、used=0。
                quota["limit"] = limit
                quota["span"] = span
                quota["window"] = now // span
                quota["used"] = 0
                quota["last"] = (limit, span, now)
            results.append({"op": "qs", "ok": True})

        elif op[0] == "qg":
            _, scope, quota_id, now = op
            quota = quotas.get((scope, quota_id))
            if quota is None:
                # 查询未配置配额报 STATE。
                fail(EXIT_STATE, "STATE")
            # 跨窗先置 window=now//span 并清 used，再读出不消费。
            roll_quota(quota, now)
            results.append(
                {
                    "op": "qg",
                    "scope": scope,
                    "id": quota_id,
                    "limit": quota["limit"],
                    "span": quota["span"],
                    "window": quota["window"],
                    "used": quota["used"],
                    "remaining": quota["limit"] - quota["used"],
                }
            )

        elif op[0] == "lg":
            _, scope, bucket_id, now = op
            bucket = buckets.get((scope, bucket_id))
            if bucket is None:
                # 查询未配置桶报 STATE。
                fail(EXIT_STATE, "STATE")
            refill(bucket, now)
            results.append(
                {
                    "op": "lg",
                    "scope": scope,
                    "id": bucket_id,
                    "r": bucket["r"],
                    "b": bucket["b"],
                    "t": bucket["t"],
                    "at": bucket["at"],
                }
            )

        elif op[0] == "os":
            _, cap, q, ttl = op
            params = (cap, q, ttl)
            if queue_cfg is not None and queue_cfg != params:
                # 异参重配报 STATE（已排队项保留不动）。
                fail(EXIT_STATE, "STATE")
            # 首配或同参重报：同参幂等。
            queue_cfg = params
            results.append({"op": "os", "ok": True})

        elif op[0] == "pc":
            # 每后端接纳容量覆盖：id 须现存（未知报 BACKEND，先于任何状态
            # 检查）；不要求已 os。同值幂等、异值覆盖，赋值即两者同一路径。
            # 调低上限不关连接、不动 FIFO 项，连接释放后自然恢复。O(1)。
            _, backend_id, cap = op
            if backend_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            cap_overrides[backend_id] = cap
            results.append({"op": "pc", "ok": True})

        elif op[0] == "pg":
            # 覆盖查询（只读）：判定顺序 INPUT（解析期）→ 未知 id BACKEND →
            # 未配置覆盖 STATE。connections 为该后端活动连接数，available=
            # max(cap-connections,0)，均为整数。不要求已 os。O(1)。
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            cap = cap_overrides.get(backend_id)
            if cap is None:
                fail(EXIT_STATE, "STATE")
            active_conns = record["conns"]
            results.append(
                {
                    "op": "pg",
                    "id": backend_id,
                    "cap": cap,
                    "connections": active_conns,
                    "available": max(cap - active_conns, 0),
                }
            )

        elif op[0] == "oa":
            _, cid, flow, c, s, key, bc, cc, sc, now = op
            if queue_cfg is None:
                # 未 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            # 路由检查先于 cid 重复判定，与 open 的 STATE 先于 CONNECTION 一致。
            # ss 后按三键限时粘性（以本操作的 now），未 ss 沿用旧二键语义。
            routed = select_route(
                key, now if sticky_ttl is not None else None, fatal=False
            )
            if routed is None:
                # 未 chash 或无可选后端。
                fail(EXIT_STATE, "STATE")
            if cid in connections or cid in wait_queue:
                # 活动或排队中 cid 重复（OrderedDict 键集即成员索引，O(1)）。
                fail(EXIT_CONNECTION, "CONNECTION")
            status, backend_id = evaluate_admit(
                routed[0], cid, flow, c, s, (bc, cc, sc), now
            )
            if status == "admit":
                # 过载分钟历史：立即接纳计入本窗 immediate。
                row = overload_window(now)
                row[0] = min(METRIC_CAP, row[0] + 1)
                if full_mode == "H":
                    # H 模式结果追加 evicted 键：立即接纳为 null。
                    results.append(
                        {
                            "op": "oa",
                            "cid": cid,
                            "state": "A",
                            "backend": backend_id,
                            "evicted": None,
                        }
                    )
                else:
                    results.append(
                        {"op": "oa", "cid": cid, "state": "A", "backend": backend_id}
                    )
            else:
                if bp_cfg is not None and bp_state == "P":
                    # P 态本应排队即报 OVERLOAD：不耗令牌、不入队、无其他变更。
                    fail(EXIT_OVERLOAD, "OVERLOAD")
                evicted_cid = None
                if len(wait_queue) >= queue_cfg[1]:
                    if full_mode == "T":
                        # FIFO 已满，尾拒绝。
                        fail(EXIT_OVERLOAD, "OVERLOAD")
                    # H 模式头淘汰：O(1) 删除 FIFO 队首（OrderedDict 键集即
                    # 成员索引，淘汰 cid 可立即复用），新项随后落队尾。旧项
                    # 未耗令牌、配额，无需返还；入队路由已产生的粘性映射保留。
                    # evicted 封顶累加并更新 last。
                    evicted_cid, evicted_item = wait_queue.popitem(last=False)
                    evict_count = min(METRIC_CAP, evict_count + 1)
                    evict_last = evicted_cid
                    # 排队等待历史：头淘汰按本次 oa.now（离队时刻）记 evicted，
                    # d=now-入队 now 落五桶；失败批次随整批回滚，不在此处理。
                    record_wait("evicted", now, evicted_item[8])
                # 不可用原因历史：Q 入队且所选后端连接数已达 os.cap 时记
                # overload（所选后端来自只含 healthy/C/A 的环，阻塞在此只可能
                # 因令牌不足、固定窗配额不足或连接达 cap；配额不足只入队，
                # 不报 RATE）；P 态/队满尾拒绝的 OVERLOAD 已在上方回滚，不记。
                # 同次至多记一次。
                if backends[routed[0]]["conns"] >= effective_cap(routed[0]):
                    record_reason(routed[0], "overload", now)
                # 三项成本随请求入队，ot 重试时按此成本扣减。新键追加到
                # OrderedDict 队尾，即 FIFO 入队（重复已在上方拒绝）。
                wait_queue[cid] = (cid, flow, c, s, key, bc, cc, sc, now)
                # 过载分钟历史：入队计入本窗 queued，并刷新该窗入队后队长峰值。
                row = overload_window(now)
                row[1] = min(METRIC_CAP, row[1] + 1)
                if len(wait_queue) > row[4]:
                    row[4] = len(wait_queue)
                if bp_cfg is not None and len(wait_queue) >= bp_cfg[1]:
                    # N 态照常入队，队长达到 high 即转 P（滞回上沿）。
                    bp_state = "P"
                if full_mode == "H":
                    # H 模式结果键序 op,cid,state,backend,evicted：Q 时
                    # backend 亦为本次路由选中的 id（与 A 同口径），普通入队
                    # evicted=null，头淘汰时为旧 cid。
                    results.append(
                        {
                            "op": "oa",
                            "cid": cid,
                            "state": "Q",
                            "backend": routed[0],
                            "evicted": evicted_cid,
                        }
                    )
                else:
                    results.append(
                        {"op": "oa", "cid": cid, "state": "Q", "backend": None}
                    )

        elif op[0] == "ot":
            _, now = op
            if queue_cfg is None:
                fail(EXIT_STATE, "STATE")
            ttl = queue_cfg[2]
            # 先删除全部 now >= 入队 now + ttl 的项（pop 保序，其余项 FIFO
            # 相对次序不变）；到期不补充/不扣令牌、不推进或扣减配额。快照
            # items 按 FIFO 遍历，额外空间 O(q)。
            expired = []
            expired_pairs = []
            for queued_cid, item in list(wait_queue.items()):
                if now >= item[8] + ttl:
                    expired.append(queued_cid)
                    # 排队等待历史：过期成功离队按 ot.now 记 expired，
                    # 记录入队 now 供 d=now-入队 now 落五桶。
                    expired_pairs.append(item[8])
                    wait_queue.pop(queued_cid)
            admitted = []
            admitted_pairs = []
            if queue_mode == "S":
                # S 模式：其余各项按 FIFO 各检查一次（逐项快照遍历，各项使用
                # 同一 ot.now，前项扣减对后项可见）。可接纳项沿用路由、容量、
                # 令牌桶与配额规则扣减并建连；阻塞项移至队尾并保持相对次序
                # ——阻塞项原地保留与之等价：被移项按遇到顺序依次落尾，最终
                # 仍按原相对次序排列。路由不可用同视为阻塞，继续检查后项。
                for queued_cid, item in list(wait_queue.items()):
                    status, _ = try_admit(
                        item[0], item[1], item[2], item[3], item[4],
                        (item[5], item[6], item[7]), now,
                    )
                    if status == "admit":
                        wait_queue.pop(queued_cid)
                        admitted.append(queued_cid)
                        # 排队等待历史：接纳成功离队，记录入队 now 供 d=
                        # ot.now-入队 now 落五桶（实际记账在循环外统一进行）。
                        admitted_pairs.append(item[8])
            elif queue_mode == "P":
                # P 模式（老化优先）：到期项已按 FIFO 删除；其余各项按
                # score=min(10^18, p+(ot.now-入队 now)//step) 降序、score
                # 并列按 FIFO 序升序各尝试一次（p 取 qa 登记的服务类基础
                # 优先级，未登记服务类 p=0）。沿用路由、容量、令牌桶与配额
                # 规则，仅接纳才扣减并建连；阻塞项不从 OrderedDict 删除，
                # 故保持原 FIFO 相对序。admitted 按尝试序输出（expired 仍
                # 按 FIFO）。排序 O(q log q)，每次尝试至多一次 route，时间
                # O(q log q + q BV log BV)、额外空间 O(q)。queue_mode 为 P
                # 必已通过 qa（qp 选 P 无登记即 STATE，ci/cb/ca 采用所载
                # F/S），aging_cfg 必非空。
                aging_step, aging_priorities = aging_cfg
                ordered = []
                for fifo_index, (queued_cid, item) in enumerate(
                    wait_queue.items()
                ):
                    base_p = aging_priorities.get(item[3], 0)
                    score = min(
                        METRIC_CAP,
                        base_p + (now - item[8]) // aging_step,
                    )
                    ordered.append((-score, fifo_index, queued_cid, item))
                ordered.sort()
                for _, _, queued_cid, item in ordered:
                    status, _ = try_admit(
                        item[0], item[1], item[2], item[3], item[4],
                        (item[5], item[6], item[7]), now,
                    )
                    if status == "admit":
                        wait_queue.pop(queued_cid)
                        admitted.append(queued_cid)
                        # 排队等待历史：接纳成功离队，记录入队 now 供 d=
                        # ot.now-入队 now 落五桶（实际记账在循环外统一进行）。
                        admitted_pairs.append(item[8])
            else:
                # F 模式：自队首重试接纳，至首个阻塞即停（每个键至多一次
                # route）。逐项以本次 ot 的 now 补充桶并按 window=now//span
                # 推进固定窗，接纳才扣令牌、增 used 并建连；前序接纳的扣减对
                # 后续项可见，首个阻塞项连同其（已推进但未扣减的）桶/配额
                # 状态保留在队内。
                while wait_queue:
                    queued_cid, item = wait_queue.popitem(last=False)
                    # 接纳时刻为本次 ot 的 now（opened_at=now），入队时刻仅用于过期；
                    # 按入队时登记的三项成本扣减，过期不扣。
                    status, _ = try_admit(
                        item[0], item[1], item[2], item[3], item[4],
                        (item[5], item[6], item[7]), now,
                    )
                    if status == "admit":
                        admitted.append(queued_cid)
                        # 排队等待历史：接纳成功离队，记录入队 now 供 d=
                        # ot.now-入队 now 落五桶（实际记账在循环外统一进行）。
                        admitted_pairs.append(item[8])
                    else:
                        # 阻塞（含路由不可用）：连同该项整体放回队首后停止。
                        # 追加到队尾再移至队首，其余项次序保持不变。
                        wait_queue[queued_cid] = item
                        wait_queue.move_to_end(queued_cid, last=False)
                        break
            if bp_cfg is not None and bp_state == "P" and len(wait_queue) <= bp_cfg[0]:
                # 滞回下沿：过期与接纳处理完后，P 态队长 <=low 即转 N。
                bp_state = "N"
            if expired or admitted:
                # 过载分钟历史：本次 ot 的过期/接纳项各计入本窗
                # expired/dequeued（过期不扣令牌，仅计数）。
                row = overload_window(now)
                row[3] = min(METRIC_CAP, row[3] + len(expired))
                row[2] = min(METRIC_CAP, row[2] + len(admitted))
            # 排队等待历史：仅成功离队的过期/接纳项按 ot.now 记账，d=
            # now-入队 now 落五桶；阻塞项（F 模式放回队首、S 模式留队）
            # 未离队不记。
            for enqueue_now in expired_pairs:
                record_wait("expired", now, enqueue_now)
            for enqueue_now in admitted_pairs:
                record_wait("admitted", now, enqueue_now)
            results.append(
                {"op": "ot", "expired": expired, "admitted": admitted}
            )

        elif op[0] == "og":
            if queue_cfg is None:
                fail(EXIT_STATE, "STATE")
            results.append(
                {"op": "og", "queue": list(wait_queue)}
            )

        elif op[0] == "oc":
            _, cid = op
            # 判定顺序：非法键集/cid 已在解析期判 INPUT；未 os 判 STATE；
            # cid 不在等待队列（活动连接或从未存在）判 CONNECTION。全部校验
            # 先于任何变更，失败批次天然不改动队列与背压状态。
            if queue_cfg is None:
                # 未 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            if cid not in wait_queue:
                # 活动中或从未入队均非排队成员。
                fail(EXIT_CONNECTION, "CONNECTION")
            # 成功：O(1) 从任意位置删除，其余项 FIFO 相对次序不变；不扣减或
            # 返还令牌、不建连接；入队路由已产生的粘性映射保留。
            cancelled_item = wait_queue.pop(cid)
            # 排队等待历史：取消成功离队，按当前逻辑时钟（last_now；入队项
            # 必由带 now 的 oa 建立，故时钟不早于入队 now）记 cancelled，
            # d=当前时钟-入队 now 落五桶。
            record_wait("cancelled", last_now, cancelled_item[8])
            if (
                bp_cfg is not None
                and bp_state == "P"
                and len(wait_queue) <= bp_cfg[0]
            ):
                # 取消后滞回下沿：P 态队长 <=low 立即转 N；N 态等其余情形不变。
                bp_state = "N"
            results.append({"op": "oc", "cid": cid, "ok": True})

        elif op[0] == "bp":
            _, low, high = op
            if queue_cfg is None:
                # 未 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            # high <= os.q 留执行期判定（os 已配，queue_cfg[1] 即 q）。
            if high > queue_cfg[1]:
                fail(EXIT_INPUT, "INPUT")
            params = (low, high)
            if bp_cfg == params:
                # 同参重报幂等且不改状态。
                results.append({"op": "bp", "ok": True})
                continue
            # 首配或异参重配：按当前队长 >=high 置 P，否则 N。
            bp_cfg = params
            bp_state = "P" if len(wait_queue) >= high else "N"
            results.append({"op": "bp", "ok": True})

        elif op[0] == "bq":
            if bp_cfg is None:
                # 未 bp 报 STATE。
                fail(EXIT_STATE, "STATE")
            queued = len(wait_queue)
            results.append(
                {
                    "op": "bq",
                    "state": bp_state,
                    "queued": queued,
                    "low": bp_cfg[0],
                    "high": bp_cfg[1],
                    "available": queue_cfg[1] - queued,
                }
            )

        elif op[0] == "qp":
            _, mode = op
            # 登记 ot 的出队策略，仅改策略，不动队列、背压与任何运行态，
            # 不要求已 os。F/S 同值幂等；切 F/S 更新保留的 F/S 并退出 P。
            # P 为老化优先：进入时保留最近 F/S（dequeue_policy），无 qa 登
            # 记报 STATE（先于任何变更，失败批回滚）；P 同值幂等，不改保留
            # 值。ce/cl 在 P 时 queue.dequeue 仍导出保留的 F/S。
            if mode == "P":
                if aging_cfg is None:
                    fail(EXIT_STATE, "STATE")
                queue_mode = "P"
            else:
                queue_mode = mode
                dequeue_policy = mode
            results.append({"op": "qp", "ok": True})

        elif op[0] == "qa":
            # 老化优先级登记：原子替换整份登记 (step, {s: p})，未登记服务
            # 类在 P 式 ot 中按 p=0；不推进时钟、不动队列与任何运行态、不
            # 要求已 os，不随 ce/cl 导出（ci/cb/ca 成功清空）。
            _, step, items = op
            aging_cfg = (step, dict(items))
            results.append({"op": "qa", "ok": True})

        elif op[0] == "rp":
            _, mode = op
            # 登记 FIFO 满载策略：默认 T，同值幂等，异值切换；仅改策略，
            # 不动队列、背压、淘汰计数与任何运行态，不要求已 os。
            full_mode = mode
            results.append({"op": "rp", "ok": True})

        elif op[0] == "rg":
            # 满载策略查询（只读）：登记策略、累计淘汰数（封顶 10^18）与
            # 最近淘汰 cid（无则 null）；不改任何状态，不要求已 os。
            results.append(
                {
                    "op": "rg",
                    "mode": full_mode,
                    "evicted": evict_count,
                    "last": evict_last,
                }
            )

        elif op[0] == "oq":
            # 等待队列只读投影：除共用时钟按 now 推进外不改任何运行态——
            # 不读写粘性映射、不补充/扣减令牌、不推进/扣减配额、不建连、不
            # 记任何分钟历史，失败批次天然回滚。items 按 FIFO，项键序
            # cid,backend,expires,expired,blocked。到期项（now>=入队
            # now+ttl）取 null、true、["E"]；未到期项按其 key/c/s/成本对
            # 当前环、粘性、桶与配额独立只读投影：环内无合格后端（未配环或
            # 无 healthy/熔断 C/排空 A 候选）取 null、false、["R"]，否则
            # backend 为投影目标，blocked 按 C,T,Q 序列出连接达 cap、任一
            # 令牌不足、任一配额不足，无阻塞为 []。各项独立从当前真实桶/配额
            # 投影，前项不影响后项。环仅在队列非空时构建一次并为全项共用：
            # 时间 O(qBV log(BV))，额外空间 O(q+BV)。
            _, now = op
            if queue_cfg is None:
                # 未 os 报 STATE（STATE 前置同 og/oc；时钟倒退已在批前
                # 通用时钟判定中先报 INPUT）。
                fail(EXIT_STATE, "STATE")
            ttl = queue_cfg[2]
            queued = list(wait_queue.values())
            # 当前环仅含 healthy、熔断 C、排空 A 后端；令牌摘要升序列表与环
            # 一同为全项共用快照，每项仅 O(log(BV)) 二分定位哈希点。
            if queued and ring_vnodes is not None:
                shared_ring = build_ring(backends, ring_vnodes)
                shared_digests = [token[0] for token in shared_ring]
            else:
                shared_ring = []
                shared_digests = []
            items = []
            for queued_cid, flow, c, s, key, bc, cc, sc, enqueue_now in queued:
                expires = enqueue_now + ttl
                entry_obj = {
                    "cid": queued_cid,
                    "backend": None,
                    "expires": expires,
                    "expired": False,
                    "blocked": [],
                }
                if now >= expires:
                    # 到期：不查环、粘性、桶与配额。
                    entry_obj["expired"] = True
                    entry_obj["blocked"] = ["E"]
                    items.append(entry_obj)
                    continue
                # ss 后按三键限时粘性（以本次 now），未 ss 沿用二键语义。
                # 排空口径从严：D 态粘性目标同 X 一样不在当前环上，沿环迁移
                # 到合格候选（blocked 无排空标记，无候选即 R）。
                route_now = now if sticky_ttl is not None else None
                target = project_route(
                    key, route_now, drain_strict=True,
                    ring_tokens=shared_ring,
                    ring_digests=shared_digests,
                )
                if target is None:
                    # 环内无合格后端（粘性目标失格且环上无候选，或未配环）。
                    entry_obj["blocked"] = ["R"]
                    items.append(entry_obj)
                    continue
                entry_obj["backend"] = target
                blocked = []
                record = backends[target]
                if record["conns"] >= effective_cap(target):
                    # C：目标后端活动连接数已达其有效容量（已配 pc 用覆盖，
                    # 否则 os.cap）。
                    blocked.append("C")
                # T：按 now 只读补充后，任一在配 B/C/S 桶令牌不足（不回写）。
                token_short = False
                for scope, bucket_id, demand in (
                    ("B", target, bc),
                    ("C", c, cc),
                    ("S", s, sc),
                ):
                    bucket = buckets.get((scope, bucket_id))
                    if bucket is not None:
                        projected = min(
                            bucket["b"],
                            bucket["t"]
                            + (now - bucket["at"]) * bucket["r"],
                        )
                        if projected < demand:
                            token_short = True
                            break
                if token_short:
                    blocked.append("T")
                # Q：推进固定窗（仅投影）后任一在配配额 used+成本>limit。
                quota_short = False
                for scope, bucket_id, demand in (
                    ("B", target, bc),
                    ("C", c, cc),
                    ("S", s, sc),
                ):
                    quota = quotas.get((scope, bucket_id))
                    if quota is not None:
                        window = now // quota["span"]
                        used = (
                            0 if window != quota["window"]
                            else quota["used"]
                        )
                        if used + demand > quota["limit"]:
                            quota_short = True
                            break
                if quota_short:
                    blocked.append("Q")
                entry_obj["blocked"] = blocked
                items.append(entry_obj)
            results.append({"op": "oq", "items": items})

        elif op[0] == "mr":
            _, backend_id, ok, ms, retries, remaps, now = op
            if backend_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            record_metric(backend_id, ok, ms, retries, remaps, now)
            results.append({"op": "mr", "ok": True})

        elif op[0] == "mg":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            window = now // 60
            metrics = record["metrics"].get(window)
            if metrics is None:
                # 查询新窗（含从未 mr）按零计，不写回存储。
                requests = errors = retries = remaps = 0
                latency = [0, 0, 0, 0, 0]
            else:
                requests = metrics[0]
                errors = metrics[1]
                retries = metrics[2]
                remaps = metrics[3]
                latency = metrics[4]
            # qps=requests/60、error_rate=100*errors/requests 均下截两位。
            qps = "%d.%02d" % divmod(requests * 100 // 60, 100)
            if requests == 0:
                error_rate = "0.00"
            else:
                rate = errors * 10000 // requests
                error_rate = "%d.%02d" % divmod(rate, 100)
            # removed 优先级与 ms 共用同一判定：drain > health > circuit >
            # fault，否则 null。
            removed = removed_reason(record, now)
            results.append(
                {
                    "op": "mg",
                    "id": backend_id,
                    "window": window,
                    "requests": requests,
                    "qps": qps,
                    "concurrency": record["conns"],
                    "errors": errors,
                    "error_rate": error_rate,
                    "latency": list(latency),
                    "retries": retries,
                    "remaps": remaps,
                    "removed": removed,
                }
            )

        elif op[0] == "mh":
            _, backend_id, start, end, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = record["metrics"]
            windows = []
            for window in range(start, end + 1):
                metrics = history.get(window)
                if metrics is None:
                    # 空窗：计数与桶为 0，两个比率串为 0.00。
                    requests = errors = retries = remaps = 0
                    latency = [0, 0, 0, 0, 0]
                else:
                    requests = metrics[0]
                    errors = metrics[1]
                    retries = metrics[2]
                    remaps = metrics[3]
                    latency = metrics[4]
                # qps=requests/60、error_rate=100*errors/requests 均下截两位。
                qps = "%d.%02d" % divmod(requests * 100 // 60, 100)
                if requests == 0:
                    error_rate = "0.00"
                else:
                    rate = errors * 10000 // requests
                    error_rate = "%d.%02d" % divmod(rate, 100)
                windows.append(
                    {
                        "window": window,
                        "requests": requests,
                        "qps": qps,
                        "errors": errors,
                        "error_rate": error_rate,
                        "latency": list(latency),
                        "retries": retries,
                        "remaps": remaps,
                    }
                )
            results.append({"op": "mh", "id": backend_id, "windows": windows})

        elif op[0] == "mu":
            # 池级统一观测（只读）：结果键序 op,window,backends；window=
            # now//60；backends 列全部现存后端并按 id 的 UTF-8 字节升序，空
            # 池为 []，项键序
            # id,requests,qps,concurrency,errors,error_rate,latency,removed,
            # retries,remaps。requests/errors/retries/remaps 与五整数
            # latency 取该窗 mh 口径（record["metrics"]），缺窗为零；
            # concurrency/removed 取同 now 的 mg 口径（活动连接数与
            # removed_reason，removed ∈ null/drain/health/circuit/fault）。
            # qps=requests/60、error_rate=100*errors/requests（零请求为 0）
            # 均向下截为两位小数字符串。除共用时钟（循环开头统一推进）外不
            # 写任何状态，失败批次天然回滚。排序 O(B log B)，removed 的
            # fault 判定 bisect 故障时间线 O(log(T+1))，合计时间
            # O(B log B+B log(T+1))、结果空间 O(B)。
            _, now = op
            window = now // 60
            items = []
            for backend_id in sorted(backends, key=encode_backend_id):
                record = backends[backend_id]
                metrics = record["metrics"].get(window)
                if metrics is None:
                    # 查询新窗（含从未 mr）按零计，不写回存储。
                    requests = errors = retries = remaps = 0
                    latency = [0, 0, 0, 0, 0]
                else:
                    requests = metrics[0]
                    errors = metrics[1]
                    retries = metrics[2]
                    remaps = metrics[3]
                    latency = list(metrics[4])
                # qps=requests/60、error_rate=100*errors/requests 均下截两位。
                qps = "%d.%02d" % divmod(requests * 100 // 60, 100)
                if requests == 0:
                    error_rate = "0.00"
                else:
                    rate = errors * 10000 // requests
                    error_rate = "%d.%02d" % divmod(rate, 100)
                items.append(
                    {
                        "id": backend_id,
                        "requests": requests,
                        "qps": qps,
                        "concurrency": record["conns"],
                        "errors": errors,
                        "error_rate": error_rate,
                        "latency": latency,
                        "removed": removed_reason(record, now),
                        "retries": retries,
                        "remaps": remaps,
                    }
                )
            results.append({"op": "mu", "window": window, "backends": items})

        elif op[0] == "lp":
            # 后端延迟分位查询（只读）：未知 id 报 BACKEND，from 早于最近
            # 60 窗下界报 STATE（与 mh 同序）。汇总区间窗的五 latency 桶，
            # 各桶逐项求和并封顶 10^18；samples 为五桶和再封顶 10^18。
            # rank=ceil(p*samples/100)，按桶 0..4 累计（截至 samples）取首个
            # 累计≥rank 者；桶上界依次为 1、10、100、1000、null。samples=0
            # 时 rank=0 且 bucket/upper=null。时间 O(R)、额外空间 O(1)。
            _, backend_id, start, end, percentile, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            buckets = [0, 0, 0, 0, 0]
            history = record["metrics"]
            for window in range(start, end + 1):
                metrics = history.get(window)
                if metrics is not None:
                    latency = metrics[4]
                    for i in range(5):
                        buckets[i] = min(
                            METRIC_CAP, buckets[i] + latency[i]
                        )
            # samples 为五桶和再封顶 10^18。
            samples = min(METRIC_CAP, sum(buckets))
            if samples == 0:
                rank = 0
                chosen = None
            else:
                # rank=ceil(p*samples/100)，按桶累计（截至 samples）取首个
                # 累计≥rank 者；p≤100 故 rank≤samples，必然落在某桶。
                rank = (percentile * samples + 99) // 100
                cumulative = 0
                chosen = None
                for i in range(5):
                    cumulative = min(samples, cumulative + buckets[i])
                    if cumulative >= rank:
                        chosen = i
                        break
            upper = (1, 10, 100, 1000, None)
            results.append(
                {
                    "op": "lp",
                    "id": backend_id,
                    "from": start,
                    "to": end,
                    "p": percentile,
                    "samples": samples,
                    "buckets": buckets,
                    "rank": rank,
                    "bucket": chosen,
                    "upper": None if chosen is None else upper[chosen],
                }
            )

        elif op[0] == "ms":
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            window = now // 60
            history = record["samples"]
            snapshot = history.get(window)
            if snapshot is None:
                # 首次采样该窗：新建该窗快照；时钟非递减，顺带丢弃 60 窗前
                # 的旧窗（每后端至多 60 窗、每窗至多 60 个不同 now，即
                # 3600 样本）。窗聚合 samples/peak 与快照同步维护，供 na
                # O(1) 取窗值，初值 0/0。
                snapshot = {"points": {}, "samples": 0, "peak": 0}
                history[window] = snapshot
                cutoff = window - 59
                for old in [w for w in history if w < cutoff]:
                    del history[old]
            # 采样值为当时的 (活动连接数, removed)，removed 沿用 mg 优先级。
            current = (record["conns"], removed_reason(record, now))
            points = snapshot["points"]
            previous = points.get(now)
            if previous is not None:
                # 同 (id, now)：采样值相同幂等，不同即冲突重报，报 STATE。
                # 幂等重报不重复计数，故 samples/peak 不变（在改聚合前判定，
                # 失败批次天然回滚）。
                if previous != current:
                    fail(EXIT_STATE, "STATE")
            else:
                points[now] = current
                # 新样本：samples 加 1、peak 取窗内并发峰值（均摊 O(1)）。
                snapshot["samples"] += 1
                if current[0] > snapshot["peak"]:
                    snapshot["peak"] = current[0]
            results.append({"op": "ms", "ok": True})

        elif op[0] == "mx":
            _, backend_id, start, end, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current_window = now // 60
            if start < max(0, current_window - 59):
                # from 早于最近 60 窗的下界（同 mh）。
                fail(EXIT_STATE, "STATE")
            history = record["samples"]
            windows = []
            for window in range(start, end + 1):
                snapshot = history.get(window)
                # removed 计数对象固定键序 drain,health,circuit,fault,none；
                # removed=null 的样本计入 none。
                counts = {
                    "drain": 0, "health": 0, "circuit": 0,
                    "fault": 0, "none": 0,
                }
                if not snapshot:
                    # 空窗：samples/peak 与各计数为 0，last 为 null。
                    windows.append(
                        {
                            "window": window,
                            "samples": 0,
                            "peak": 0,
                            "last": None,
                            "removed": counts,
                        }
                    )
                    continue
                # samples/peak 取 ms 同步维护的窗聚合；last 与 removed 仍逐
                # 样本统计（mx 时间 O(R+S) 不变）。内层 dict 按采样先后
                # （共用时钟非递减）保序，末次并发即末项。
                last = 0
                for _, (conns, reason) in snapshot["points"].items():
                    last = conns
                    counts[reason if reason is not None else "none"] += 1
                windows.append(
                    {
                        "window": window,
                        "samples": snapshot["samples"],
                        "peak": snapshot["peak"],
                        "last": last,
                        "removed": counts,
                    }
                )
            results.append({"op": "mx", "id": backend_id, "windows": windows})

        elif op[0] == "rh":
            # 不可用原因分钟历史只读查询：未知 id 报 BACKEND，from 早于最近
            # 60 窗下界报 STATE（与 mh/mx/fh 同序）；不改任何计数，失败批次
            # 天然回滚。返回键序 op,id,windows；windows 覆盖 from..to 并升序，
            # 项键序 window,health,drain,circuit,overload，值为非负整数，
            # 空窗全 0。逐窗拷贝，避免结果被批次内后续记账污染。
            _, backend_id, start, end, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = record["reason_hist"]
            windows = []
            for window in range(start, end + 1):
                counts = history.get(window)
                if counts is None:
                    health = drain = circuit = overload = 0
                else:
                    health = counts["health"][0]
                    drain = counts["drain"][0]
                    circuit = counts["circuit"][0]
                    overload = counts["overload"][0]
                windows.append(
                    {
                        "window": window,
                        "health": health,
                        "drain": drain,
                        "circuit": circuit,
                        "overload": overload,
                    }
                )
            results.append({"op": "rh", "id": backend_id, "windows": windows})

        elif op[0] == "rt":
            # 原因事件时刻只读查询：校验次序同 rh——未知 id 报 BACKEND，
            # from 早于最近 60 窗下界报 STATE；不改任何历史，失败批次天然
            # 回滚。返回键序 op,id,windows；windows 覆盖 from..to 并升序，
            # 项键序 window,health,drain,circuit,overload，四因各为键序
            # count,first,last 的对象：count 为该窗该因事件数（封顶 10^18），
            # first/last 为首次/最近事件的 now（封顶后 last 仍更新），无事件
            # 取 0,null,null。逐项拷贝当下值，避免结果被批次内后续记账污染。
            _, backend_id, start, end, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = record["reason_hist"]
            windows = []
            for window in range(start, end + 1):
                counts = history.get(window)
                entry = {"window": window}
                for name in ("health", "drain", "circuit", "overload"):
                    # 无该窗或该因无事件：0,null,null；first 仅首次事件时
                    # 记录，last 每次事件更新（计数封顶后亦然）。
                    if counts is None or counts[name][0] == 0:
                        entry[name] = {"count": 0, "first": None, "last": None}
                    else:
                        count, first, last = counts[name]
                        entry[name] = {
                            "count": count, "first": first, "last": last,
                        }
                windows.append(entry)
            results.append({"op": "rt", "id": backend_id, "windows": windows})

        elif op[0] == "rr":
            # 重试/重映射时刻历史只读查询：校验次序同 rt——未知 id 报
            # BACKEND，from 早于最近 60 窗下界报 STATE；不改任何历史，失败
            # 批次天然回滚。返回键序 op,id,windows；windows 覆盖闭区间
            # from..to 并升序，项键序 window,retries,remaps，两项各为键序
            # count,first,last 的对象：count 为该窗该项写入的正数之和（封顶
            # 10^18），first/last 为首次/最近写入的 now（封顶后 last 仍更
            # 新），无记录取 0,null,null。逐项拷贝当下值，避免结果被批次
            # 内后续记账污染。
            _, backend_id, start, end, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = record["retry_hist"]
            windows = []
            for window in range(start, end + 1):
                entry = history.get(window)
                item = {"window": window}
                for name in ("retries", "remaps"):
                    # 无该窗或该项无写入：0,null,null；first 仅首次写入时
                    # 记录，last 每次写入更新（计数封顶后亦然）。
                    if entry is None or entry[name][0] == 0:
                        item[name] = {"count": 0, "first": None, "last": None}
                    else:
                        count, first, last = entry[name]
                        item[name] = {
                            "count": count, "first": first, "last": last,
                        }
                windows.append(item)
            results.append({"op": "rr", "id": backend_id, "windows": windows})

        elif op[0] == "ra":
            # 全池不可用原因汇总（只读）：from 早于最近 60 窗下界报 STATE
            # （同 rh）；不改任何计数，失败批次天然回滚。返回键序
            # op,windows；windows 覆盖 from..to 并升序，项键序
            # window,backends,total；backends 按加入序列全部现存后端，项键
            # 序 id,health,drain,circuit,overload（缺窗全 0）；total 键序
            # health,drain,circuit,overload，为全池逐项求和并封顶 10^18；
            # 空池 backends 为空、total 全 0。逐窗逐键拷贝，避免结果被批次
            # 内后续记账污染。
            _, start, end, now = op
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            windows = []
            for window in range(start, end + 1):
                total = {"health": 0, "drain": 0, "circuit": 0, "overload": 0}
                entries = []
                for backend_id, record in backends.items():
                    counts = record["reason_hist"].get(window)
                    entry = {"id": backend_id}
                    for reason in ("health", "drain", "circuit", "overload"):
                        # 缺窗：该项为 0，求和不受影响。
                        value = 0 if counts is None else counts[reason][0]
                        entry[reason] = value
                        total[reason] = min(METRIC_CAP, total[reason] + value)
                    entries.append(entry)
                windows.append(
                    {
                        "window": window,
                        "backends": entries,
                        "total": total,
                    }
                )
            results.append({"op": "ra", "windows": windows})

        elif op[0] == "ma":
            # 全池请求指标历史（只读）：from 早于最近 60 窗下界报 STATE
            # （同 ra）；不改任何度量，失败批次天然回滚。返回键序
            # op,windows；windows 覆盖 from..to 并升序，项键序
            # window,requests,qps,errors,error_rate,latency,retries,
            # remaps：四项计数与五 latency 桶为全部现存后端同窗 mh 数据逐项
            # 求和并封顶 10^18（已删除后端随记录消失不汇总，同 id 重加只含
            # 新实例的空历史，ci 重建已清度量）；qps、error_rate 按 mh 口径
            # 下截两位；缺窗或空池计数与桶为 0、两字符串为 0.00。逐项拷贝，
            # 避免结果被批次内后续记账污染。
            _, start, end, now = op
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            windows = []
            for window in range(start, end + 1):
                requests = errors = retries = remaps = 0
                latency = [0, 0, 0, 0, 0]
                for record in backends.values():
                    metrics = record["metrics"].get(window)
                    if metrics is None:
                        # 缺窗：该后端本窗不计入。
                        continue
                    requests = min(METRIC_CAP, requests + metrics[0])
                    errors = min(METRIC_CAP, errors + metrics[1])
                    retries = min(METRIC_CAP, retries + metrics[2])
                    remaps = min(METRIC_CAP, remaps + metrics[3])
                    for i in range(5):
                        latency[i] = min(
                            METRIC_CAP, latency[i] + metrics[4][i]
                        )
                # qps=requests/60、error_rate=100*errors/requests 均下截两位。
                qps = "%d.%02d" % divmod(requests * 100 // 60, 100)
                if requests == 0:
                    error_rate = "0.00"
                else:
                    rate = errors * 10000 // requests
                    error_rate = "%d.%02d" % divmod(rate, 100)
                windows.append(
                    {
                        "window": window,
                        "requests": requests,
                        "qps": qps,
                        "errors": errors,
                        "error_rate": error_rate,
                        "latency": latency,
                        "retries": retries,
                        "remaps": remaps,
                    }
                )
            results.append({"op": "ma", "windows": windows})

        elif op[0] == "oh":
            # 过载分钟历史（只读）：未 os 报 STATE；from 早于最近 60 窗下界
            # 报 STATE（同 ra/ma）；不改任何计数，失败批次天然回滚。返回键序
            # op,windows；windows 覆盖 from..to 并升序，项键序
            # window,immediate,queued,dequeued,expired,peak，空窗全 0。
            # 逐窗拷贝，避免结果被批次内后续记账污染。
            _, start, end, now = op
            if queue_cfg is None:
                # 未配置 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            windows = []
            for window in range(start, end + 1):
                row = overload_hist.get(window)
                if row is None:
                    immediate = queued = dequeued = expired = peak = 0
                else:
                    immediate, queued, dequeued, expired, peak = row
                windows.append(
                    {
                        "window": window,
                        "immediate": immediate,
                        "queued": queued,
                        "dequeued": dequeued,
                        "expired": expired,
                        "peak": peak,
                    }
                )
            results.append({"op": "oh", "windows": windows})

        elif op[0] == "wh":
            # 排队等待历史（只读）：未 os 报 STATE；from 早于最近 60 窗下界
            # 报 STATE（同 oh/ra/ma）；除推进共用时钟外不改任何状态，失败
            # 批次天然回滚。返回键序 op,windows；windows 覆盖 from..to 并升
            # 序，项键序 window,admitted,expired,cancelled,evicted，四项均为
            # 5 个非负整数（等待时长桶 ≤0、≤1、≤10、≤100、>100），空窗全
            # 零。逐窗拷贝，避免结果被批次内后续记账污染。
            _, start, end, now = op
            if queue_cfg is None:
                # 未配置 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            empty = [0, 0, 0, 0, 0]
            windows = []
            for window in range(start, end + 1):
                row = wait_hist.get(window)
                if row is None:
                    admitted = expired = cancelled = evicted = empty
                else:
                    admitted = list(row["admitted"])
                    expired = list(row["expired"])
                    cancelled = list(row["cancelled"])
                    evicted = list(row["evicted"])
                windows.append(
                    {
                        "window": window,
                        "admitted": admitted,
                        "expired": expired,
                        "cancelled": cancelled,
                        "evicted": evicted,
                    }
                )
            results.append({"op": "wh", "windows": windows})

        elif op[0] == "wp":
            # 排队等待分位查询（只读）：未 os 报 STATE；from 早于最近 60 窗
            # 下界报 STATE（同 wh）；除推进共用时钟外不改任何状态，失败批次
            # 天然回滚。汇总闭区间内该 kind 五桶（逐桶封顶 10^18），
            # samples 为桶和再封顶；samples>0 时 rank=ceil(p*samples/100)，
            # bucket 取累计首次不小于 rank 的桶（0..4），upper 依次为
            # 0/1/10/100/null；无样本 rank=0、bucket 与 upper 为 null。返回
            # 键序 op,kind,from,to,p,samples,buckets,rank,bucket,upper。
            _, kind_code, start, end, percentile, now = op
            if queue_cfg is None:
                # 未配置 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            kind_name = {
                "A": "admitted",
                "E": "expired",
                "C": "cancelled",
                "V": "evicted",
            }[kind_code]
            buckets = [0, 0, 0, 0, 0]
            for window in range(start, end + 1):
                row = wait_hist.get(window)
                if row is None:
                    # 空窗全 0。
                    continue
                counts = row[kind_name]
                for index in range(5):
                    buckets[index] = min(
                        METRIC_CAP, buckets[index] + counts[index]
                    )
            # samples 为桶和，再封顶 10^18。
            samples = min(METRIC_CAP, sum(buckets))
            if samples > 0:
                # ceil(p*samples/100)：p≥1 故 rank≥1。
                rank = (percentile * samples + 99) // 100
                cumulative = 0
                bucket = None
                for index in range(5):
                    cumulative += buckets[index]
                    if cumulative >= rank:
                        bucket = index
                        break
                upper = (0, 1, 10, 100, None)[bucket]
            else:
                rank = 0
                bucket = None
                upper = None
            results.append(
                {
                    "op": "wp",
                    "kind": kind_code,
                    "from": start,
                    "to": end,
                    "p": percentile,
                    "samples": samples,
                    "buckets": buckets,
                    "rank": rank,
                    "bucket": bucket,
                    "upper": upper,
                }
            )

        elif op[0] == "wa":
            # 排队等待分位告警（各 kind 独立状态机，全池一份）：首评固化
            # (p,hi,lo,n) 并自 N 态起评；此后参数须相同且 w 仅同前（同窗同
            # 参原样返回首评结果，不推进时钟外的状态机）或 +1，变参或跳窗
            # （含回退）报 STATE。窗口越界（w 未结束或超出最近 60 窗保留
            # 下界）同样 STATE；窗口判定先于参数/缓存，与 pa/ea 同序；未 os
            # 先报 STATE（同 wh/wp）。窗值取 wait_hist 中该 kind 在 [w,w]
            # 单窗五桶按 wp 规则计算的 samples、bucket、upper（单窗即区间
            # [w,w] 的 wp；无样本 0、null、null）。N 态连续 n 窗
            # bucket>=hi 转 A，A 态连续 n 窗 bucket<=lo 转 N；方向不符与转
            # 换后连续数清 0；转换时 changed=true。返回键序
            # op,kind,w,p,state,samples,bucket,upper,run,changed。wa 记账
            # 时间 O(1)、额外空间 O(1)。
            _, kind_code, w, percentile, hi, lo, n, now = op
            if queue_cfg is None:
                # 未配置 os 报 STATE。
                fail(EXIT_STATE, "STATE")
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            entry = wait_alerts.get(kind_code)
            if entry is not None:
                if (percentile, hi, lo, n) != (
                    entry["p"], entry["hi"], entry["lo"], entry["n"]
                ):
                    # 变参。
                    fail(EXIT_STATE, "STATE")
                if w == entry["w"]:
                    # 同窗同参重报：原样返回首评结果，不推进状态机。
                    results.append(dict(entry["result"]))
                    continue
                if w != entry["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            kind_name = {
                "A": "admitted",
                "E": "expired",
                "C": "cancelled",
                "V": "evicted",
            }[kind_code]
            row = wait_hist.get(w)
            if row is None:
                # 无样本：三者为 0、null、null，run 清零（沿 wp 口径）。
                samples = 0
                bucket = None
                upper = None
            else:
                kind_buckets = list(row[kind_name])
                # 单窗无需封顶求和：桶计数本身已封顶 10^18，samples 沿用
                # wp 的五桶和再封顶。
                samples = min(METRIC_CAP, sum(kind_buckets))
                if samples == 0:
                    # 该 kind 单窗无样本：0、null、null。
                    bucket = None
                    upper = None
                else:
                    # rank=ceil(p*samples/100)，按桶 0..4 累计（截至
                    # samples）取首个累计≥rank 者；p≤100 故 rank≤samples。
                    rank = (percentile * samples + 99) // 100
                    cumulative = 0
                    bucket = None
                    for i in range(5):
                        cumulative = min(
                            samples, cumulative + kind_buckets[i]
                        )
                        if cumulative >= rank:
                            bucket = i
                            break
                    # 等待时长桶上界依次为 0、1、10、100、null（同 wp）。
                    upper = (0, 1, 10, 100, None)[bucket]
            if entry is None:
                state = "N"
                run_count = 0
            else:
                state = entry["state"]
                run_count = entry["run"]
            changed = False
            if state == "N":
                if bucket is not None and bucket >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符（含无样本 bucket=null）：连续数清 0。
                    run_count = 0
            else:
                if bucket is not None and bucket <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符（含无样本 bucket=null）：连续数清 0。
                    run_count = 0
            result = {
                "op": "wa",
                "kind": kind_code,
                "w": w,
                "p": percentile,
                "state": state,
                "samples": samples,
                "bucket": bucket,
                "upper": upper,
                "run": run_count,
                "changed": changed,
            }
            wait_alerts[kind_code] = {
                "p": percentile,
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            results.append(result)

        elif op[0] == "lh":
            # 多维限流历史（只读）：B 的未知 id 报 BACKEND（先于状态检查）；
            # 该 id 既无在配桶也无在配配额、from 早于最近 60 窗下界均报
            # STATE；不改任何计数，失败批次天然回滚。返回键序
            # op,scope,id,windows；windows 覆盖 from..to 并升序，项键序
            # window,admitted,units,token,quota，空窗全 0。逐窗拷贝，避免
            # 结果被批次内后续记账污染。
            _, scope, hist_id, start, end, now = op
            if scope == "B" and hist_id not in backends:
                fail(EXIT_BACKEND, "BACKEND")
            key_pair = (scope, hist_id)
            if key_pair not in buckets and key_pair not in quotas:
                # 该 id 既无桶也无配额：无历史可查。
                fail(EXIT_STATE, "STATE")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = limit_hist.get(key_pair)
            windows = []
            for window in range(start, end + 1):
                if history is None:
                    admitted = units = token = quota = 0
                else:
                    row = history.get(window)
                    if row is None:
                        admitted = units = token = quota = 0
                    else:
                        admitted, units, token, quota = row
                windows.append(
                    {
                        "window": window,
                        "admitted": admitted,
                        "units": units,
                        "token": token,
                        "quota": quota,
                    }
                )
            results.append(
                {"op": "lh", "scope": scope, "id": hist_id, "windows": windows}
            )

        elif op[0] == "lt":
            # 限流热点查询（只读）：汇总该 scope 全部在配（有桶或配额）标识
            # 在 from..to 闭区间内 lh 的 admitted、units、token、quota；无任一
            # 在配标识、from 早于最近 60 窗下界均报 STATE；不改任何计数，
            # 失败批次天然回滚。blocked=token+quota（封顶 10^18）；按
            # blocked 降序、id 的 UTF-8 字节升序取至多 k 项。返回键序
            # op,scope,from,to,items；items 项键序
            # id,admitted,units,token,quota,blocked。逐窗求和并封顶，拷贝出
            # 的计数避免结果被批次内后续记账污染。
            _, scope, start, end, limit_k, now = op
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            # 在配标识：该 scope 下有桶或配额者（B 的桶/配额均挂现存后端）。
            active_ids = set()
            for pair_scope, ident in buckets:
                if pair_scope == scope:
                    active_ids.add(ident)
            for pair_scope, ident in quotas:
                if pair_scope == scope:
                    active_ids.add(ident)
            if not active_ids:
                # 该 scope 无在配标识：无热点可查。
                fail(EXIT_STATE, "STATE")
            items = []
            for ident in active_ids:
                admitted = units = token = quota = 0
                history = limit_hist.get((scope, ident))
                if history is not None:
                    for window in range(start, end + 1):
                        row = history.get(window)
                        if row is None:
                            # 空窗全 0。
                            continue
                        admitted = min(METRIC_CAP, admitted + row[0])
                        units = min(METRIC_CAP, units + row[1])
                        token = min(METRIC_CAP, token + row[2])
                        quota = min(METRIC_CAP, quota + row[3])
                blocked = min(METRIC_CAP, token + quota)
                items.append(
                    {
                        "id": ident,
                        "admitted": admitted,
                        "units": units,
                        "token": token,
                        "quota": quota,
                        "blocked": blocked,
                    }
                )
            # blocked 降序、平手按 id 的 UTF-8 字节升序；取至多 k 项。
            items.sort(
                key=lambda item: (-item["blocked"], item["id"].encode("utf-8"))
            )
            del items[limit_k:]
            results.append(
                {
                    "op": "lt",
                    "scope": scope,
                    "from": start,
                    "to": end,
                    "items": items,
                }
            )

        elif op[0] == "mo":
            # 全池增量快照。同 seq、now 重报原样返回缓存结果，不推进时钟、
            # 不重存基线（重报可能晚于推进时钟的其它操作，故须在共用时钟
            # 倒退判定之前 continue，与 ls 同参重报同式）。
            _, seq, now = op
            if (
                mo_cache is not None
                and mo_cache[0] == seq
                and mo_cache[1] == now
            ):
                results.append(mo_cache[2])
                continue
            # 时钟倒退先于 seq 序列关系，与其余 now 操作同序判 INPUT。
            if last_now is not None and now < last_now:
                fail(EXIT_INPUT, "INPUT")
            # seq 序列：首次必须为 1，此后逐次递增 1；同 seq 异 now（重报
            # 未命中上方缓存）、倒序或跳号均 STATE。
            if seq != mo_seq:
                fail(EXIT_STATE, "STATE")
            window = now // 60
            # 先按当前状态构造完整结果：四项计数与五延迟桶为当前窗 mr
            # 累计减基线（mo_base.window 与本窗相同才作基线，首次或跨窗
            # 基线为零）；concurrency、removed 取同 now 的 mg 现值。
            items = []
            for backend_id, record in backends.items():
                metrics = record["metrics"].get(window)
                if metrics is None:
                    cur_req = cur_err = cur_retries = cur_remaps = 0
                    cur_latency = [0, 0, 0, 0, 0]
                else:
                    cur_req = metrics[0]
                    cur_err = metrics[1]
                    cur_retries = metrics[2]
                    cur_remaps = metrics[3]
                    cur_latency = metrics[4]
                base = record["mo_base"]
                if base[0] == window:
                    requests = cur_req - base[1]
                    errors = cur_err - base[2]
                    retries = cur_retries - base[3]
                    remaps = cur_remaps - base[4]
                    latency = [
                        cur_latency[i] - base[5][i] for i in range(5)
                    ]
                else:
                    # 首次快照或与上次 mo 跨窗：增量按零基线。
                    requests = cur_req
                    errors = cur_err
                    retries = cur_retries
                    remaps = cur_remaps
                    latency = list(cur_latency)
                # qps=requests/60、error_rate=100*errors/requests 均下截两位。
                qps = "%d.%02d" % divmod(requests * 100 // 60, 100)
                if requests == 0:
                    error_rate = "0.00"
                else:
                    rate = errors * 10000 // requests
                    error_rate = "%d.%02d" % divmod(rate, 100)
                items.append(
                    {
                        "id": backend_id,
                        "requests": requests,
                        "qps": qps,
                        "concurrency": record["conns"],
                        "errors": errors,
                        "error_rate": error_rate,
                        "latency": latency,
                        "retries": retries,
                        "remaps": remaps,
                        "removed": removed_reason(record, now),
                    }
                )
            result = {
                "op": "mo",
                "seq": seq,
                "window": window,
                "backends": items,
            }
            # 构造成功后提交：推进共用时钟，按现存后端把当前窗 mr 累计
            # （缺窗为零）存为新基线，推进游标并缓存结果。
            last_now = now
            for backend_id, record in backends.items():
                metrics = record["metrics"].get(window)
                if metrics is None:
                    record["mo_base"] = [
                        window, 0, 0, 0, 0, [0, 0, 0, 0, 0]
                    ]
                else:
                    record["mo_base"] = [
                        window,
                        metrics[0],
                        metrics[1],
                        metrics[2],
                        metrics[3],
                        list(metrics[4]),
                    ]
            mo_cache = (seq, now, result)
            mo_seq = seq + 1
            results.append(result)

        elif op[0] == "ce":
            # 导出纯配置（登记值），不含任何运行态。
            results.append({"op": "ce", "config": export_config()})

        elif op[0] == "ct":
            # 配置指纹（只读，不推进时钟）：当前 ce.config 规范化
            # version=11 对象的 SHA-256，键序 op,digest。
            results.append(
                {"op": "ct", "digest": config_digest(export_config())}
            )

        elif op[0] == "ci":
            _, config, base, now = op
            # B 限流、B 配额、faults 与 capacities 引用未知后端：BACKEND，
            # 先于活动状态判定。
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            if base is not None and base != config_digest(export_config()):
                # 乐观并发保护：base 与操作前指纹不等即拒绝热加载，
                # 先于活动连接或排队检查。
                fail(EXIT_STATE, "STATE")
            # 有活动连接或排队项时拒绝热加载：STATE。
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")
            if next_rev > 10 ** 18:
                # rev 耗尽：不分配、不改历史。
                fail(EXIT_STATE, "STATE")
            # 校验全部通过，原子替换配置并以 now 重建默认运行态（v7+ 同步
            # 载入 faults 时间线，故障运行态统计/基线仍重置；queue 策略随
            # 配置载入，v1..v8 为默认 F/T；capacities 显式覆盖随配置原子
            # 替换，旧版或 [] 清空；硬时限按 lifetime 载入，null 或 v1..v10
            # 清除）。
            before_export = export_config()
            before_digest = config_digest(before_export)
            apply_config(config, now)
            # 成功后把规范化 version=11 配置存为提交：rev 从 1 起递增，
            # 仅保留最近 16 条；失败不分配、不改历史。export_config 产出
            # 全新结构（lifetime 为刚载入的登记值），提交后的 qp/rp/pc/tm
            # 修改不影响已存快照。
            new_snapshot = export_config()
            commit_history.append((next_rev, new_snapshot))
            after_digest = config_digest(new_snapshot)
            new_rev = next_rev
            next_rev += 1
            if len(commit_history) > 16:
                commit_history.pop(0)
            # ci 成功清除既有配置预约。
            reservation = None
            # ci 成功同时清除全池端点切换预约；生命周期审计：清除现存预约
            # 记 CLEAR（before 为被清除预约身份），本无预约不记。
            if ep_switch is not None:
                ep_audit_append(
                    "CLEAR", now, (ep_switch[3], ep_switch[2]), None,
                )
            ep_switch = None
            # 审计：成功并分配新 rev 时按 rev 升序追加（仅留最近 64 条），
            # section 恒 null；before 为操作前指纹，after 为新提交指纹。
            # 同一事件内固化段级差异（仅指纹，按顶层键序）。
            audit_events.append(
                {
                    "rev": new_rev,
                    "now": now,
                    "kind": "ci",
                    "section": None,
                    "before": before_digest,
                    "after": after_digest,
                }
            )
            audit_sections.append(
                tuple(config_section_changes(before_export, new_snapshot))
            )
            results.append({"op": "ci", "ok": True})

        elif op[0] == "cv":
            _, config, now = op
            # B 限流、B 配额、faults 与 capacities 引用未知后端：BACKEND，
            # 先于活动状态判定（同 ci 的错误优先级）。
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 预检不应用配置：活动连接或排队项仅令 applicable=false，不报
            # STATE；成功仅推进时钟（已在共用时钟块完成），配置、连接、
            # 队列、粘性、桶、配额、指标、告警、rev 与提交历史均不变。
            results.append(
                {
                    "op": "cv",
                    "applicable": not connections and not wait_queue,
                    "connections": len(connections),
                    "queued": len(wait_queue),
                    "config": export_normalized_config(config),
                }
            )

        elif op[0] == "cd":
            _, config, now = op
            # B 限流、B 配额、faults 与 capacities 引用未知后端：BACKEND，
            # 判定顺序与优先级同 cv；活动连接或排队项不影响预览，不报 STATE。
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 预览不应用候选：比较当前 ce.config 与候选规范化配置的
            # backends。base/target 为 ct 摘要（规范化 version=11 对象的
            # SHA-256）；added/removed 为 id 数组，分别按候选/当前加入序；
            # changed 按候选序列出共有且变化者，fields 按
            # weight,d,fail,success,circuit,drain,endpoint 列差异；order
            # 为两侧完整 id 序列是否不同。成功仅推进时钟（已在共用时钟块
            # 完成），其余状态不变。O(N)，N 为规范化配置大小。
            current_export = export_config()
            current_by_id = {item["id"]: item for item in current_export["backends"]}
            candidate_export = export_normalized_config(config)
            candidate_ids = [item["id"] for item in candidate_export["backends"]]
            current_ids = [item["id"] for item in current_export["backends"]]
            changed = []
            for item in candidate_export["backends"]:
                current_item = current_by_id.get(item["id"])
                if current_item is None:
                    continue
                fields = [
                    field
                    for field in (
                        "weight", "d", "fail", "success",
                        "circuit", "drain", "endpoint",
                    )
                    if item[field] != current_item[field]
                ]
                if fields:
                    changed.append({"id": item["id"], "fields": fields})
            results.append(
                {
                    "op": "cd",
                    "base": config_digest(current_export),
                    "target": config_digest(candidate_export),
                    "added": [
                        backend_id
                        for backend_id in candidate_ids
                        if backend_id not in current_by_id
                    ],
                    "removed": [
                        backend_id
                        for backend_id in current_ids
                        if backend_id not in config_backend_ids
                    ],
                    "changed": changed,
                    "order": candidate_ids != current_ids,
                }
            )

        elif op[0] == "pd":
            _, config, now = op
            # B 限流、B 配额、faults 与 capacities 引用未知候选后端：
            # BACKEND，判定顺序与优先级同 cv；活动连接或排队项不影响预览，
            # 不报 STATE。
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 预览不应用候选：比较当前 ce.config 与候选规范化 version=11
            # 配置的非 backends 部分。base/target 为两份完整规范化配置的 ct
            # 摘要；changes 按 vnodes,limits,overload,sticky,idle,
            # backpressure,scheduler,faults,quotas,queue,capacities,lifetime
            # 列差异项（lifetime 在 capacities 后、末置），项键序
            # section,before,after，before/after 复用对应 ce 字段的导出值
            # （类型、键序、数组排序与值格式一致；capacities 的 before 含
            # 当前 pc 显式覆盖；lifetime 的 before 含当前 tm 硬时限登记）；
            # 仅 backends 变化时 changes 为空。成功仅推进时钟（已在共用时钟
            # 块完成），其余状态不变。O(N)，N 为规范化配置大小。
            current_export = export_config()
            candidate_export = export_normalized_config(config)
            changes = []
            for section in (
                "vnodes", "limits", "overload", "sticky", "idle",
                "backpressure", "scheduler", "faults", "quotas", "queue",
                "capacities", "lifetime",
            ):
                before = current_export[section]
                after = candidate_export[section]
                if before != after:
                    changes.append(
                        {"section": section, "before": before, "after": after}
                    )
            results.append(
                {
                    "op": "pd",
                    "base": config_digest(current_export),
                    "target": config_digest(candidate_export),
                    "changes": changes,
                }
            )

        elif op[0] == "hd":
            # 哈希配置预演（不应用配置）：以当前与候选配置各自的 backends、
            # vnodes 分别建环——忽略运行态（健康、熔断、排空）与粘性，全部后端
            # 按加入序上环，哈希规则沿用 chash，按 keys 原序独立映射。B 限流/
            # 配额与 faults 引用未知后端报 BACKEND（优先级同 cv/cd/pd，先于
            # STATE）；任一 vnodes 为 null 或任一侧 backends 为空报 STATE。
            # 成功仅推进时钟（已在共用时钟块完成），不改任何状态；失败批次
            # 天然回滚。N 为两环令牌总数、K 为 keys 长度：两环各排序一次共
            # O(N log N)，每键两次二分共 O(K log N)，额外空间 O(N+K)。
            _, config, keys, now = op
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            candidate_export = export_normalized_config(config)
            candidate_ids = [item["id"] for item in candidate_export["backends"]]
            if config["vnodes"] is None or not candidate_ids:
                # 候选未配环或候选后端为空：无法为候选建环。
                fail(EXIT_STATE, "STATE")
            current_export = export_config()
            current_ids = [item["id"] for item in current_export["backends"]]
            if ring_vnodes is None or not current_ids:
                # 当前未 chash 或当前后端为空：无法为当前建环（初始空配置即
                # 如此）。
                fail(EXIT_STATE, "STATE")
            target_tokens = build_static_ring(candidate_ids, config["vnodes"])
            base_tokens = build_static_ring(current_ids, ring_vnodes)
            target_digests = [token[0] for token in target_tokens]
            base_digests = [token[0] for token in base_tokens]
            cases = []
            stable = 0
            for key in keys:
                before = lookup_ring(base_tokens, base_digests, key)
                after = lookup_ring(target_tokens, target_digests, key)
                changed = before != after
                if not changed:
                    stable += 1
                cases.append(
                    {
                        "key": key,
                        "before": before,
                        "after": after,
                        "changed": changed,
                    }
                )
            total = len(keys)
            results.append(
                {
                    "op": "hd",
                    "base": config_digest(current_export),
                    "target": config_digest(candidate_export),
                    "cases": cases,
                    "summary": {
                        "total": total,
                        "stable": stable,
                        "remapped": total - stable,
                    },
                }
            )

        elif op[0] == "hb":
            # 哈希分布预演（不应用配置）：沿用 hd 的静态环与 SHA-256 规则，
            # 以当前与候选配置各自的 backends 加入顺序、vnodes 分别建环，
            # 忽略健康、熔断、排空、故障时间线与粘性运行态；每个 key 在两侧
            # 独立映射、重复键按出现次数计数，但按后端汇总落点计数而非逐键
            # 明细。候选 B 限流/B 配额/faults/capacities 引用未知后端报
            # BACKEND（优先级同 hd，先于 STATE）；任一侧 vnodes 为 null 或
            # backends 为空报 STATE。成功仅推进时钟（已在共用时钟块完成），
            # 不改任何状态；失败批次天然回滚。N 为两环令牌总数、K 为 keys
            # 长度、B 为两侧后端并集：建环排序 O(N log N)，每键两次二分共
            # O(K log N)，额外空间 O(N+B)。
            _, config, keys, now = op
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            candidate_export = export_normalized_config(config)
            candidate_ids = [item["id"] for item in candidate_export["backends"]]
            if config["vnodes"] is None or not candidate_ids:
                # 候选未配环或候选后端为空：无法为候选建环。
                fail(EXIT_STATE, "STATE")
            current_export = export_config()
            current_ids = [item["id"] for item in current_export["backends"]]
            if ring_vnodes is None or not current_ids:
                # 当前未 chash 或当前后端为空：无法为当前建环（初始空配置即
                # 如此）。
                fail(EXIT_STATE, "STATE")
            target_tokens = build_static_ring(candidate_ids, config["vnodes"])
            base_tokens = build_static_ring(current_ids, ring_vnodes)
            target_digests = [token[0] for token in target_tokens]
            base_digests = [token[0] for token in base_tokens]
            before_counts = {backend_id: 0 for backend_id in current_ids}
            after_counts = {backend_id: 0 for backend_id in candidate_ids}
            stable = 0
            for key in keys:
                before = lookup_ring(base_tokens, base_digests, key)
                after = lookup_ring(target_tokens, target_digests, key)
                before_counts[before] += 1
                after_counts[after] += 1
                if before == after:
                    stable += 1
            # backends 先按当前后端加入顺序，再按候选顺序追加仅候选侧后端。
            ordered_ids = list(current_ids)
            current_id_set = set(current_ids)
            for backend_id in candidate_ids:
                if backend_id not in current_id_set:
                    ordered_ids.append(backend_id)
            backend_rows = []
            for backend_id in ordered_ids:
                before_count = before_counts.get(backend_id, 0)
                after_count = after_counts.get(backend_id, 0)
                backend_rows.append(
                    {
                        "id": backend_id,
                        "before": before_count,
                        "after": after_count,
                        "delta": after_count - before_count,
                    }
                )
            total = len(keys)
            remapped = total - stable
            # rate=floor(10000*remapped/total)/100，两位定点串，向下截断。
            rate = "%d.%02d" % divmod(remapped * 10000 // total, 100)
            results.append(
                {
                    "op": "hb",
                    "base": config_digest(current_export),
                    "target": config_digest(candidate_export),
                    "backends": backend_rows,
                    "summary": {
                        "total": total,
                        "stable": stable,
                        "remapped": remapped,
                        "rate": rate,
                        "before_min": min(before_counts.values()),
                        "before_max": max(before_counts.values()),
                        "after_min": min(after_counts.values()),
                        "after_max": max(after_counts.values()),
                    },
                }
            )

        elif op[0] == "cl":
            # 配置提交历史（只读）：current 为最新 rev 或 null，commits 按
            # rev 升序（历史本就按分配序追加），项键序 rev,config，config
            # 复用 ce 的逐层键序与值格式。O(16(B+M+T+Q))。
            results.append(
                {
                    "op": "cl",
                    "current": (
                        commit_history[-1][0] if commit_history else None
                    ),
                    "commits": [
                        {"rev": rev, "config": snapshot}
                        for rev, snapshot in commit_history
                    ],
                }
            )

        elif op[0] == "al":
            # 配置变更审计（只读，不推进时钟）：events 为保留事件（按 rev
            # 升序，至多 64 条）的快照，逐项键序
            # rev,now,kind,section,before,after，响应键序 op,events。重复
            # 查询逐字节相同；事件追加后不就地修改，逐项复制成全新结构。
            # O(A)，A≤64。
            results.append(
                {
                    "op": "al",
                    "events": [
                        {
                            "rev": event["rev"],
                            "now": event["now"],
                            "kind": event["kind"],
                            "section": event["section"],
                            "before": event["before"],
                            "after": event["after"],
                        }
                        for event in audit_events
                    ],
                }
            )

        elif op[0] == "ai":
            # 审计游标查询（只读，不推进时钟）：复用 al 按 rev 升序保留的
            # 最近 64 条事件。latest 为已分配最大 rev（next_rev-1，初始 0），
            # after>latest（游标越过最新事件、指向尚未分配的 rev）报 STATE。
            # 历史非空且 after 小于最旧 rev 减 1 时，after 与最旧事件之间
            # 必有已按 64 条淘汰的事件，置 truncated=true 并自最旧事件读取；
            # after 等于最旧 rev 减 1（缺口恰为淘汰区、不漏可读事件）或更大
            # 时不截断，仅取 rev>after 的事件。至多取 limit 条并逐项全新复
            # 制（键序沿用 al）；next 为末条 rev、无条目等于 after；more 为
            # 该窗口之后是否仍有可读事件。响应键序
            # op,after,next,truncated,more,events。O(A)，额外空间 O(limit)，
            # A≤64；不推进时钟、失败批次回滚。
            _, after, limit = op
            latest = next_rev - 1
            if after > latest:
                fail(EXIT_STATE, "STATE")
            truncated = False
            if audit_events and after < audit_events[0]["rev"] - 1:
                truncated = True
                candidates = audit_events
            else:
                candidates = (
                    event for event in audit_events if event["rev"] > after
                )
            picked = []
            more = False
            for event in candidates:
                if len(picked) < limit:
                    picked.append(event)
                else:
                    more = True
                    break
            results.append(
                {
                    "op": "ai",
                    "after": after,
                    "next": picked[-1]["rev"] if picked else after,
                    "truncated": truncated,
                    "more": more,
                    "events": [
                        {
                            "rev": event["rev"],
                            "now": event["now"],
                            "kind": event["kind"],
                            "section": event["section"],
                            "before": event["before"],
                            "after": event["after"],
                        }
                        for event in picked
                    ],
                }
            )

        elif op[0] == "ad":
            # 审计段级差异查询（只读，不推进时钟、不改淘汰窗口）：按 rev 取
            # 与 al/ai 同窗口（最近 64 条）保留的事件。audit_events 与
            # audit_sections 同序追加与淘汰，同下标即同一修订的段级差异。
            # rev 从未分配、大于最新修订或其事件已随窗口淘汰：STATE。响应固
            # 定键序 op,rev,now,kind,section,before,after,changes；前七项
            # 与同 rev 的 al/ai 事件逐值一致；changes 按 AD_SECTIONS 顶层键
            # 序（排除 version）仅列前后值不同的段，项键序
            # section,before,after，两指纹为段值的 64 位小写 SHA-256。重复
            # 查询逐项全新复制、逐字节一致。O(S) 时间与额外空间，S 为规范
            # 化顶层段数；失败批次回滚且无 stdout。
            _, target_rev = op
            target_event = None
            target_changes = None
            for index, event in enumerate(audit_events):
                if event["rev"] == target_rev:
                    target_event = event
                    target_changes = audit_sections[index]
                    break
            if target_event is None:
                fail(EXIT_STATE, "STATE")
            results.append(
                {
                    "op": "ad",
                    "rev": target_event["rev"],
                    "now": target_event["now"],
                    "kind": target_event["kind"],
                    "section": target_event["section"],
                    "before": target_event["before"],
                    "after": target_event["after"],
                    "changes": [
                        {
                            "section": section,
                            "before": before_digest,
                            "after": after_digest,
                        }
                        for section, before_digest, after_digest
                        in target_changes
                    ],
                }
            )

        elif op[0] == "ag":
            # 审计区间汇总（只读，不推进时钟、不改审计窗口、修订号、配置与
            # 运行态）：以左开右闭区间 (after,until] 汇总与 al/ai/ad 同窗口
            # （最近 64 条）保留的配置变更事件。until 超过已分配最大 rev
            # （next_rev-1，初始 0）报 STATE；空历史仅允许 until=0。淘汰口
            # 径沿用 ai：历史非空且 after 小于最旧保留 rev 减一时
            # truncated=true，即使区间内无事件；已淘汰事件不进入 events 与
            # 汇总。events 按 rev 升序（deque 本即此序），项键序
            # rev,now,kind,section,before,after,changes，前六项同 al/ai，
            # changes 直接采用 ad 固化的段级差异（按 AD_SECTIONS 序，项键
            # 序 section,before,after）并逐项全新复制。summary 键序
            # total,ci,cb,cu,ca,changed,sections：total 为返回事件数，四个
            # 同名字段按 kind 计数，changed 为 changes 非空的事件数；
            # sections 只列出现过的配置段并按 AD_SECTIONS 顶层段序排列，
            # events 为包含该段的事件数（同一事件同段只计一次）。区间无事
            # 件时 events 与 sections 为空数组、各计数为 0。O(64S) 时间与
            # 额外空间，S 为规范化顶层段数；失败批次回滚且无 stdout。
            _, after, until = op
            latest = next_rev - 1
            if until > latest:
                fail(EXIT_STATE, "STATE")
            truncated = bool(
                audit_events and after < audit_events[0]["rev"] - 1
            )
            kind_counts = {"ci": 0, "cb": 0, "cu": 0, "ca": 0}
            changed_count = 0
            section_counts = {section: 0 for section in AD_SECTIONS}
            out_events = []
            for index, event in enumerate(audit_events):
                if not (after < event["rev"] <= until):
                    continue
                kind_counts[event["kind"]] += 1
                event_changes = audit_sections[index]
                if event_changes:
                    changed_count += 1
                counted_sections = set()
                out_changes = []
                for section, before_digest, after_digest in event_changes:
                    out_changes.append(
                        {
                            "section": section,
                            "before": before_digest,
                            "after": after_digest,
                        }
                    )
                    if section not in counted_sections:
                        counted_sections.add(section)
                        section_counts[section] += 1
                out_events.append(
                    {
                        "rev": event["rev"],
                        "now": event["now"],
                        "kind": event["kind"],
                        "section": event["section"],
                        "before": event["before"],
                        "after": event["after"],
                        "changes": out_changes,
                    }
                )
            out_sections = [
                {"section": section, "events": section_counts[section]}
                for section in AD_SECTIONS
                if section_counts[section]
            ]
            results.append(
                {
                    "op": "ag",
                    "after": after,
                    "until": until,
                    "truncated": truncated,
                    "events": out_events,
                    "summary": {
                        "total": len(out_events),
                        "ci": kind_counts["ci"],
                        "cb": kind_counts["cb"],
                        "cu": kind_counts["cu"],
                        "ca": kind_counts["ca"],
                        "changed": changed_count,
                        "sections": out_sections,
                    },
                }
            )

        elif op[0] in ("cb", "cb_cond"):
            # 配置回滚：按目标快照执行 ci 的原子替换与默认运行态重建，成功
            # 另建新 rev；全部校验先于任何变更，失败天然回滚时钟、配置、
            # 运行态、rev 与历史。四键乐观形态 cb_cond 先确认目标 rev 仍在
            # 十六条提交窗口，再比较 base 与操作开始时当前规范化 v11 配置
            # 摘要（ct 口径）：不匹配报 STATE，且不应用快照、不推进时钟、
            # 不清预约、不分配 rev、不写审计（时钟虽已由共用块先行更新，
            # 失败随整批丢弃，与既有 cb/cu/ca 同式回滚）。base 匹配后沿用
            # cb 全部判定与效果，响应固定键序 op,target,rev,ok（op 恒
            # "cb"，与三键同形）；即使目标配置与当前逐值相同也新建 rev。
            if op[0] == "cb_cond":
                _, target_rev, base, now = op
            else:
                _, target_rev, now = op
                base = None
            snapshot = None
            for rev, committed in commit_history:
                if rev == target_rev:
                    snapshot = committed
                    break
            if snapshot is None:
                # 目标不存在（从未分配或已按 16 条淘汰）。
                fail(EXIT_STATE, "STATE")
            if base is not None and base != config_digest(export_config()):
                # 乐观并发保护：base 与操作开始时当前配置摘要不等即拒绝，
                # 先于 rev 耗尽与活动连接或排队检查；不改动任何状态。
                fail(EXIT_STATE, "STATE")
            if next_rev > 10 ** 18:
                # rev 耗尽。
                fail(EXIT_STATE, "STATE")
            # 有活动连接或排队项时拒绝回滚：STATE。
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")
            # 快照即规范化 version=11 配置（含 queue 策略、capacities
            # 覆盖与 lifetime 硬时限），重解析后沿用 ci 的替换语义；快照
            # 来自 export_config，必然合法，不会抛 INPUT。queue.dequeue/
            # full 随快照恢复，队列清空且淘汰计数、最近淘汰 cid 重置（0、
            # null）；capacities 覆盖随快照恢复；硬时限按快照 lifetime
            # 载入，null 清除。
            before_export = export_config()
            before_digest = config_digest(before_export)
            apply_config(parse_config(snapshot), now)
            # 原历史保留，追加新 rev 后再按 16 条淘汰。
            new_snapshot = export_config()
            commit_history.append((next_rev, new_snapshot))
            after_digest = config_digest(new_snapshot)
            new_rev = next_rev
            next_rev += 1
            if len(commit_history) > 16:
                commit_history.pop(0)
            # cb 成功清除既有配置预约。
            reservation = None
            # cb 成功同时清除全池端点切换预约；生命周期审计：清除现存预约
            # 记 CLEAR，本无预约不记。
            if ep_switch is not None:
                ep_audit_append(
                    "CLEAR", now, (ep_switch[3], ep_switch[2]), None,
                )
            ep_switch = None
            # 审计：成功并分配新 rev 时追加（仅留最近 64 条），section 恒
            # null；before 为回滚前指纹，after 为目标快照指纹。同一事件内
            # 固化段级差异（仅指纹，按顶层键序）。
            audit_events.append(
                {
                    "rev": new_rev,
                    "now": now,
                    "kind": "cb",
                    "section": None,
                    "before": before_digest,
                    "after": after_digest,
                }
            )
            audit_sections.append(
                tuple(config_section_changes(before_export, new_snapshot))
            )
            results.append(
                {"op": "cb", "target": target_rev, "rev": new_rev, "ok": True}
            )

        elif op[0] == "cu":
            # 单字段配置热加载：以当前 ce.config 规范化 version=11 导出为
            # 底稿，仅替换 section 一段，再按 ci 整体校验（结构、键序、类型、
            # 范围、排序与交叉约束）。校验通过前不改任何配置或运行态；时钟
            # 已由共用时钟块推进，失败随整批丢弃即回滚。
            _, base, section, value, now = op
            before_export = export_config()
            before_digest = config_digest(before_export)
            # 复制顶层映射后在原键位替换该段：替换不改变顶层键序，其余段
            # 与操作前完全一致；候选的 version 恒为 11。
            candidate = dict(before_export)
            candidate[section] = value
            # 按 ci 同型校验整份候选：键序、结构、类型、范围、排序或交叉
            # 约束（如 scheduler=H 须 vnodes 非 null、backpressure 须
            # overload 非 null 且 low<high≤q）非法均 INPUT。
            config = parse_config(candidate)
            # B 限流、B 配额、faults 与 capacities 引用未知候选后端：
            # BACKEND，先于 base 与活动状态判定（同 ci 的错误优先级）。
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 校验后 base 与操作前指纹不等：STATE，先于活动连接或排队检查。
            if base != before_digest:
                fail(EXIT_STATE, "STATE")
            # 有活动连接或排队项时拒绝热加载：STATE。
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")
            if next_rev > 10 ** 18:
                # rev 耗尽：不分配、不改历史。
                fail(EXIT_STATE, "STATE")
            # 沿用 ci 的原子替换与默认运行态重建（桶满、配额按 quotas 以
            # now 重置、队空、粘性清空、度量归零、faults 时间线载入、queue
            # 策略与 capacities 覆盖替换、lifetime 载入、告警与历史清空）。
            apply_config(config, now)
            target_export = export_config()
            target_digest = config_digest(target_export)
            # 值未变也新建 rev；成功后提交并按最近 16 条淘汰（同 ci/cb）。
            commit_history.append((next_rev, target_export))
            new_rev = next_rev
            next_rev += 1
            if len(commit_history) > 16:
                commit_history.pop(0)
            # cu 成功清除既有配置预约。
            reservation = None
            # cu 成功同时清除全池端点切换预约；生命周期审计：清除现存预约
            # 记 CLEAR，本无预约不记。
            if ep_switch is not None:
                ep_audit_append(
                    "CLEAR", now, (ep_switch[3], ep_switch[2]), None,
                )
            ep_switch = None
            # 审计：成功并分配新 rev 时追加（仅留最近 64 条），section 取被
            # 替换顶层字段；before/after 即响应中的 base/target 摘要（值未变
            # 两摘要相同仍记录）。同一事件内固化段级差异：按实际前后规范化
            # 值计算，section 字段仅表示请求替换的段，值未变时不列入 changes。
            audit_events.append(
                {
                    "rev": new_rev,
                    "now": now,
                    "kind": "cu",
                    "section": section,
                    "before": before_digest,
                    "after": target_digest,
                }
            )
            audit_sections.append(
                tuple(config_section_changes(before_export, target_export))
            )
            results.append(
                {
                    "op": "cu",
                    "base": before_digest,
                    "target": target_digest,
                    "rev": new_rev,
                    "ok": True,
                }
            )

        elif op[0] == "cp":
            # 配置预约：at>=now 为时间字段判定（先于 BACKEND）；B 限流、
            # B 配额、faults 与 capacities 引用未知后端报 BACKEND，优先级同
            # cv。成功把候选规范化为 version=11 快照保存但不应用、不建 rev、
            # 不改任何运行态；同 digest、at 重报幂等（原样返回且不重存快
            # 照）。快照为 export_normalized_config 产出的全新结构，额外
            # 时间 O(N)、空间 O(N)，N 为规范化配置大小。
            _, config, at, now = op
            if at < now:
                # at 仅表示触发时刻：早于本次 now 非法。时间字段判定先于
                # cv 的配置后端引用 BACKEND 检查（同全局 INPUT 先于 BACKEND
                # 的优先级）。
                fail(EXIT_INPUT, "INPUT")
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            snapshot = export_normalized_config(config)
            digest = config_digest(snapshot)
            # 同 digest、at 重报幂等：原样返回且不重存快照（保留原预约）；
            # 否则整体替换为新预约（含同 digest 不同 at 或同 at 不同配置）。
            if reservation is None or (reservation[2], reservation[1]) != (
                digest, at
            ):
                reservation = (snapshot, at, digest)
            results.append({"op": "cp", "digest": digest, "at": at})

        elif op[0] == "cq":
            # 预约查询（只读，O(1)）：不受连接、队列限制；pending 为 bool，
            # 无预约时 digest、at 均为 null。成功仅推进时钟（已在共用时钟
            # 块完成），不改预约。
            _, now = op
            if reservation is None:
                results.append(
                    {"op": "cq", "pending": False, "digest": None, "at": None}
                )
            else:
                _, at, digest = reservation
                results.append(
                    {"op": "cq", "pending": True, "digest": digest, "at": at}
                )

        elif op[0] in ("ca", "ca_cond"):
            # 预约生效：两种形态共用同一生效路径。两键 ca 直接按当前预约
            # 生效；四键 ca（op,digest,at,now）为条件形态，仅当当前预约的
            # 规范化配置摘要与触发时刻分别等于请求 digest、at 时继续，
            # 没有预约或任一身份不符报 STATE，且不得应用配置、清除预约、
            # 分配 rev 或写审计（身份比较 O(1)，先于其他一切 STATE 判定）。
            # 身份匹配后与直接 ca 同序：now<at、有活动连接或排队项、rev
            # 耗尽（同 ci）报 STATE。全部校验先于任何变更。通过后按保存的
            # v11 快照执行 ci 的原子替换、以 now 重建默认运行态并新建 rev
            # （快照来自 export_normalized_config，重解析必然合法），成功
            # 清除预约；失败天然原子回滚（预约、时钟、配置、rev 均不变）。
            if op[0] == "ca_cond":
                _, wanted_digest, wanted_at, now = op
                if (
                    reservation is None
                    or reservation[2] != wanted_digest
                    or reservation[1] != wanted_at
                ):
                    fail(EXIT_STATE, "STATE")
            else:
                _, now = op
                if reservation is None:
                    fail(EXIT_STATE, "STATE")
            snapshot, at, digest = reservation
            if now < at:
                fail(EXIT_STATE, "STATE")
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")
            if next_rev > 10 ** 18:
                fail(EXIT_STATE, "STATE")
            before_export = export_config()
            before_digest = config_digest(before_export)
            apply_config(parse_config(snapshot), now)
            new_snapshot = export_config()
            commit_history.append((next_rev, new_snapshot))
            after_digest = config_digest(new_snapshot)
            new_rev = next_rev
            next_rev += 1
            if len(commit_history) > 16:
                commit_history.pop(0)
            reservation = None
            # ca 成功同时清除全池端点切换预约；生命周期审计：清除现存预约
            # 记 CLEAR，本无预约不记（能走到此处 reservation 非空，但
            # ep_switch 可能本就为 None）。
            if ep_switch is not None:
                ep_audit_append(
                    "CLEAR", now, (ep_switch[3], ep_switch[2]), None,
                )
            ep_switch = None
            # 审计：成功并分配新 rev 时追加（仅留最近 64 条），section 恒
            # null；before 为生效前指纹，after 为预约快照指纹（同响应 digest）。
            # 同一事件内固化段级差异（仅指纹，按顶层键序）。条件形态同样
            # 追加 kind=ca 事件，与同状态下直接 ca 成功后的提交历史与审计
            # 状态逐值一致。
            audit_events.append(
                {
                    "rev": new_rev,
                    "now": now,
                    "kind": "ca",
                    "section": None,
                    "before": before_digest,
                    "after": after_digest,
                }
            )
            audit_sections.append(
                tuple(config_section_changes(before_export, new_snapshot))
            )
            results.append(
                {"op": "ca", "digest": digest, "rev": new_rev, "ok": True}
            )

        elif op[0] == "cx":
            # 配置预约条件取消：形状/字段类型/范围/UTF-8 与时钟倒退已在
            # 解析期及共用时钟块判 INPUT，且 INPUT 判定先于预约匹配。当前
            # 有预约且 digest、at 同时匹配时原子删除预约，cancelled=true
            # 并回显请求的 digest、at；无预约为幂等空操作，cancelled=false、
            # 同键序回显，不创建修订、审计事件或任何其他状态；有预约但
            # digest 或 at 任一不匹配报 STATE 并保留原预约。取消只影响待
            # 生效预约，不应用候选配置，当前配置、后端、连接、队列、粘性、
            # 指标、告警、提交历史与 next_rev 均不变；now 已由共用时钟块
            # 推进，at 仅匹配、不推动时钟。失败随整批丢弃，时钟与此前操作
            # 一并回滚。
            _, digest, at, now = op
            if reservation is None:
                results.append(
                    {"op": "cx", "cancelled": False,
                     "digest": digest, "at": at}
                )
            else:
                _, reserved_at, reserved_digest = reservation
                if reserved_digest != digest or reserved_at != at:
                    fail(EXIT_STATE, "STATE")
                reservation = None
                results.append(
                    {"op": "cx", "cancelled": True,
                     "digest": digest, "at": at}
                )

        elif op[0] == "cy":
            # 配置预约条件替换（并发安全的改期/换配置）：形状/字段类型/
            # 范围/UTF-8 与时钟倒退已在解析期及共用时钟块判 INPUT。本分支
            # 判定顺序同 cp 与全局 INPUT 先于 BACKEND、BACKEND 先于 STATE：
            # at<now 先判 INPUT，再校验候选配置 B 限流、B 配额、faults 与
            # capacities 对候选后端集合的引用（BACKEND，引用的是候选配置
            # 自带后端而非现存后端），最后读取当前预约做条件匹配。
            # 无预约为 STATE；当前预约既不等于旧值（base、base_at）也不
            # 等于新值（候选规范化摘要、at）时为 STATE。旧值匹配时原子
            # 替换为候选规范化快照；当前已等于新值时视为幂等重报——即使
            # 旧值已过期（预约已被另一条成功的 cy/cp 改成新值）也成功，
            # 且不重写预约状态（保留既有快照对象）。只改待生效预约：不
            # 应用配置、不建 rev 或审计事件，当前配置、后端、连接、队列、
            # 粘性、指标、告警、提交历史与 next_rev 均不变。失败随整批
            # 丢弃，时钟与此前操作一并回滚。规范化为 O(N) 时间与空间，
            # N 为候选规范化配置大小。
            _, base, base_at, config, at, now = op
            if at < now:
                fail(EXIT_INPUT, "INPUT")
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for scope, quota_id, _, _ in config["quotas"]:
                if scope == "B" and quota_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for override_id in config["capacities"]:
                if override_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 先规范化候选（O(N)，全新结构）再读当前预约做条件判定。
            snapshot = export_normalized_config(config)
            digest = config_digest(snapshot)
            if reservation is None:
                fail(EXIT_STATE, "STATE")
            _, reserved_at, reserved_digest = reservation
            matches_old = reserved_digest == base and reserved_at == base_at
            matches_new = reserved_digest == digest and reserved_at == at
            if not matches_old and not matches_new:
                fail(EXIT_STATE, "STATE")
            if matches_old and not matches_new:
                # 原子替换为候选快照；幂等重报（matches_new）不重写状态。
                reservation = (snapshot, at, digest)
            # 旧值匹配完成替换与新值幂等重报逐字节同输出：回显请求 base、
            # 候选摘要与新 at，ok 恒 true。
            results.append(
                {"op": "cy", "base": base, "digest": digest,
                 "at": at, "ok": True}
            )

        elif op[0] == "fs":
            _, backend_id, segment = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            # 把该 id 的故障时间线替换为单段：同参（同为该单段）重报幂等，
            # 不改登记也不清恢复基线；异参覆盖只清按段恢复判定基线、不清
            # 计数；均返回 ok。
            new_faults = [segment]
            if record["faults"] != new_faults:
                record["fault_base"] = set()
            record["faults"] = new_faults
            record["fault_a"] = [segment[1]]
            results.append({"op": "fs", "ok": True})

        elif op[0] == "fb":
            _, plan = op
            # 未知 id 报 BACKEND，先于任何变更；失败批次原子回滚。
            for item_id in plan:
                if item_id not in backends:
                    fail(EXIT_BACKEND, "BACKEND")
            # 与 fs/fp 同源：按后端加入序把全体时间线一次替换为各 id 的单段
            # （未列入的后端清空），空计划即清空；整体替换使同计划重报天然
            # 幂等。异参替换或移除只清按段恢复判定基线、不清计数，同参不动
            # 基线。
            for backend_id, record in backends.items():
                new_fault = plan.get(backend_id)
                new_faults = [] if new_fault is None else [new_fault]
                if record["faults"] != new_faults:
                    record["fault_base"] = set()
                record["faults"] = new_faults
                record["fault_a"] = (
                    [] if new_fault is None else [new_fault[1]]
                )
            results.append({"op": "fb", "ok": True})

        elif op[0] == "fp":
            _, plan = op
            # 未知 id 报 BACKEND，先于任何变更（INPUT 已在解析期判完）；失败
            # 批次原子回滚。
            for item_id in plan:
                if item_id not in backends:
                    fail(EXIT_BACKEND, "BACKEND")
            # 原子替换各列入 id 的时间线（未列入的后端保持原样；items 为空
            # 即无操作）。plan 内段已按 a 规范化（升序且不重叠）；fault_a
            # 与 faults 平行更新，供活动段 O(log T_b) 查找。同计划重报列表
            # 逐段相等，天然幂等；异参替换只清按段恢复判定基线、不清计数。
            for backend_id, new_faults in plan.items():
                record = backends[backend_id]
                if record["faults"] != new_faults:
                    record["fault_base"] = set()
                record["faults"] = new_faults
                record["fault_a"] = [segment[1] for segment in new_faults]
            results.append({"op": "fp", "ok": True})

        elif op[0] == "fq":
            _, now = op
            # 按后端加入序、段 a 升序列出全部段；effect 按各段自身窗口
            # [a,z) 计算：窗口外（含段间隙）N，窗口内 D 为 D、F 按
            # ((now-a)//v)%2=0 取 D 否则 N、S 取 S。down/slow 计 effect 为
            # D/S 的段数。结果快照随批次末尾序列化，此处构造的即全新对象。
            faults = []
            down = 0
            slow = 0
            for backend_id, record in backends.items():
                for k, a, z, v in record["faults"]:
                    if not a <= now < z:
                        effect = "N"
                    elif k == "D":
                        effect = "D"
                    elif k == "F":
                        effect = "D" if ((now - a) // v) % 2 == 0 else "N"
                    else:  # S：窗口内仅变慢。
                        effect = "S"
                    if effect == "D":
                        down += 1
                    elif effect == "S":
                        slow += 1
                    faults.append(
                        {
                            "id": backend_id,
                            "k": k,
                            "a": a,
                            "z": z,
                            "v": v,
                            "effect": effect,
                        }
                    )
            results.append(
                {"op": "fq", "faults": faults, "down": down, "slow": slow}
            )

        elif op[0] == "br":
            # 全池运行态快照：除共用时钟按 now 推进外不改任何运行态，失败
            # 批次天然回滚。backends 按加入序，项键序
            # id,health,circuit,drain,fault,connections,ready,blocked。
            # fault 取 fq 同款 effect（N/D/S）；ready 要求 healthy、熔断
            # C（含未配）、排空 A 且 fault≠D（S 不阻断）；blocked 按
            # health,circuit,drain,fault 顺序列出未满足项，ready 时为空。
            # 活动段查找为 O(log T_b)，整体时间 O(B log(T+1))、空间 O(B)。
            _, now = op
            snapshot = []
            for backend_id, record in backends.items():
                circuit = record["circuit"]
                circuit_state = (
                    "C" if circuit is None else circuit["state"]
                )
                drain_state = record["drain"]["state"]
                effect = fault_effect(active_fault(record, now), now)
                blocked = []
                if not record["healthy"]:
                    blocked.append("health")
                if circuit_state != "C":
                    blocked.append("circuit")
                if drain_state != "A":
                    blocked.append("drain")
                if effect == "D":
                    blocked.append("fault")
                snapshot.append(
                    {
                        "id": backend_id,
                        "health": (
                            "healthy" if record["healthy"] else "unhealthy"
                        ),
                        "circuit": circuit_state,
                        "drain": drain_state,
                        "fault": effect,
                        "connections": record["conns"],
                        "ready": not blocked,
                        "blocked": blocked,
                    }
                )
            results.append({"op": "br", "now": now, "backends": snapshot})

        elif op[0] == "ru":
            # 不可用时长查询：除共用时钟按 now 推进（批前通用时钟块）外只读，
            # 失败批次天然回滚。reasons 按 drain,health,circuit,fault 排序，
            # 项键序 reason,since,duration，duration=now-since；各原因事实取
            # unavail_reason_fact（ua 同口径），仅列入生效原因。
            # 活动段查找 O(log T_b)，整体时间 O(log(T+1))、空间 O(1)。
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            reasons = []
            for reason in ("drain", "health", "circuit", "fault"):
                active, since = unavail_reason_fact(record, reason, now)
                if active:
                    reasons.append(
                        {
                            "reason": reason,
                            "since": since,
                            "duration": now - since,
                        }
                    )
            results.append(
                {"op": "ru", "id": backend_id, "now": now, "reasons": reasons}
            )

        elif op[0] == "ua":
            # 连续不可用时长告警：按 (id,reason) 独立评估，除共用时钟按 now
            # 推进与自身告警状态外只读，失败批次天然回滚。active/since/
            # duration 沿用同一时刻 ru 的单原因事实口径（unavail_reason_fact），
            # 不因更高优先级原因遮蔽；不生效取 false/null/0，生效时 since 为
            # 本次连续区间起点、duration=now-since。active 且
            # duration>=threshold 为 A 否则 N；changed 仅相对该组合上次成功
            # 评估发生转换，首次以前态 N 起评（首次已超阈值即 changed=true）。
            # 首评固化 threshold，此后改值报 STATE（INPUT 已在解析期判）；同
            # id,reason,threshold,now 重报原样返回首评结果、不推进状态。fault
            # 活动段查找 O(log T_b)，其余三因 O(1)，整体 O(log(T+1))、空间
            # O(1)；状态总空间 O(B)（每后端至多四键）。
            _, backend_id, reason, threshold, now = op
            record = backends.get(backend_id)
            if record is None:
                # 未知 id 先于一切告警状态判定。
                fail(EXIT_BACKEND, "BACKEND")
            key = (backend_id, reason)
            entry = unavail_alerts.get(key)
            if entry is not None:
                if threshold != entry["threshold"]:
                    # 已登记组合变更 threshold：STATE，不改状态。
                    fail(EXIT_STATE, "STATE")
                if now == entry["now"]:
                    # 同 id,reason,threshold,now 重报：原样返回首评结果，
                    # 不重复推进状态；此时刻之后的原因变化须更大 now 才可见。
                    results.append(dict(entry["result"]))
                    continue
            active, since = unavail_reason_fact(record, reason, now)
            if active:
                duration = now - since
                state = "A" if duration >= threshold else "N"
            else:
                duration = 0
                state = "N"
            prev_state = "N" if entry is None else entry["state"]
            changed = state != prev_state
            result = {
                "op": "ua",
                "id": backend_id,
                "reason": reason,
                "threshold": threshold,
                "state": state,
                "active": active,
                "since": since,
                "duration": duration,
                "changed": changed,
            }
            unavail_alerts[key] = {
                "threshold": threshold,
                "state": state,
                "now": now,
                "result": dict(result),
            }
            results.append(result)

        elif op[0] == "hm":
            # H pick 记账只读查询：未知 id 报 BACKEND；不改变任何计数，失败
            # 批次天然回滚。返回键序
            # op,id,total,first,sticky,expired,removed,health,circuit,drain。
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            counts = record["pick_counts"]
            results.append(
                {
                    "op": "hm",
                    "id": backend_id,
                    "total": counts["total"],
                    "first": counts["first"],
                    "sticky": counts["sticky"],
                    "expired": counts["expired"],
                    "removed": counts["removed"],
                    "health": counts["health"],
                    "circuit": counts["circuit"],
                    "drain": counts["drain"],
                }
            )

        elif op[0] == "fm":
            # 故障演练统计只读查询：未知 id 报 BACKEND；不改变任何计数与
            # 恢复基线，失败批次天然回滚。返回键序 op,id,D,F,S；D/F/S 各为
            # 键序 affected,rejected,retries,remaps,recovered 的对象。必须
            # 快照：结果在批次末尾才序列化，直接引用会被后续记账污染。
            _, backend_id = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            stats = record["fault_stats"]
            results.append(
                {
                    "op": "fm",
                    "id": backend_id,
                    "D": dict(stats["D"]),
                    "F": dict(stats["F"]),
                    "S": dict(stats["S"]),
                }
            )

        elif op[0] == "fh":
            # 故障统计分钟历史只读查询：未知 id 报 BACKEND，from 早于最近
            # 60 窗下界报 STATE（与 mh/mx 同序）；不改任何计数与恢复基线，
            # 失败批次天然回滚。返回键序 op,id,windows；windows 覆盖 from..to
            # 并升序，项键序 window,D,F,S，三类各为键序
            # affected,rejected,retries,remaps,recovered 的对象；空窗五项 0。
            # 逐 kind 拷贝，避免结果在批次末尾被后续记账污染。
            _, backend_id, start, end, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = record["fault_hist"]
            windows = []
            for window in range(start, end + 1):
                stats = history.get(window)
                if stats is None:
                    empty = new_fault_stats()
                    windows.append(
                        {
                            "window": window,
                            "D": dict(empty["D"]),
                            "F": dict(empty["F"]),
                            "S": dict(empty["S"]),
                        }
                    )
                else:
                    windows.append(
                        {
                            "window": window,
                            "D": dict(stats["D"]),
                            "F": dict(stats["F"]),
                            "S": dict(stats["S"]),
                        }
                    )
            results.append({"op": "fh", "id": backend_id, "windows": windows})

        elif op[0] == "fa":
            # 全池故障汇总（只读）：from 早于最近 60 窗下界报 STATE（同
            # fh）；不改任何计数，失败批次天然回滚。返回键序 op,windows；
            # windows 覆盖 from..to 并升序，项键序 window,backends,total；
            # backends 按加入序列全部现存后端，项键序 id,D,F,S（fh 同款五
            # 键对象，缺窗为 0）；total 键序 D,F,S，为全池逐项求和并封顶
            # 10^18。逐窗逐类拷贝，避免结果被批次内后续记账污染。
            _, start, end, now = op
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            windows = []
            for window in range(start, end + 1):
                total = new_fault_stats()
                entries = []
                for backend_id, record in backends.items():
                    stats = record["fault_hist"].get(window)
                    if stats is None:
                        # 缺窗：各项为 0，求和不受影响。
                        stats = new_fault_stats()
                    entry = {"id": backend_id}
                    for kind in "DFS":
                        counts = stats[kind]
                        entry[kind] = dict(counts)
                        for field, value in counts.items():
                            total[kind][field] = min(
                                METRIC_CAP, total[kind][field] + value
                            )
                    entries.append(entry)
                windows.append(
                    {
                        "window": window,
                        "backends": entries,
                        "total": total,
                    }
                )
            results.append({"op": "fa", "windows": windows})

        elif op[0] == "fe":
            # 全池故障阈值告警：首评固化 (hi,lo,n) 并自 N 态起评；此后阈值
            # 须相同且 w 仅同前（同窗原样返回首评结果，不推进状态机）或
            # +1，变阈值或跳窗（含回退）报 STATE。w 窗须已结束且在 fh 保留
            # 窗内。v 为全池现存后端 w 窗三类 rejected 的逐项封顶和；N 态
            # 连续 n 窗 v>=hi 转 A，A 态连续 n 窗 v<=lo 转 N，否则连续计数
            # 归零；转换时 changed=true 且 run 归零。返回键序
            # op,w,state,v,run,changed。
            _, w, hi, lo, n, now = op
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            if alert is not None:
                if (hi, lo, n) != (alert["hi"], alert["lo"], alert["n"]):
                    # 变阈值。
                    fail(EXIT_STATE, "STATE")
                if w == alert["w"]:
                    # 同窗重报：原样返回首评结果，不推进状态机。
                    results.append(dict(alert["result"]))
                    continue
                if w != alert["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            v = 0
            for record in backends.values():
                stats = record["fault_hist"].get(w)
                if stats is not None:
                    for kind in "DFS":
                        v = min(METRIC_CAP, v + stats[kind]["rejected"])
            if alert is None:
                state = "N"
                run_count = 0
            else:
                state = alert["state"]
                run_count = alert["run"]
            changed = False
            prev_state = state
            if state == "N":
                if v >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    run_count = 0
            else:
                if v <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    run_count = 0
            result = {
                "op": "fe",
                "w": w,
                "state": state,
                "v": v,
                "run": run_count,
                "changed": changed,
            }
            if changed:
                # 状态转换：追加事件（键序 window,from,to,v,hi,lo,n），记录
                # 触发窗、转换前后状态、该次 v 与固化阈值；未转换不记录。
                # 同窗重报在上方已 continue，不会走到这里，故不重复。
                alert_events.append(
                    {
                        "window": w,
                        "from": prev_state,
                        "to": state,
                        "v": v,
                        "hi": hi,
                        "lo": lo,
                        "n": n,
                    }
                )
            # 评估后删除早于 w-59 的事件；w 严格递增，前端裁剪摊还 O(1)。
            cutoff = w - 59
            while alert_events and alert_events[0]["window"] < cutoff:
                alert_events.popleft()
            alert = {
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            results.append(result)

        elif op[0] == "ah":
            # 告警转换历史（只读）：from 早于最近 60 窗下界报 STATE（同
            # fa）；不推进告警状态机也不清理历史，失败批次天然回滚。返回
            # 键序 op,events；events 仅含区间 [from,to] 内的事件，按
            # window 升序（事件本就按评估窗递增入队），项键序
            # window,from,to,v,hi,lo,n；无转换返回空数组。逐项拷贝，避免
            # 结果被批次内后续评估污染。事件至多 60 项，时间 O(60)。
            _, start, end, now = op
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            events = [
                dict(event)
                for event in alert_events
                if start <= event["window"] <= end
            ]
            results.append({"op": "ah", "events": events})

        elif op[0] == "ea":
            # 后端错误率告警（每后端独立状态机）：首评固化 (hi,lo,n) 并自 N
            # 态起评；此后阈值须相同且 w 仅同前（同窗同阈值原样返回首评结果，
            # 不推进时钟外的状态机）或 +1，变阈值或跳窗（含回退）报 STATE。
            # 窗口越界（w 未结束或超出最近 60 窗保留下界）同样 STATE；窗口
            # 判定先于阈值/缓存，与 fe 同序。v 取该后端 w 窗 mh 同款
            # requests/errors：无请求 v=0，否则
            # v=floor(10000*errors/requests)，rate 为 v/100 两位定点串。N 态
            # 连续 n 窗 v>=hi 转 A，A 态连续 n 窗 v<=lo 转 N；方向不符与转换
            # 后连续数清 0；转换时 changed=true。转换（N→A 或 A→N）时追加
            # 一个历史事件，同窗重报（上方 continue）与未转换不追加；每次
            # 评估成功后删除该后端 window<w-59 的事件。返回键序
            # op,id,w,state,requests,errors,rate,run,changed。时间 O(1)。
            _, backend_id, w, hi, lo, n, now = op
            record = backends.get(backend_id)
            if record is None:
                # 未知 id 先于一切状态机判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            entry = err_alerts.get(backend_id)
            if entry is not None:
                if (hi, lo, n) != (entry["hi"], entry["lo"], entry["n"]):
                    # 变阈值。
                    fail(EXIT_STATE, "STATE")
                if w == entry["w"]:
                    # 同窗同阈值重报：原样返回首评结果，不推进状态机。
                    results.append(dict(entry["result"]))
                    continue
                if w != entry["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            metrics = record["metrics"].get(w)
            if metrics is None:
                requests = errors = 0
            else:
                requests = metrics[0]
                errors = metrics[1]
            # 无请求 v=0；否则 floor(10000*errors/requests)。
            v = 0 if requests == 0 else errors * 10000 // requests
            rate = "%d.%02d" % divmod(v, 100)
            if entry is None:
                state = "N"
                run_count = 0
            else:
                state = entry["state"]
                run_count = entry["run"]
            changed = False
            prev_state = state
            if state == "N":
                if v >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            else:
                if v <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            result = {
                "op": "ea",
                "id": backend_id,
                "w": w,
                "state": state,
                "requests": requests,
                "errors": errors,
                "rate": rate,
                "run": run_count,
                "changed": changed,
            }
            err_alerts[backend_id] = {
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            if changed:
                # 状态转换：追加事件（键序 window,from,to,rate,hi,lo,n），
                # 记录触发窗、转换前后状态（仅 N/A）、该次评估的两位定点
                # rate 串与固化阈值；未转换不记录。同窗重报在上方已
                # continue，不会走到这里，故不重复。
                history = err_events.get(backend_id)
                if history is None:
                    history = deque()
                    err_events[backend_id] = history
                history.append(
                    {
                        "window": w,
                        "from": prev_state,
                        "to": state,
                        "rate": rate,
                        "hi": hi,
                        "lo": lo,
                        "n": n,
                    }
                )
            # 评估后删除该后端早于 w-59 的事件；w 严格递增，前端裁剪摊还
            # O(1)，每后端至多 60 项。未转换且无队列时直接跳过。
            history = err_events.get(backend_id)
            if history is not None:
                cutoff = w - 59
                while history and history[0]["window"] < cutoff:
                    history.popleft()
            results.append(result)

        elif op[0] == "eh":
            # 后端错误率告警转换历史（只读）：未知 id 判 BACKEND，先于窗
            # 状态；from 早于最近 60 窗下界报 STATE（同 ah/fa）。不推进告警
            # 状态机、不清理历史，失败批次天然回滚。返回键序 op,id,events；
            # events 仅含该后端区间 [from,to] 内的事件，按 window 升序
            # （事件本就按评估窗递增入队），项键序
            # window,from,to,rate,hi,lo,n；无事件返回空数组。逐项拷贝，
            # 避免结果被批次内后续评估污染。事件至多 60 项，时间 O(60)。
            _, backend_id, start, end, now = op
            if backend_id not in backends:
                # 未知 id 先于窗口状态判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = err_events.get(backend_id)
            if history is None:
                events = []
            else:
                events = [
                    dict(event)
                    for event in history
                    if start <= event["window"] <= end
                ]
            results.append(
                {"op": "eh", "id": backend_id, "events": events}
            )

        elif op[0] == "pa":
            # 后端延迟分位告警（每后端独立状态机）：首评固化 (p,hi,lo,n)
            # 并自 N 态起评；此后参数须相同且 w 仅同前（同窗同参原样返回
            # 首评结果，不推进时钟外的状态机）或 +1，变参或跳窗（含回退）
            # 报 STATE。窗口越界（w 未结束或超出最近 60 窗保留下界）同样
            # STATE；窗口判定先于参数/缓存，与 ea 同序。窗值取该后端 w 窗
            # 五 latency 桶按 lp 规则计算的 samples、bucket、upper（单窗即
            # 区间 [w,w] 的 lp；无样本 0、null、null）。N 态连续 n 窗
            # bucket>=hi 转 A，A 态连续 n 窗 bucket<=lo 转 N；方向不符与转
            # 换后连续数清 0；转换时 changed=true。转换（N→A 或 A→N）时追加
            # 一个历史事件，同窗重报（上方 continue）与未转换不追加；每次
            # 评估成功后删除该后端 window<w-59 的事件。返回键序
            # op,id,w,p,state,samples,bucket,upper,run,changed（不变）。pa
            # 记账时间 O(1)。
            _, backend_id, w, percentile, hi, lo, n, now = op
            record = backends.get(backend_id)
            if record is None:
                # 未知 id 先于一切状态机判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            entry = percent_alerts.get(backend_id)
            if entry is not None:
                if (percentile, hi, lo, n) != (
                    entry["p"], entry["hi"], entry["lo"], entry["n"]
                ):
                    # 变参。
                    fail(EXIT_STATE, "STATE")
                if w == entry["w"]:
                    # 同窗同参重报：原样返回首评结果，不推进状态机。
                    results.append(dict(entry["result"]))
                    continue
                if w != entry["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            metrics = record["metrics"].get(w)
            if metrics is None:
                # 无样本：三者为 0、null、null，run 清零（沿 lp 口径）。
                samples = 0
                bucket = None
                upper = None
            else:
                latency = metrics[4]
                latency_buckets = list(latency)
                # 单窗无需封顶求和：桶计数本身已封顶 10^18，samples 沿用
                # lp 的五桶和再封顶。
                samples = min(METRIC_CAP, sum(latency_buckets))
                # rank=ceil(p*samples/100)，按桶 0..4 累计（截至 samples）
                # 取首个累计≥rank 者；p≤100 故 rank≤samples。
                rank = (percentile * samples + 99) // 100
                cumulative = 0
                bucket = None
                for i in range(5):
                    cumulative = min(
                        samples, cumulative + latency_buckets[i]
                    )
                    if cumulative >= rank:
                        bucket = i
                        break
                # 桶上界依次为 1、10、100、1000、null。
                upper = (1, 10, 100, 1000, None)[bucket]
            if entry is None:
                state = "N"
                run_count = 0
            else:
                state = entry["state"]
                run_count = entry["run"]
            changed = False
            prev_state = state
            if state == "N":
                if bucket is not None and bucket >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符（含无样本 bucket=null）：连续数清 0。
                    run_count = 0
            else:
                if bucket is not None and bucket <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符（含无样本 bucket=null）：连续数清 0。
                    run_count = 0
            result = {
                "op": "pa",
                "id": backend_id,
                "w": w,
                "p": percentile,
                "state": state,
                "samples": samples,
                "bucket": bucket,
                "upper": upper,
                "run": run_count,
                "changed": changed,
            }
            percent_alerts[backend_id] = {
                "p": percentile,
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            if changed:
                # 状态转换：追加事件（键序
                # window,from,to,p,samples,bucket,upper,hi,lo,n），记录触发
                # 窗、转换前后状态（仅 N/A），p 与 bucket/upper 沿用该次
                # pa 的评估值，samples/hi/lo/n 为整数；未转换不记录。同窗
                # 重报在上方已 continue，不会走到这里，故不重复。
                history = percent_events.get(backend_id)
                if history is None:
                    history = deque()
                    percent_events[backend_id] = history
                history.append(
                    {
                        "window": w,
                        "from": prev_state,
                        "to": state,
                        "p": percentile,
                        "samples": samples,
                        "bucket": bucket,
                        "upper": upper,
                        "hi": hi,
                        "lo": lo,
                        "n": n,
                    }
                )
            # 评估后删除该后端早于 w-59 的事件；w 严格递增，前端裁剪摊还
            # O(1)，每后端至多 60 项。未转换且无队列时直接跳过。
            history = percent_events.get(backend_id)
            if history is not None:
                cutoff = w - 59
                while history and history[0]["window"] < cutoff:
                    history.popleft()
            results.append(result)

        elif op[0] == "ph":
            # 后端延迟分位告警转换历史（只读）：未知 id 判 BACKEND，先于窗
            # 状态；from 早于最近 60 窗下界报 STATE（同 eh/ah）。不推进告警
            # 状态机、不清理历史，失败批次天然回滚。返回键序 op,id,events；
            # events 仅含该后端区间 [from,to] 内的事件，按 window 升序
            # （事件本就按评估窗递增入队），项键序
            # window,from,to,p,samples,bucket,upper,hi,lo,n；无事件返回空
            # 数组。逐项拷贝，避免结果被批次内后续评估污染。事件至多 60 项，
            # 时间 O(60)。
            _, backend_id, start, end, now = op
            if backend_id not in backends:
                # 未知 id 先于窗口状态判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = percent_events.get(backend_id)
            if history is None:
                events = []
            else:
                events = [
                    dict(event)
                    for event in history
                    if start <= event["window"] <= end
                ]
            results.append(
                {"op": "ph", "id": backend_id, "events": events}
            )

        elif op[0] == "xa":
            # 每后端重试/重映射告警（每 (id,k) 独立状态机）：首评固化
            # (hi,lo,n) 并自 N 态起评；此后阈值须相同且 w 仅同前（同窗同
            # 参原样返回首评结果，不推进时钟外的状态机）或 +1，变参或跳窗
            # （含回退）报 STATE。窗口越界（w 未结束或超出最近 60 窗保留
            # 下界）同样 STATE；窗口判定先于阈值/缓存，与 pa/ea 同序。k=R
            # 取该后端 w 窗 mh 口径 retries 累计，k=M 取 remaps 累计（缺窗
            # value=0），皆为非负整数。N 态连续 n 窗 value>=hi 转 A，A 态
            # 连续 n 窗 value<=lo 转 N；方向不符与转换后连续数清 0；转换时
            # changed=true。转换（N→A 或 A→N）时追加一个历史事件，同窗重
            # 报（上方 continue）与未转换不追加；每次推进状态机的成功评估
            # 后删除该键 window<w-59 的事件。返回键序
            # op,id,k,w,state,value,run,changed（不变）。xa 记账（含追加历
            # 史）均摊 O(1)、额外空间 O(B)。
            _, backend_id, kind_code, w, hi, lo, n, now = op
            record = backends.get(backend_id)
            if record is None:
                # 未知 id 先于一切状态机判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            alert_key = (backend_id, kind_code)
            entry = retry_alerts.get(alert_key)
            if entry is not None:
                if (hi, lo, n) != (entry["hi"], entry["lo"], entry["n"]):
                    # 变参。
                    fail(EXIT_STATE, "STATE")
                if w == entry["w"]:
                    # 同窗同参重报：原样返回首评结果，不推进状态机。
                    results.append(dict(entry["result"]))
                    continue
                if w != entry["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            metrics = record["metrics"].get(w)
            if metrics is None:
                # 缺窗：该窗 retries/remaps 累计均为 0。
                value = 0
            else:
                # metrics 布局 [requests,errors,retries,remaps,latency]；
                # R 取 retries（下标 2），M 取 remaps（下标 3）。
                value = metrics[2] if kind_code == "R" else metrics[3]
            if entry is None:
                state = "N"
                run_count = 0
            else:
                state = entry["state"]
                run_count = entry["run"]
            changed = False
            prev_state = state
            if state == "N":
                if value >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            else:
                if value <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            result = {
                "op": "xa",
                "id": backend_id,
                "k": kind_code,
                "w": w,
                "state": state,
                "value": value,
                "run": run_count,
                "changed": changed,
            }
            retry_alerts[alert_key] = {
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            if changed:
                # 状态转换：追加事件（键序 window,from,to,value,hi,lo,n），
                # 记录触发窗、转换前后状态（仅 N/A）、该次 xa 的评估值与固
                # 化阈值（五个数值字段皆为整数）；未转换不记录。同窗重报在
                # 上方已 continue，不会走到这里，故不重复。
                history = retry_events.get(alert_key)
                if history is None:
                    history = deque()
                    retry_events[alert_key] = history
                history.append(
                    {
                        "window": w,
                        "from": prev_state,
                        "to": state,
                        "value": value,
                        "hi": hi,
                        "lo": lo,
                        "n": n,
                    }
                )
            # 评估后删除该键早于 w-59 的事件；w 严格递增，前端裁剪摊还
            # O(1)，每键至多 60 项。未转换且无队列时直接跳过。
            history = retry_events.get(alert_key)
            if history is not None:
                cutoff = w - 59
                while history and history[0]["window"] < cutoff:
                    history.popleft()
            results.append(result)

        elif op[0] == "xh":
            # 每后端重试/重映射告警转换历史（只读）：未知 id 判 BACKEND，先
            # 于窗状态；from 早于最近 60 窗下界报 STATE（同 ph/eh）。不推进
            # 告警状态机、不清理历史，失败批次天然回滚。返回键序
            # op,id,k,events；events 仅含该 (id,k) 区间 [from,to] 内的事件，
            # 按 window 升序（事件本就按评估窗递增入队），项键序
            # window,from,to,value,hi,lo,n；无事件返回空数组。逐项拷贝，避
            # 免结果被批次内后续评估污染。事件至多 60 项，时间 O(60)。
            _, backend_id, kind_code, start, end, now = op
            if backend_id not in backends:
                # 未知 id 先于窗口状态判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            history = retry_events.get((backend_id, kind_code))
            if history is None:
                events = []
            else:
                events = [
                    dict(event)
                    for event in history
                    if start <= event["window"] <= end
                ]
            results.append(
                {"op": "xh", "id": backend_id, "k": kind_code,
                 "events": events}
            )

        elif op[0] == "xg":
            # 每后端重试/重映射告警汇总（只读）：未知 id 判 BACKEND，先于
            # 窗状态；from 早于最近 60 窗下界报 STATE（同 xh）。不推进告警
            # 状态机、不裁剪历史，失败批次天然回滚。state 取该 (id,k) 当前
            # xa 状态，从未评估为 N；last 取现存历史中最近转换窗（队列按
            # window 升序，即末项之窗），不限查询区间，无事件为 null；
            # raised/cleared 分别统计闭区间 [from,to] 内 N→A、A→N 事件，
            # total 为两者之和。返回键序
            # op,id,k,state,last,total,raised,cleared。事件至多 60 项，单次
            # 遍历完成统计，时间 O(60)、额外空间 O(1)。
            _, backend_id, kind_code, start, end, now = op
            if backend_id not in backends:
                # 未知 id 先于窗口状态判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            entry = retry_alerts.get((backend_id, kind_code))
            state = "N" if entry is None else entry["state"]
            history = retry_events.get((backend_id, kind_code))
            last_window = None
            raised = 0
            cleared = 0
            if history is not None:
                for event in history:
                    window = event["window"]
                    # last 不限查询区间：现存 60 窗历史中的最近转换窗。
                    if last_window is None or window > last_window:
                        last_window = window
                    if start <= window <= end:
                        if event["to"] == "A":
                            # N→A。
                            raised += 1
                        else:
                            # A→N。
                            cleared += 1
            results.append(
                {"op": "xg", "id": backend_id, "k": kind_code,
                 "state": state, "last": last_window,
                 "total": raised + cleared, "raised": raised,
                 "cleared": cleared}
            )

        elif op[0] == "xp":
            # 池级重试/重映射告警概览（只读）：from 早于最近 60 窗下界报
            # STATE（同 xg）；不推进告警状态机、不裁剪历史，失败批次天然回
            # 滚。backends 仅列现存后端，按 id 的 UTF-8 字节升序，项键序
            # id,R,M；R/M 各按 state,raised,cleared——state 取该 (id,k) 当
            # 前 xa 状态（未首评为 N），raised/cleared 为闭区间 [from,to]
            # 内现存历史的 N→A、A→N 事件数。total 键序 R,M，各按
            # alerting,raised,cleared：alerting 为当前 A 态后端数，后二值
            # 为明细之和；三值封顶 10^18。空池 backends 为空数组、total 全
            # 零。每后端两键历史各至多 60 项，时间 O(B log B+60B)、额外
            # 空间 O(B)。
            _, start, end, now = op
            current = now // 60
            if start < max(0, current - 59):
                # from 早于最近 60 窗的下界。
                fail(EXIT_STATE, "STATE")
            total = {
                "R": {"alerting": 0, "raised": 0, "cleared": 0},
                "M": {"alerting": 0, "raised": 0, "cleared": 0},
            }
            entries = []
            for backend_id in sorted(backends, key=encode_backend_id):
                item = {"id": backend_id, "R": None, "M": None}
                for kind_code in ("R", "M"):
                    entry = retry_alerts.get((backend_id, kind_code))
                    state = "N" if entry is None else entry["state"]
                    raised = 0
                    cleared = 0
                    history = retry_events.get((backend_id, kind_code))
                    if history is not None:
                        for event in history:
                            if start <= event["window"] <= end:
                                if event["to"] == "A":
                                    # N→A。
                                    raised += 1
                                else:
                                    # A→N。
                                    cleared += 1
                    kind_total = total[kind_code]
                    if state == "A":
                        kind_total["alerting"] = min(
                            METRIC_CAP, kind_total["alerting"] + 1
                        )
                    kind_total["raised"] = min(
                        METRIC_CAP, kind_total["raised"] + raised
                    )
                    kind_total["cleared"] = min(
                        METRIC_CAP, kind_total["cleared"] + cleared
                    )
                    item[kind_code] = {
                        "state": state,
                        "raised": raised,
                        "cleared": cleared,
                    }
                entries.append(item)
            results.append(
                {"op": "xp", "backends": entries, "total": total}
            )

        elif op[0] == "na":
            # 每后端并发告警（每 id 独立状态机）：首评固化 (hi,lo,n) 并自 N
            # 态起评；此后阈值须相同且 w 仅同前（同窗同参原样返回首评结果，
            # 不推进状态机）或 +1，变参或跳窗（含回退）报 STATE。窗口越界
            # （w 未结束或超出最近 60 窗保留下界）同样 STATE；窗口判定先于
            # 阈值/缓存，与 xa 同序。窗值取该后端 w 窗 mx 口径的 samples/
            # peak（空窗均为 0），皆为非负整数。N 态连续 n 窗 samples>0 且
            # peak>=hi 转 A，A 态连续 n 窗 samples>0 且 peak<=lo 转 N；方向
            # 不符与转换后连续数清 0；转换时 changed=true。返回键序
            # op,id,w,state,samples,peak,run,changed。na 记账均摊 O(1)、
            # 额外空间 O(B)。
            _, backend_id, w, hi, lo, n, now = op
            record = backends.get(backend_id)
            if record is None:
                # 未知 id 先于一切状态机判定。
                fail(EXIT_BACKEND, "BACKEND")
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            entry = conc_alerts.get(backend_id)
            if entry is not None:
                if (hi, lo, n) != (entry["hi"], entry["lo"], entry["n"]):
                    # 变参。
                    fail(EXIT_STATE, "STATE")
                if w == entry["w"]:
                    # 同窗同参重报：原样返回首评结果，不推进状态机。
                    results.append(dict(entry["result"]))
                    continue
                if w != entry["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            snapshot = record["samples"].get(w)
            if not snapshot:
                # 空窗：samples/peak 均为 0。
                samples = 0
                peak = 0
            else:
                # 取 ms 同步维护的窗聚合，O(1)，不逐样本扫描。
                samples = snapshot["samples"]
                peak = snapshot["peak"]
            if entry is None:
                state = "N"
                run_count = 0
            else:
                state = entry["state"]
                run_count = entry["run"]
            changed = False
            if state == "N":
                if samples > 0 and peak >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            else:
                if samples > 0 and peak <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            result = {
                "op": "na",
                "id": backend_id,
                "w": w,
                "state": state,
                "samples": samples,
                "peak": peak,
                "run": run_count,
                "changed": changed,
            }
            conc_alerts[backend_id] = {
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            results.append(result)

        elif op[0] == "le":
            # 限流告警（每 (scope,id) 独立状态机）：首评固化 (hi,lo,n) 并自
            # N 态起评；此后阈值须相同且 w 仅同前（同窗同参原样返回首评结
            # 果，不推进状态机）或 +1，变参或跳窗（含回退）报 STATE。B 的
            # 未知 id 先于一切判定报 BACKEND；该标识既无在配桶也无在配配
            # 额、窗口未结束（含未来窗）或超出最近 60 窗保留下界均报
            # STATE；窗口判定先于阈值/缓存，与 na 同序。窗值取该标识 lh
            # 的 w 窗 token/quota（空窗均为 0），value 为二者之和并封顶
            # 10^18。N 态连续 n 窗 value>=hi 转 A，A 态连续 n 窗
            # value<=lo 转 N；方向不符与转换后连续数清 0；转换时
            # changed=true。返回键序
            # op,scope,id,w,state,token,quota,value,run,changed。le 记账
            # 均摊 O(1)、额外空间 O(K)。
            _, scope, alert_id, w, hi, lo, n, now = op
            if scope == "B" and alert_id not in backends:
                # B 的未知 id 先于一切状态机判定。
                fail(EXIT_BACKEND, "BACKEND")
            key_pair = (scope, alert_id)
            if key_pair not in buckets and key_pair not in quotas:
                # 该标识既无在配桶也无在配配额：无窗值可评。
                fail(EXIT_STATE, "STATE")
            current = now // 60
            if w >= current or w < max(0, current - 59):
                # 窗未结束（含未来窗），或已超出最近 60 窗的保留下界。
                fail(EXIT_STATE, "STATE")
            entry = limit_alerts.get(key_pair)
            if entry is not None:
                if (hi, lo, n) != (entry["hi"], entry["lo"], entry["n"]):
                    # 变参。
                    fail(EXIT_STATE, "STATE")
                if w == entry["w"]:
                    # 同窗同参重报：原样返回首评结果，不推进状态机。
                    results.append(dict(entry["result"]))
                    continue
                if w != entry["w"] + 1:
                    # 跳窗（含回退）。
                    fail(EXIT_STATE, "STATE")
            history = limit_hist.get(key_pair)
            row = history.get(w) if history is not None else None
            if row is None:
                # 空窗：token/quota 均为 0。
                token = 0
                quota = 0
            else:
                # 取 lh 同步维护的窗计数，O(1)，不逐窗扫描。
                token = row[2]
                quota = row[3]
            value = min(METRIC_CAP, token + quota)
            if entry is None:
                state = "N"
                run_count = 0
            else:
                state = entry["state"]
                run_count = entry["run"]
            changed = False
            if state == "N":
                if value >= hi:
                    run_count += 1
                    if run_count >= n:
                        state = "A"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            else:
                if value <= lo:
                    run_count += 1
                    if run_count >= n:
                        state = "N"
                        run_count = 0
                        changed = True
                else:
                    # 方向不符：连续数清 0。
                    run_count = 0
            result = {
                "op": "le",
                "scope": scope,
                "id": alert_id,
                "w": w,
                "state": state,
                "token": token,
                "quota": quota,
                "value": value,
                "run": run_count,
                "changed": changed,
            }
            limit_alerts[key_pair] = {
                "hi": hi,
                "lo": lo,
                "n": n,
                "state": state,
                "run": run_count,
                "w": w,
                "result": dict(result),
            }
            results.append(result)

        elif op[0] == "fx":
            _, cid, flow, key, timeout, now = op
            if ring_vnodes is None:
                # 未 chash 报 STATE，先于 cid 重复判定（与 oa 一致）。
                fail(EXIT_STATE, "STATE")
            if cid in connections:
                fail(EXIT_CONNECTION, "CONNECTION")
            # 环同 route：仅健康、熔断 C、排空 A 后端；不读写粘性映射。
            tokens = build_ring(backends, ring_vnodes)
            remaps = 0
            state = "R"
            chosen_id = None
            latency = 0
            first_id = None  # 环遍历的首个候选后端，backend=null 时归属于它
            if tokens:
                digests = [token[0] for token in tokens]
                key_hash = int.from_bytes(
                    hashlib.sha256(key.encode("utf-8")).digest(), "big"
                )
                index = bisect.bisect_left(digests, key_hash)
                if index == len(tokens):
                    index = 0  # 越界回绕到环首
                seen = set()
                for offset in range(len(tokens)):
                    backend_id = tokens[(index + offset) % len(tokens)][3]
                    if backend_id in seen:
                        continue
                    seen.add(backend_id)
                    if first_id is None:
                        first_id = backend_id
                    record = backends[backend_id]
                    segment = active_fault(record, now)
                    effect = fault_effect(segment, now)
                    cost = segment[3] if effect == "S" else 0
                    if effect == "D":
                        # D/故障相位 F 不可用：受影响且 D 失败，跳过即 remap。
                        observe_fault(record, segment, effect, True, now)
                        bump_fault(record, segment, "remaps", now)
                        remaps += 1
                        continue
                    # 可用后端：活动 S 段耗时超 timeout 记 rejected；effect N
                    # （含段间隙与 F 非故障相位）只作恢复观察。首个可用后端即
                    # 终止遍历，耗时超限不建连。
                    observe_fault(record, segment, effect, cost > timeout, now)
                    chosen_id = backend_id
                    latency = cost
                    if cost <= timeout:
                        state = "A"
                    break
            if state == "A":
                # 按 open 建连（opened_at=now）。
                establish_connection(cid, chosen_id, flow, now)
            results.append(
                {
                    "op": "fx",
                    "cid": cid,
                    "state": state,
                    "backend": chosen_id,
                    "latency": latency,
                    "remaps": remaps,
                }
            )
            # fx 完成后只追加一次等价 mr 度量，不新增结果项：归属取结果
            # backend，为 null 时取环遍历首个后端，环内无候选不记。
            metric_id = chosen_id if chosen_id is not None else first_id
            if metric_id is not None:
                record_metric(
                    metric_id, state == "A", latency, remaps, remaps, now
                )

        elif op[0] == "fr":
            _, cid, flow, key, timeout, max_attempts, now = op
            if ring_vnodes is None:
                # 未配环报 STATE，且先于 cid 重复判定。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无候选同样报 STATE（区别于 fx 的 R），先于 cid。
                fail(EXIT_STATE, "STATE")
            if cid in connections:
                fail(EXIT_CONNECTION, "CONNECTION")
            digests = [token[0] for token in tokens]
            key_hash = int.from_bytes(
                hashlib.sha256(key.encode("utf-8")).digest(), "big"
            )
            index = bisect.bisect_left(digests, key_hash)
            if index == len(tokens):
                index = 0  # 越界回绕到环首
            # 自哈希点按 fx 顺序遍历不同后端，至多 max 个，每个候选即一次尝试：
            # D/故障相位 F 失败、耗时 0；S 的 v>timeout 失败、耗时 timeout；
            # 否则成功、耗时 v 或 0。attempts 记录 (后端, 本次耗时)。
            attempts_made = []
            seen = set()
            state = "R"
            chosen_id = None
            for offset in range(len(tokens)):
                if len(attempts_made) >= max_attempts:
                    break
                backend_id = tokens[(index + offset) % len(tokens)][3]
                if backend_id in seen:
                    continue
                seen.add(backend_id)
                record = backends[backend_id]
                segment = active_fault(record, now)
                effect = fault_effect(segment, now)
                cost = segment[3] if effect == "S" else 0
                if effect == "D":
                    # D/故障相位 F：本尝试失败、耗时 0；受影响且 D 失败。
                    observe_fault(record, segment, effect, True, now)
                    attempts_made.append((backend_id, 0))
                    continue
                if cost > timeout:
                    # S 且 v>timeout：本尝试失败，耗时按 timeout 计后重试。
                    observe_fault(record, segment, effect, True, now)
                    attempts_made.append((backend_id, timeout))
                    continue
                # 首个成功尝试即终止：耗时 v 或 0；effect N（含段间隙与 F
                # 非故障相位）只作恢复观察。
                observe_fault(record, segment, effect, False, now)
                attempts_made.append((backend_id, cost))
                chosen_id = backend_id
                state = "A"
                break
            attempts = len(attempts_made)
            retries = max(attempts - 1, 0)
            latency = sum(cost for _, cost in attempts_made)
            # fr 失败后确有下一尝试时，为失败后端该次活动段的种类 retries、
            # remaps 各加 1：即除末次尝试外的全部（失败）尝试，同请求同后端
            # 至多一次（遍历本就去重）。
            for attempt_index in range(attempts - 1):
                attempt_record = backends[attempts_made[attempt_index][0]]
                attempt_segment = active_fault(attempt_record, now)
                bump_fault(attempt_record, attempt_segment, "retries", now)
                bump_fault(attempt_record, attempt_segment, "remaps", now)
            if state == "A":
                # 成功按 open 建连（opened_at=now）；耗尽拒绝不建连。
                establish_connection(cid, chosen_id, flow, now)
            results.append(
                {
                    "op": "fr",
                    "cid": cid,
                    "state": state,
                    "backend": chosen_id,
                    "attempts": attempts,
                    "retries": retries,
                    "latency": latency,
                    "remaps": retries,
                }
            )
            # 每个尝试后端各记一条 mr：仅成功尝试 ok=true；ms 为本次耗时；
            # 首项 retries/remaps 记总值，余项为 0。
            for idx, (metric_id, cost) in enumerate(attempts_made):
                attempt_ok = state == "A" and idx == attempts - 1
                attempt_retries = retries if idx == 0 else 0
                record_metric(
                    metric_id,
                    attempt_ok,
                    cost,
                    attempt_retries,
                    attempt_retries,
                    now,
                )

        elif op[0] == "fi":
            # 组合故障预演（只读）：在同一 now 对每个 key 独立模拟 fr 的哈希
            # 遍历与 D/F/S 规则，至多尝试 max 个不同后端；除共用时钟按 now
            # 推进外不改任何运行态（不读写粘性映射、不建连、不记 mr/fm/fh），
            # 失败批次天然回滚。环只建一次（digests 与之同序），时间 O(KBV)、
            # 额外空间 O(K+BV)。
            _, keys, timeout, max_attempts, now = op
            if ring_vnodes is None:
                # 未配环报 STATE，同 fr。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无合格候选同样报 STATE（同 fr，区别于 fx 的 R）。
                fail(EXIT_STATE, "STATE")
            digests = [token[0] for token in tokens]
            cases = []
            accepted = 0
            rejected = 0
            total_attempts = 0
            total_retries = 0
            for key in keys:
                state, chosen_id, attempts, latency = simulate_fr(
                    tokens, digests, key, timeout, max_attempts, now
                )
                if state == "A":
                    accepted += 1
                else:
                    rejected += 1
                total_attempts += attempts
                total_retries += max(attempts - 1, 0)
                cases.append(
                    {
                        "key": key,
                        "state": state,
                        "backend": chosen_id,
                        "attempts": attempts,
                        "latency": latency,
                    }
                )
            results.append(
                {
                    "op": "fi",
                    "now": now,
                    "cases": cases,
                    "summary": {
                        "accepted": accepted,
                        "rejected": rejected,
                        "attempts": total_attempts,
                        "retries": total_retries,
                        "remaps": total_retries,
                    },
                }
            )

        elif op[0] == "ft":
            # 故障时间线预演（只读）：对同一 key 在 times 各时刻独立模拟 fr
            # 的哈希遍历与 D/F/S 规则，每时刻至多尝试 max 个不同后端；除共用
            # 时钟按 now 推进外不改任何运行态（不读写粘性映射、不建连、不记
            # mr/fm/fh、不动告警），失败批次天然回滚。环只建一次（digests
            # 与之同序），时间 O(PBV)、额外空间 O(P+BV)，P 为 times 项数。
            _, key, times, timeout, max_attempts, now = op
            if ring_vnodes is None:
                # 未配环报 STATE，同 fr/fi。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无合格候选同样报 STATE（同 fr/fi）。
                fail(EXIT_STATE, "STATE")
            digests = [token[0] for token in tokens]
            cases = []
            for at in times:
                state, chosen_id, attempts, latency = simulate_fr(
                    tokens, digests, key, timeout, max_attempts, at
                )
                retries = max(attempts - 1, 0)
                cases.append(
                    {
                        "at": at,
                        "state": state,
                        "backend": chosen_id,
                        "attempts": attempts,
                        "retries": retries,
                        "remaps": retries,
                        "latency": latency,
                    }
                )
            results.append(
                {
                    "op": "ft",
                    "key": key,
                    "now": now,
                    "cases": cases,
                }
            )

        elif op[0] == "fd":
            # 故障差异预演（只读）：按 times 外层、keys 内层，对每个
            # (at, key) 分别用当前故障时间线与候选计划（fp 同款语义：列入
            # id 的时间线原子替换、未列入的保持原样）独立模拟 fr，逐项比较
            # 前后结果；除共用时钟按 now 推进外不改任何运行态（不读写粘性
            # 映射、不建连、不记 mr/fm/fh、不动告警），失败批次天然回滚。
            # 环与候选视图各只建一次，时间 O(T+PKBV)、额外空间 O(T+PK+BV)，
            # T/P/K 为 items/times/keys 项数。
            _, plan, keys, times, timeout, max_attempts, now = op
            for item_id in plan:
                if item_id not in backends:
                    # 候选引用未知后端报 BACKEND，先于环状态判定。
                    fail(EXIT_BACKEND, "BACKEND")
            if ring_vnodes is None:
                # 未配环报 STATE，同 fr/fi/ft。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无合格候选同样报 STATE（同 fr/fi/ft）。
                fail(EXIT_STATE, "STATE")
            digests = [token[0] for token in tokens]
            # 候选视图：列入 id 用计划段（解析期已按 a 规范化），未列入
            # 沿用当前时间线；全部 (at, key) 复用同一份只读视图。
            after_view = {}
            for backend_id, record in backends.items():
                new_faults = plan.get(backend_id)
                if new_faults is None:
                    after_view[backend_id] = (
                        record["faults"],
                        record["fault_a"],
                    )
                else:
                    after_view[backend_id] = (
                        new_faults,
                        [segment[1] for segment in new_faults],
                    )
            cases = []
            changed_count = 0
            for at in times:
                for key in keys:
                    before_state, before_id, before_attempts, before_latency = (
                        simulate_fr(
                            tokens, digests, key, timeout, max_attempts, at
                        )
                    )
                    after_state, after_id, after_attempts, after_latency = (
                        simulate_fr(
                            tokens, digests, key, timeout, max_attempts, at,
                            after_view,
                        )
                    )
                    before_retries = max(before_attempts - 1, 0)
                    after_retries = max(after_attempts - 1, 0)
                    before = {
                        "state": before_state,
                        "backend": before_id,
                        "attempts": before_attempts,
                        "retries": before_retries,
                        "remaps": before_retries,
                        "latency": before_latency,
                    }
                    after = {
                        "state": after_state,
                        "backend": after_id,
                        "attempts": after_attempts,
                        "retries": after_retries,
                        "remaps": after_retries,
                        "latency": after_latency,
                    }
                    changed = before != after
                    if changed:
                        changed_count += 1
                    cases.append(
                        {
                            "at": at,
                            "key": key,
                            "before": before,
                            "after": after,
                            "changed": changed,
                        }
                    )
            results.append(
                {
                    "op": "fd",
                    "now": now,
                    "cases": cases,
                    "summary": {
                        "total": len(cases),
                        "stable": len(cases) - changed_count,
                        "changed": changed_count,
                    },
                }
            )

        elif op[0] == "fc":
            # 故障候选单键预演（只读）：在 at 时刻分别用当前故障时间线与
            # 候选计划（fd 同款语义：列入 id 的时间线原子替换、未列入的保持
            # 原样）各模拟一次 fr 的哈希遍历与 D/F/S 规则，并按尝试序产出
            # trace；at 仅为只读模拟时刻、不进时钟，除共用时钟按 now 推进外
            # 不改任何运行态（不读写粘性映射、不建连、不记 mr/fm/fh、不动
            # 告警），失败批次天然回滚。环与候选视图各只建一次，建环排序
            # O(BV log(BV))，两次模拟各至多遍历 max 个不同后端，总时间
            # O(T+BV log(BV))；环/digests/视图 O(BV)、候选计划 O(T)，
            # 两侧 trace 各至多 max 项且不超过环上后端数，额外空间
            # O(T+BV)（T 为 items 项数，B 为后端数，V 为每后端虚拟节点）。
            _, plan, key, at, timeout, max_attempts, now = op
            for item_id in plan:
                if item_id not in backends:
                    # 候选引用未知后端报 BACKEND，先于环状态判定（同 fd）。
                    fail(EXIT_BACKEND, "BACKEND")
            if ring_vnodes is None:
                # 未配环报 STATE，同 fr/fi/ft/fd。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无合格候选同样报 STATE（同 fr/fi/ft/fd）。
                fail(EXIT_STATE, "STATE")
            digests = [token[0] for token in tokens]
            # 候选视图同 fd：列入 id 用计划段（解析期已按 a 规范化），未列入
            # 沿用当前时间线；前后两次模拟复用同一份只读视图。
            after_view = {}
            for backend_id, record in backends.items():
                new_faults = plan.get(backend_id)
                if new_faults is None:
                    after_view[backend_id] = (
                        record["faults"],
                        record["fault_a"],
                    )
                else:
                    after_view[backend_id] = (
                        new_faults,
                        [segment[1] for segment in new_faults],
                    )
            (
                before_state, before_id, before_attempts,
                before_latency, before_trace,
            ) = simulate_fc(
                tokens, digests, key, timeout, max_attempts, at
            )
            (
                after_state, after_id, after_attempts,
                after_latency, after_trace,
            ) = simulate_fc(
                tokens, digests, key, timeout, max_attempts, at,
                after_view,
            )
            before_retries = max(before_attempts - 1, 0)
            after_retries = max(after_attempts - 1, 0)
            before = {
                "state": before_state,
                "backend": before_id,
                "attempts": before_attempts,
                "retries": before_retries,
                "remaps": before_retries,
                "latency": before_latency,
                "trace": before_trace,
            }
            after = {
                "state": after_state,
                "backend": after_id,
                "attempts": after_attempts,
                "retries": after_retries,
                "remaps": after_retries,
                "latency": after_latency,
                "trace": after_trace,
            }
            results.append(
                {
                    "op": "fc",
                    "key": key,
                    "at": at,
                    "before": before,
                    "after": after,
                    "changed": before != after,
                }
            )

        elif op[0] == "oi":
            # 只读接纳预演：须已配置 chash 与 os；环同 route（仅健康、熔断
            # C、排空 A 后端），自 key 哈希点按 fr 顺序遍历不同后端至多 max
            # 个，按 fault、slow、capacity、quota 优先判失败。除共用时钟按
            # now 推进外不改任何运行态（不读写粘性映射、不建连、不扣令牌/
            # 配额、不记 mr/fm/fh），桶补充与固定窗推进只在只读投影上进行，
            # 失败批天然回滚。时间 O(BV)、额外空间 O(BV)。
            _, key, c, s, bc, cc, sc, timeout, max_attempts, now = op
            if ring_vnodes is None:
                # 未配环报 STATE，同 fr/fi。
                fail(EXIT_STATE, "STATE")
            if queue_cfg is None:
                # 未 os 报 STATE（capacity 判定依赖 os.cap）。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无合格候选同样报 STATE（同 fr/fi）。
                fail(EXIT_STATE, "STATE")
            digests = [token[0] for token in tokens]
            (
                state,
                chosen_id,
                attempts,
                latency,
                counts,
            ) = simulate_oi(
                tokens,
                digests,
                key,
                c,
                s,
                (bc, cc, sc),
                timeout,
                max_attempts,
                now,
            )
            retries = max(attempts - 1, 0)
            results.append(
                {
                    "op": "oi",
                    "state": state,
                    "backend": chosen_id,
                    "attempts": attempts,
                    "latency": latency,
                    "retries": retries,
                    "remaps": retries,
                    "fault": counts["fault"],
                    "slow": counts["slow"],
                    "capacity": counts["capacity"],
                    "quota": counts["quota"],
                }
            )

        elif op[0] == "od":
            # 只读接纳明细：遍历、判定优先级与 STATE 前置（未配环、未 os、
            # 环内无合格候选）同 oi；除共用时钟按 now 推进外不改任何运行
            # 态，桶补充与固定窗推进只在只读投影上进行，失败批天然回滚。
            # 结果键序 op,state,backend,trace；时间 O(BV)、额外空间 O(BV)。
            _, key, c, s, bc, cc, sc, timeout, max_attempts, now = op
            if ring_vnodes is None:
                # 未配环报 STATE，同 oi。
                fail(EXIT_STATE, "STATE")
            if queue_cfg is None:
                # 未 os 报 STATE（capacity 判定依赖 os.cap）。
                fail(EXIT_STATE, "STATE")
            tokens = build_ring(backends, ring_vnodes)
            if not tokens:
                # 环内无合格候选同样报 STATE（同 oi）。
                fail(EXIT_STATE, "STATE")
            digests = [token[0] for token in tokens]
            state, chosen_id, trace = simulate_od(
                tokens,
                digests,
                key,
                c,
                s,
                (bc, cc, sc),
                timeout,
                max_attempts,
                now,
            )
            results.append(
                {
                    "op": "od",
                    "state": state,
                    "backend": chosen_id,
                    "trace": trace,
                }
            )

        elif op[0] == "ts":
            _, ttl = op
            if ttl_cfg is None:
                # 首配作用于既有与后续连接（既有连接的 last 即其 opened_at）。
                ttl_cfg = ttl
            elif ttl_cfg != ttl:
                # 异值重配报 STATE；同值幂等。
                fail(EXIT_STATE, "STATE")
            results.append({"op": "ts", "ok": True})

        elif op[0] == "tm":
            _, ttl = op
            # 同值幂等；异值覆盖并立即作用于既有与后续连接（硬截止恒为
            # opened_at+当前登记值，故无需改写每连接状态）。不要求存在连接。
            hard_ttl_cfg = ttl
            results.append({"op": "tm", "ok": True})

        elif op[0] == "tk":
            _, cid, now = op
            if ttl_cfg is None:
                fail(EXIT_STATE, "STATE")
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            if (
                hard_ttl_cfg is not None
                and now >= connection[2] + hard_ttl_cfg
            ):
                # 命中硬到期同样报 CONNECTION，不刷新 last；硬截止不被 tk 延长。
                fail(EXIT_CONNECTION, "CONNECTION")
            if now >= connection[3] + ttl_cfg:
                # 命中已到期连接同样报 CONNECTION，不刷新 last。
                fail(EXIT_CONNECTION, "CONNECTION")
            connection[3] = now
            results.append({"op": "tk", "ok": True})

        elif op[0] == "te":
            _, cid, now = op
            if ttl_cfg is None and hard_ttl_cfg is None:
                # ts、tm 均未配报 STATE。
                fail(EXIT_STATE, "STATE")
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            idle, lifetime, deadline = conn_deadlines(connection)
            if now < deadline:
                state = "A"
                reason = None
            else:
                state = "E"
                # 空闲与硬截止同时到期取 L。
                reason = (
                    "L"
                    if lifetime is not None and now >= lifetime
                    else "I"
                )
            results.append(
                {
                    "op": "te",
                    "cid": cid,
                    "idle": idle,
                    "lifetime": lifetime,
                    "deadline": deadline,
                    "state": state,
                    "reason": reason,
                }
            )

        elif op[0] == "tg":
            _, cid, now = op
            if ttl_cfg is None:
                fail(EXIT_STATE, "STATE")
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            deadline = connection[3] + ttl_cfg
            # 查询不删除：到期仅表现为 state=E。
            results.append(
                {
                    "op": "tg",
                    "cid": cid,
                    "backend": connection[0],
                    "state": "A" if now < deadline else "E",
                    "opened": connection[2],
                    "last": connection[3],
                    "deadline": deadline,
                }
            )

        elif op[0] == "tx":
            _, now = op
            if ttl_cfg is None and hard_ttl_cfg is None:
                # ts、tm 均未配报 STATE（任一已配即可清理对应到期连接）。
                fail(EXIT_STATE, "STATE")
            # 按建连顺序（dict 保序）删除全部 now >= deadline 的连接（空闲
            # 或硬截止到期）并递减后端并发。
            expired = []
            for cid, connection in list(connections.items()):
                _, _, deadline = conn_deadlines(connection)
                if now >= deadline:
                    expired.append(cid)
                    del connections[cid]
                    conn_endpoints.pop(cid, None)
                    record = backends[connection[0]]
                    record["conns"] -= 1
                    drain = record["drain"]
                    if drain["state"] == "D" and record["conns"] == 0:
                        # 排空中末连消失即转 X，end 取本次 tx 的 now。
                        drain["state"] = "X"
                        drain["end"] = now
            results.append({"op": "tx", "expired": expired})

        elif op[0] == "ep":
            # 登记后端 IP 端点：同参幂等、异参覆盖，均返回 ok；O(1)。
            _, backend_id, host, port = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            record["endpoint"] = (host, port)
            results.append({"op": "ep", "ok": True})

        elif op[0] == "fw":
            # 连接转发快照查询（只读）：取建连时的快照，不受后续 ep 影响；
            # 未知 cid 报 CONNECTION，无快照报 STATE；O(1)。
            _, cid = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            endpoint = conn_endpoints.get(cid)
            if endpoint is None:
                fail(EXIT_STATE, "STATE")
            results.append(
                {
                    "op": "fw",
                    "cid": cid,
                    "backend": connection[0],
                    "host": endpoint[0],
                    "port": endpoint[1],
                }
            )

        elif op[0] == "eq":
            # 端点轮换存量查询（只读）：除推进时钟（已在循环开头完成）外不
            # 改任何运行态。目标后端须存在且已通过 ep 配置当前端点；按全局
            # 建连顺序（dict 保序）单遍统计该后端活动连接：快照与当前端点
            # 完全相同为 fresh，其余（含无快照）为 stale；items 仅列 stale，
            # 无快照项 host/port 为 null，age=now-opened。O(C)、O(C)。
            _, backend_id, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            endpoint = record["endpoint"]
            if endpoint is None:
                fail(EXIT_STATE, "STATE")
            total = 0
            fresh = 0
            items = []
            for cid, connection in connections.items():
                if connection[0] != backend_id:
                    continue
                total += 1
                snapshot = conn_endpoints.get(cid)
                if snapshot == endpoint:
                    fresh += 1
                    continue
                opened = connection[2]
                items.append(
                    {
                        "cid": cid,
                        "host": None if snapshot is None else snapshot[0],
                        "port": None if snapshot is None else snapshot[1],
                        "opened": opened,
                        "age": now - opened,
                    }
                )
            results.append(
                {
                    "op": "eq",
                    "id": backend_id,
                    "now": now,
                    "current": {"host": endpoint[0], "port": endpoint[1]},
                    "total": total,
                    "fresh": fresh,
                    "stale": total - fresh,
                    "items": items,
                }
            )

        elif op[0] == "ec":
            # 端点轮换存量清理：只关闭目标后端中快照不同于当前端点且
            # opened<=before 的活动连接，按全局建连顺序删除连接及快照、逐
            # 条递减并发，cids 保持关闭顺序；不扣令牌或配额，也不触发排队
            # 接纳。清理使排空 D 状态后端失去最后连接时沿用 close 规则转 X、
            # end=now。O(C)、O(C)。
            _, backend_id, before, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            endpoint = record["endpoint"]
            if endpoint is None:
                fail(EXIT_STATE, "STATE")
            closed_cids = []
            for cid, connection in list(connections.items()):
                if connection[0] != backend_id:
                    continue
                snapshot = conn_endpoints.get(cid)
                if snapshot == endpoint or connection[2] > before:
                    continue
                del connections[cid]
                conn_endpoints.pop(cid, None)
                record["conns"] -= 1
                closed_cids.append(cid)
            drain = record["drain"]
            if drain["state"] == "D" and record["conns"] == 0:
                # 沿用 close：排空中最后连接被清理即转 X，end 取本次 now。
                drain["state"] = "X"
                drain["end"] = now
            results.append(
                {
                    "op": "ec",
                    "id": backend_id,
                    "now": now,
                    "closed": len(closed_cids),
                    "cids": closed_cids,
                }
            )

        elif op[0] == "er":
            # 全池端点轮换盘点（只读）：除推进时钟（已在循环开头完成）外不
            # 改任何运行态。items 按后端加入顺序列出已通过 ep 登记当前端
            # 点的后端；每后端的 connections 沿用 eq 口径，仅列 stale（快
            # 照不同于当前端点，含无快照）连接并按全局建连顺序（dict 保
            # 序）排列，无快照项 host/port 为 null，age=now-opened。先单
            # 遍扫描连接按后端聚合计数与 stale 明细（O(C)），再按加入序
            # 遍历后端（O(B)）输出，汇总跨后端计数。O(B+C)、O(B+C)。
            _, now = op
            # backend_id -> [total, fresh, stale_items]
            per_backend = {}
            for cid, connection in connections.items():
                backend_id = connection[0]
                record = backends[backend_id]
                endpoint = record["endpoint"]
                if endpoint is None:
                    # 未登记端点的后端不计入盘点。
                    continue
                entry = per_backend.get(backend_id)
                if entry is None:
                    entry = [0, 0, []]
                    per_backend[backend_id] = entry
                entry[0] += 1
                snapshot = conn_endpoints.get(cid)
                if snapshot == endpoint:
                    entry[1] += 1
                    continue
                opened = connection[2]
                entry[2].append(
                    {
                        "cid": cid,
                        "host": None if snapshot is None else snapshot[0],
                        "port": None if snapshot is None else snapshot[1],
                        "opened": opened,
                        "age": now - opened,
                    }
                )
            items = []
            sum_total = 0
            sum_fresh = 0
            for backend_id, record in backends.items():
                endpoint = record["endpoint"]
                if endpoint is None:
                    continue
                total, fresh, stale_items = per_backend.get(
                    backend_id, (0, 0, [])
                )
                stale = total - fresh
                items.append(
                    {
                        "id": backend_id,
                        "current": {"host": endpoint[0], "port": endpoint[1]},
                        "total": total,
                        "fresh": fresh,
                        "stale": stale,
                        "connections": stale_items,
                    }
                )
                sum_total += total
                sum_fresh += fresh
            results.append(
                {
                    "op": "er",
                    "now": now,
                    "items": items,
                    "summary": {
                        "backends": len(items),
                        "total": sum_total,
                        "fresh": sum_fresh,
                        "stale": sum_total - sum_fresh,
                    },
                }
            )

        elif op[0] == "ex":
            # 全池端点轮换批量清理：处理集合为已通过 ep 登记当前端点的后
            # 端；按全局建连顺序单遍扫描，仅关闭快照不同于当前端点且
            # opened<=before 的活动连接，删除连接及快照、逐条递减并发；
            # cids 按全局关闭次序分组，items 仅列本次有关闭项的后端且按
            # 后端加入顺序排列。不消费等待队列、令牌或配额，也不触发重新
            # 调度。沿用 ec：排空 D 后端失去最后连接时转 X、end=now，不改
            # forced。无符合项为成功的确定性空操作；处理无失败路径，天然
            # 原子。O(B+C)、O(B+C)。
            _, before, now = op
            eligible = {
                backend_id
                for backend_id, record in backends.items()
                if record["endpoint"] is not None
            }
            # backend_id -> 本次关闭 cids（保持全局建连顺序）
            closed_groups = {}
            for cid, connection in list(connections.items()):
                backend_id = connection[0]
                if backend_id not in eligible:
                    continue
                record = backends[backend_id]
                snapshot = conn_endpoints.get(cid)
                if snapshot == record["endpoint"] or connection[2] > before:
                    continue
                del connections[cid]
                conn_endpoints.pop(cid, None)
                record["conns"] -= 1
                closed_groups.setdefault(backend_id, []).append(cid)
            items = []
            closed_total = 0
            for backend_id, record in backends.items():
                cids = closed_groups.get(backend_id)
                if not cids:
                    continue
                drain = record["drain"]
                if drain["state"] == "D" and record["conns"] == 0:
                    # 沿用 ec/close：排空中最后连接被清理即转 X，end 取本
                    # 次 now；forced 维持原值不变。
                    drain["state"] = "X"
                    drain["end"] = now
                items.append({"id": backend_id, "cids": cids})
                closed_total += len(cids)
            results.append(
                {
                    "op": "ex",
                    "now": now,
                    "items": items,
                    "closed": closed_total,
                }
            )

        elif op[0] == "ey":
            # 后端端点原子条件变更：形状/字段/编码与时钟倒退已在解析期及
            # 共用时钟块判 INPUT，INPUT 先于 BACKEND、BACKEND 先于 STATE。
            # 后端不存在报 BACKEND；当前端点等于 target 时无论 base 是否
            # 仍匹配均视为成功的幂等重报且不重写登记；否则只有当前端点与
            # base 逐值相等时才原子写入 target（target 为 null 即清除登
            # 记）；其余比较失败报 STATE。只改后端当前端点：不迁移、关闭
            # 或重新调度活动连接，不消费令牌、配额或等待队列，建连快照与
            # 其他运行态均不变。失败随整批丢弃，时钟与此前操作一并回滚。
            # O(1)、O(1)。
            _, backend_id, base, target, now = op
            record = backends.get(backend_id)
            if record is None:
                fail(EXIT_BACKEND, "BACKEND")
            current = record["endpoint"]
            if current != target:
                if current != base:
                    fail(EXIT_STATE, "STATE")
                # 条件匹配：原子写入（清除或改登记）。
                record["endpoint"] = target
            results.append(
                {
                    "op": "ey",
                    "id": backend_id,
                    "endpoint": (
                        None if target is None
                        else {"host": target[0], "port": target[1]}
                    ),
                    "ok": True,
                }
            )

        elif op[0] == "eb":
            # 批量后端端点原子条件变更：形状/字段/数量/重复 id/端点/编码
            # 与时钟倒退已在解析期及共用时钟块判 INPUT，INPUT 先于
            # BACKEND、BACKEND 先于 STATE。执行期先按 items 顺序确认所有
            # 后端存在（首个未知报 BACKEND），再统一检查条件，全部成立后
            # 才按输入顺序写入——任何阶段都不提前写入。每项当前端点等于
            # target 时视为已满足（不要求 base 仍匹配、不重写登记），否则
            # 必须逐值等于 base 才可写入 target（target 为 null 即清除登
            # 记）；任一后端既不等于 base 也不等于 target 报 STATE。只改
            # 各后端当前端点：不迁移、关闭或重新调度活动连接，不消费令
            # 牌、配额或等待队列，建连快照与其他运行态均不变，语义逐项同
            # ey。失败随整批丢弃，时钟与此前操作一并回滚。O(N)、O(N)。
            _, change_items, now = op
            records = []
            for backend_id, _base, _target in change_items:
                record = backends.get(backend_id)
                if record is None:
                    fail(EXIT_BACKEND, "BACKEND")
                records.append(record)
            # 条件整批预检：全部成立才进入写入，保证原子性。
            for record, (_backend_id, base, target) in zip(
                records, change_items
            ):
                current = record["endpoint"]
                if current == target or current == base:
                    continue
                fail(EXIT_STATE, "STATE")
            result_items = []
            for record, (backend_id, base, target) in zip(
                records, change_items
            ):
                if record["endpoint"] != target:
                    # 预检已保证此处旧值逐值等于 base，原子写入。
                    record["endpoint"] = target
                result_items.append(
                    {
                        "id": backend_id,
                        "endpoint": (
                            None if target is None
                            else {"host": target[0], "port": target[1]}
                        ),
                    }
                )
            results.append(
                {"op": "eb", "items": result_items, "ok": True}
            )

        elif op[0] == "ez":
            # 批量端点切换只读预演：形状/字段/数量/重复 id/端点/编码与时
            # 钟倒退已在解析期及共用时钟块判 INPUT，INPUT 先于 BACKEND。
            # 执行期先按 items 顺序确认所有后端存在（首个未知报 BACKEND），
            # 再按操作起始快照计算——除共用块已推进的时钟外不改变任何运
            # 行态，故后端端点、连接与建连快照均保持快照口径。为请求后端
            # 建立 O(N) 计数槽，单遍扫描活动连接 O(C)：total 为该后端活
            # 动连接数，fresh 按建连时端点快照是否等于 target 划分，无快
            # 照按 None 比较（target=null 时无快照连接计 fresh），
            # stale=total-fresh。每项 current==target 为 unchanged，否则
            # current==base 为 applicable，其余为 conflict；存在任一
            # conflict 时 ready=false，但仍返回完整预演明细。O(N+C)、
            # O(N)。
            _, preview_items, now = op
            records = []
            for backend_id, _base, _target in preview_items:
                record = backends.get(backend_id)
                if record is None:
                    fail(EXIT_BACKEND, "BACKEND")
                records.append(record)
            # 请求 id -> items 下标（id 已在解析期保证互不重复）。
            index_by_id = {}
            # 每项 [total, fresh]，按请求顺序排列。
            counts = []
            for idx, (backend_id, _base, _target) in enumerate(preview_items):
                index_by_id[backend_id] = idx
                counts.append([0, 0])
            for _cid, connection in connections.items():
                idx = index_by_id.get(connection[0])
                if idx is None:
                    continue
                counts[idx][0] += 1
                target = preview_items[idx][2]
                snapshot = conn_endpoints.get(_cid)
                if snapshot == target:
                    counts[idx][1] += 1
            result_items = []
            n_applicable = 0
            n_unchanged = 0
            n_conflict = 0
            sum_connections = 0
            sum_stale = 0
            ready = True
            for (backend_id, base, target), record, (total, fresh) in zip(
                preview_items, records, counts
            ):
                current = record["endpoint"]
                if current == target:
                    status = "unchanged"
                    n_unchanged += 1
                elif current == base:
                    status = "applicable"
                    n_applicable += 1
                else:
                    status = "conflict"
                    n_conflict += 1
                    ready = False
                stale = total - fresh
                sum_connections += total
                sum_stale += stale
                result_items.append(
                    {
                        "id": backend_id,
                        "current": (
                            None if current is None
                            else {"host": current[0], "port": current[1]}
                        ),
                        "target": (
                            None if target is None
                            else {"host": target[0], "port": target[1]}
                        ),
                        "status": status,
                        "total": total,
                        "fresh": fresh,
                        "stale": stale,
                    }
                )
            results.append(
                {
                    "op": "ez",
                    "now": now,
                    "ready": ready,
                    "items": result_items,
                    "summary": {
                        "total": len(preview_items),
                        "applicable": n_applicable,
                        "unchanged": n_unchanged,
                        "conflict": n_conflict,
                        "connections": sum_connections,
                        "stale": sum_stale,
                    },
                }
            )

        elif op[0] == "ej":
            # 批量端点条件切换并清理旧连接：形状/字段/数量/重复 id/端点/编
            # 码、before 关系与时钟倒退已在解析期及共用时钟块判 INPUT，
            # INPUT 先于 BACKEND、BACKEND 先于 STATE。执行期先按 items 顺
            # 序确认所有后端存在（首个未知报 BACKEND），再按操作开始时的
            # 快照统一判定条件，全部成立后才写入与清理——任何阶段都不提
            # 前变更。每项当前端点等于 target 时视为已满足（不要求 base
            # 仍匹配、不重写登记），否则必须逐值等于 base 才写入 target；
            # 任一后端既不等于 base 也不等于 target 整批报 STATE。条件成
            # 立后先按输入顺序把尚未到达 target 的端点原子更新到 target，
            # 再按全局建连顺序（dict 保序）单遍扫描活动连接：仅 items 所
            # 列后端中 opened<=before 且建连端点快照不等于该项 target 的
            # 连接被关闭，删除连接与快照、逐条递减并发；无快照按 None 比
            # 较（target 为 null 时无快照连接保留）。items 后端之外、
            # opened>before 或快照等于 target 的连接均不动。不消费等待队
            # 列、令牌或配额，也不触发重新调度。沿用 ec/ex：仅当本次有关
            # 闭项的排空 D 后端因此失去最后连接时转 X、end=now，forced 维
            # 持原值不变。失败随整批丢弃，时钟与此前操作一并回滚。
            # O(N+C)、O(N+C)。
            _, change_items, before, now = op
            records = []
            for backend_id, _base, _target in change_items:
                record = backends.get(backend_id)
                if record is None:
                    fail(EXIT_BACKEND, "BACKEND")
                records.append(record)
            # 条件整批预检（操作开始时快照）：全部成立才进入写入，保证
            # 原子性；预检阶段不修改登记。
            for record, (_backend_id, base, target) in zip(
                records, change_items
            ):
                current = record["endpoint"]
                if current == target or current == base:
                    continue
                fail(EXIT_STATE, "STATE")
            # 条件全部成立：先按输入顺序原子更新尚未到达 target 的端点。
            for record, (_backend_id, base, target) in zip(
                records, change_items
            ):
                if record["endpoint"] != target:
                    # 预检已保证此处旧值逐值等于 base，原子写入。
                    record["endpoint"] = target
            # 请求 id -> items 下标（id 已在解析期保证互不重复）。
            index_by_id = {}
            for idx, (backend_id, _base, _target) in enumerate(change_items):
                index_by_id[backend_id] = idx
            # 每项本次关闭 cids（按全局建连顺序追加）。
            closed_groups = [[] for _ in change_items]
            for cid, connection in list(connections.items()):
                idx = index_by_id.get(connection[0])
                if idx is None:
                    continue
                if connection[2] > before:
                    continue
                target = change_items[idx][2]
                snapshot = conn_endpoints.get(cid)
                if snapshot == target:
                    # 含无快照连接与 target=null：None==None 保留。
                    continue
                record = records[idx]
                del connections[cid]
                conn_endpoints.pop(cid, None)
                record["conns"] -= 1
                closed_groups[idx].append(cid)
            result_items = []
            closed_total = 0
            for (backend_id, _base, target), record, cids in zip(
                change_items, records, closed_groups
            ):
                if cids:
                    drain = record["drain"]
                    if drain["state"] == "D" and record["conns"] == 0:
                        # 沿用 ec/ex：因本次清理失去最后连接的排空 D 后
                        # 端转 X，end 取本次 now；forced 维持原值不变。
                        drain["state"] = "X"
                        drain["end"] = now
                result_items.append(
                    {
                        "id": backend_id,
                        "endpoint": (
                            None if target is None
                            else {"host": target[0], "port": target[1]}
                        ),
                        "cids": cids,
                    }
                )
                closed_total += len(cids)
            results.append(
                {
                    "op": "ej",
                    "now": now,
                    "items": result_items,
                    "closed": closed_total,
                    "ok": True,
                }
            )

        elif op[0] == "es":
            # 全池唯一端点切换预约建立：形状/字段/数量/重复 id/端点/编码、
            # 时间范围与关系（now<=at、before<=at）及时钟倒退已在解析期及
            # 共用时钟块判 INPUT，INPUT 先于 BACKEND、BACKEND 先于 STATE。
            # 执行期仅按 items 顺序确认所有后端存在（首个未知报 BACKEND），
            # 不检查端点条件、不切换端点、不关闭连接。全部后端引用有效后
            # 保存规范化快照（解析期得到的 (id, base, target) 元组列表），
            # 全池唯一：同身份（digest、at）重报幂等且不重写快照，其余情
            # 形整体替换既有预约。O(N)，N 为预约项数。
            _, switch_items, before, at, now = op
            for backend_id, _base, _target in switch_items:
                if backends.get(backend_id) is None:
                    fail(EXIT_BACKEND, "BACKEND")
            digest = endpoint_switch_digest(switch_items, before)
            old_identity = ep_identity(ep_switch)
            if ep_switch is None or (ep_switch[3], ep_switch[2]) != (
                digest, at
            ):
                ep_switch = (switch_items, before, at, digest)
                # 生命周期审计：仅真正改变预约身份时追加——从空建立记
                # SET，以不同身份覆盖记 REPLACE；同身份幂等重报不记。
                ep_audit_append(
                    "SET" if old_identity is None else "REPLACE",
                    now, old_identity, (digest, at),
                )
            results.append(
                {"op": "es", "digest": digest, "at": at, "ok": True}
            )

        elif op[0] == "en":
            # 全池端点切换预约查询：只读、不推进时钟。无预约时 pending=
            # false，digest/at/before 依次为 null、items 为空数组；有预约
            # 时按保存顺序回显规范化项（固定键序 id,base,target，端点 null
            # 或 host,port 对象）。O(N) 输出。
            if ep_switch is None:
                results.append(
                    {
                        "op": "en",
                        "pending": False,
                        "digest": None,
                        "at": None,
                        "before": None,
                        "items": [],
                    }
                )
            else:
                q_items, q_before, q_at, q_digest = ep_switch
                results.append(
                    {
                        "op": "en",
                        "pending": True,
                        "digest": q_digest,
                        "at": q_at,
                        "before": q_before,
                        "items": [
                            {
                                "id": item_id,
                                "base": endpoint_json(base),
                                "target": endpoint_json(target),
                            }
                            for item_id, base, target in q_items
                        ],
                    }
                )

        elif op[0] == "eu":
            # 全池端点切换预约取消：形状/字段类型/范围/UTF-8 与时钟倒退已
            # 在解析期及共用时钟块判 INPUT，INPUT 先于预约匹配。无预约为
            # 幂等空操作：cancelled=false 并回显请求 digest、at，不改任何
            # 状态；有预约且 digest、at 同时匹配时原子删除，cancelled=true；
            # 有预约但任一身份不符报 STATE 并保留预约。取消只影响预约本
            # 身，不切换端点、不动连接。O(1)。
            _, digest, at, now = op
            if ep_switch is None:
                results.append(
                    {"op": "eu", "digest": digest, "at": at,
                     "cancelled": False}
                )
            else:
                _, _, reserved_at, reserved_digest = ep_switch
                if reserved_digest != digest or reserved_at != at:
                    fail(EXIT_STATE, "STATE")
                # 生命周期审计：成功取消现存预约记 CANCEL；无预约的幂等空
                # 取消已在上方分支返回、不产生事件。
                ep_audit_append(
                    "CANCEL", now, (reserved_digest, reserved_at), None,
                )
                ep_switch = None
                results.append(
                    {"op": "eu", "digest": digest, "at": at,
                     "cancelled": True}
                )

        elif op[0] == "ei":
            # 全池端点切换预约条件生效：形状/字段类型/范围/UTF-8 与时钟倒
            # 退已在解析期及共用时钟块判 INPUT。按序判定：无预约、身份
            # （digest、at）任一不符或 now<at（过早）报 STATE 且保留预约；
            # 进入生效后按 items 顺序确认后端仍全部存在（建立后被 remove
            # 删除报 BACKEND/3，优先于端点条件判定，同 ej 的 BACKEND 先于
            # STATE）；再按操作开始时快照统一预检端点条件（当前端点等于
            # target 视为已满足，否则须逐值等于 base，任一冲突报 STATE 且
            # 保留预约）。全部成立后沿用 ej：按输入顺序写入尚未到达 target
            # 的端点，按全局建连顺序单遍关闭 items 后端中 opened<=before
            # 且建连快照不等于该项 target 的连接，逐条递减并发；排空 D 后
            # 端因本次关闭失去最后连接时转 X、end=now，forced 维持原值；
            # 不消费队列、令牌、配额，不触发重新调度。成功后删除预约。
            # O(N+C)，N 为预约项数、C 为活动连接数。
            _, digest, at, now = op
            if ep_switch is None:
                fail(EXIT_STATE, "STATE")
            saved_items, saved_before, reserved_at, reserved_digest = ep_switch
            if reserved_digest != digest or reserved_at != at:
                fail(EXIT_STATE, "STATE")
            if now < at:
                # 过早：now 仅到达后才允许生效。
                fail(EXIT_STATE, "STATE")
            records = []
            for backend_id, _base, _target in saved_items:
                record = backends.get(backend_id)
                if record is None:
                    # 生效时后端已删除：BACKEND，先于端点条件判定。
                    fail(EXIT_BACKEND, "BACKEND")
                records.append(record)
            # 条件整批预检（操作开始时快照），与 ej 同式；预检不写登记。
            for record, (_backend_id, base, target) in zip(
                records, saved_items
            ):
                current = record["endpoint"]
                if current == target or current == base:
                    continue
                fail(EXIT_STATE, "STATE")
            # 条件全部成立：先按输入顺序原子更新尚未到达 target 的端点。
            for record, (_backend_id, base, target) in zip(
                records, saved_items
            ):
                if record["endpoint"] != target:
                    record["endpoint"] = target
            index_by_id = {}
            for idx, (backend_id, _base, _target) in enumerate(saved_items):
                index_by_id[backend_id] = idx
            closed_groups = [[] for _ in saved_items]
            for cid, connection in list(connections.items()):
                idx = index_by_id.get(connection[0])
                if idx is None:
                    continue
                if connection[2] > saved_before:
                    continue
                target = saved_items[idx][2]
                snapshot = conn_endpoints.get(cid)
                if snapshot == target:
                    continue
                record = records[idx]
                del connections[cid]
                conn_endpoints.pop(cid, None)
                record["conns"] -= 1
                closed_groups[idx].append(cid)
            result_items = []
            closed_total = 0
            for (backend_id, _base, target), record, cids in zip(
                saved_items, records, closed_groups
            ):
                if cids:
                    drain = record["drain"]
                    if drain["state"] == "D" and record["conns"] == 0:
                        drain["state"] = "X"
                        drain["end"] = now
                result_items.append(
                    {
                        "id": backend_id,
                        "endpoint": endpoint_json(target),
                        "cids": cids,
                    }
                )
                closed_total += len(cids)
            # 生命周期审计：成功生效记 APPLY（失败路径均在任何写入之前
            # fail，不产生事件）；随后删除预约。
            ep_audit_append("APPLY", now, (digest, at), None)
            ep_switch = None
            results.append(
                {
                    "op": "ei",
                    "now": now,
                    "items": result_items,
                    "closed": closed_total,
                    "ok": True,
                }
            )

        elif op[0] == "ed":
            # 全池端点切换预约轮询到期处理：轮询者只提交 now，原子读取并处
            # 理当前预约，避免查询与执行之间预约被替换。形状/字段类型/范围/
            # UTF-8 与时钟倒退已在解析期及共用时钟块判 INPUT；本操作没有其
            # 他错误路径——无预约、未到期、后端已删除、端点前提冲突都是成
            # 功结果而非错误。先按操作开始快照与预约顺序逐项检查后端与端
            # 点前提，并单遍扫描活动连接 O(C)：按全局建连序列出 opened 不
            # 晚于 before 且建连端点快照不等于该项 target 的 cids（无快照
            # 按 None 比较，opened==before 列入），删除后端不可能持有活动
            # 连接（remove 要求 conns==0），其 cids 恒空。整体状态：now<at
            # 为 WAITING（优先于其余判定，item 仍按快照各自报告）；否则优
            # 先 MISSING，其次 CONFLICT；两者都成功返回完整影响但保留预约
            # 且不改业务状态。全部现存且当前端点等于各自 target 或 base 时
            # APPLIED：先按预约顺序切换需要变化的端点，再关闭列出的连接
            # 并删除预约；排空 D 后端因本次关闭失去最后连接时沿用 ei/ej 规
            # 则转 X、end=now、forced 不变；不消费队列、令牌或配额，不重新
            # 调度。affected 恒为各项 cids 总数；closed 仅 APPLIED 等于
            # affected，其余为 0。O(N+C) 时间与结果空间，N 为预约项数、C
            # 为活动连接数。
            _, now = op
            if ep_switch is None:
                # 无预约：成功空结果，不改端点、连接或预约。
                results.append(
                    {
                        "op": "ed",
                        "now": now,
                        "status": "EMPTY",
                        "digest": None,
                        "at": None,
                        "items": [],
                        "affected": 0,
                        "closed": 0,
                    }
                )
            else:
                saved_items, saved_before, reserved_at, reserved_digest = (
                    ep_switch
                )
                # 预约顺序逐项快照：record 为 None 表示建立后后端已删除。
                item_records = []
                for backend_id, _base, _target in saved_items:
                    item_records.append(backends.get(backend_id))
                index_by_id = {}
                groups = [[] for _ in saved_items]
                for idx, ((backend_id, _b, _t), record) in enumerate(
                    zip(saved_items, item_records)
                ):
                    if record is not None:
                        index_by_id[backend_id] = idx
                for cid, connection in connections.items():
                    idx = index_by_id.get(connection[0])
                    if idx is None:
                        continue
                    if connection[2] > saved_before:
                        continue
                    target = saved_items[idx][2]
                    snapshot = conn_endpoints.get(cid)
                    if snapshot == target:
                        continue
                    groups[idx].append(cid)
                result_items = []
                any_missing = False
                any_conflict = False
                affected_total = 0
                for (backend_id, base, target), record, cids in zip(
                    saved_items, item_records, groups
                ):
                    if record is None:
                        exists = False
                        current = None
                        status = "MISSING"
                        any_missing = True
                    else:
                        exists = True
                        current = record["endpoint"]
                        if current == target:
                            status = "UNCHANGED"
                        elif current == base:
                            status = "APPLICABLE"
                        else:
                            status = "CONFLICT"
                            any_conflict = True
                    affected_total += len(cids)
                    result_items.append(
                        {
                            "id": backend_id,
                            "exists": exists,
                            "current": endpoint_json(current),
                            "target": endpoint_json(target),
                            "status": status,
                            "cids": cids,
                        }
                    )
                # 整体状态优先 WAITING（now<at），其次 MISSING，再其次
                # CONFLICT；全部可执行才 APPLIED。
                if now < reserved_at:
                    overall = "WAITING"
                elif any_missing:
                    overall = "MISSING"
                elif any_conflict:
                    overall = "CONFLICT"
                else:
                    overall = "APPLIED"
                closed_total = 0
                if overall == "APPLIED":
                    # 条件全部成立（预检已在快照阶段完成）：先按预约顺序
                    # 原子更新尚未到达 target 的端点。
                    for record, (_backend_id, base, target) in zip(
                        item_records, saved_items
                    ):
                        if record["endpoint"] != target:
                            record["endpoint"] = target
                    # 再按全局建连顺序（groups 内顺序即 dict 建连序）关闭
                    # 列出的连接，删除连接与快照、逐条递减并发。
                    for record, cids in zip(item_records, groups):
                        for cid in cids:
                            del connections[cid]
                            conn_endpoints.pop(cid, None)
                            record["conns"] -= 1
                    # 排空 D 后端因本次关闭失去最后连接时转 X，end=now，
                    # forced 维持原值；不消费队列、令牌或配额。
                    for record, cids in zip(item_records, groups):
                        if cids:
                            drain = record["drain"]
                            if drain["state"] == "D" and record["conns"] == 0:
                                drain["state"] = "X"
                                drain["end"] = now
                    closed_total = affected_total
                    # 生命周期审计：仅 APPLIED 记 APPLY；EMPTY/WAITING/
                    # MISSING/CONFLICT 不改变预约，均不产生事件。
                    ep_audit_append(
                        "APPLY", now,
                        (reserved_digest, reserved_at), None,
                    )
                    # 成功后删除预约。
                    ep_switch = None
                results.append(
                    {
                        "op": "ed",
                        "now": now,
                        "status": overall,
                        "digest": reserved_digest,
                        "at": reserved_at,
                        "items": result_items,
                        "affected": affected_total,
                        "closed": closed_total,
                    }
                )

        elif op[0] == "eo":
            # 全池端点切换预约条件替换（并发安全的改期/换内容）：形状/字段/
            # 数量/重复 id/端点/编码、时间范围与关系（now<=at、before<=at）
            # 及时钟倒退已在解析期及共用时钟块判 INPUT，INPUT 先于 BACKEND、
            # BACKEND 先于 STATE。执行期先按 items 顺序确认候选引用的后端全
            # 部存在（首个未知报 BACKEND），再读取当前预约做条件判定：无预
            # 约为 STATE；当前预约既不等于旧身份（base、base_at）也不等于
            # 新身份（候选摘要、at）时为 STATE 并保留原预约。旧身份匹配时
            # 原子替换为候选规范化快照；当前已等于新身份时视为幂等重报——
            # 即使旧身份已失效（预约已被另一成功的 eo/es 改成新值）也成功，
            # 且不重写预约状态（保留既有快照对象）。只改待生效预约：不切换
            # 端点、不关闭或调度连接，不消费队列、令牌或配额。失败随整批
            # 丢弃，时钟与此前操作一并回滚。O(N)，N 为 items 项数与规范化
            # 编码长度之和。
            _, base, base_at, switch_items, before, at, now = op
            for backend_id, _b, _t in switch_items:
                if backends.get(backend_id) is None:
                    fail(EXIT_BACKEND, "BACKEND")
            digest = endpoint_switch_digest(switch_items, before)
            if ep_switch is None:
                fail(EXIT_STATE, "STATE")
            _, _, reserved_at, reserved_digest = ep_switch
            matches_old = reserved_digest == base and reserved_at == base_at
            matches_new = reserved_digest == digest and reserved_at == at
            if not matches_old and not matches_new:
                fail(EXIT_STATE, "STATE")
            if matches_old and not matches_new:
                # 原子替换为候选快照；幂等重报（matches_new）不重写状态。
                ep_switch = (switch_items, before, at, digest)
                # 生命周期审计：仅旧身份匹配的首次替换记 REPLACE；已等于
                # 新身份的幂等重报（含新旧身份相同）不重写、不记事件。
                ep_audit_append(
                    "REPLACE", now,
                    (reserved_digest, reserved_at), (digest, at),
                )
            # 首次替换与同一请求重报逐字节同输出：base 回显请求值，digest
            # 为候选摘要，at 回显新时刻，ok 恒 true。
            results.append(
                {"op": "eo", "base": base, "digest": digest,
                 "at": at, "ok": True}
            )

        elif op[0] == "ev":
            # 全池端点切换预约只读预演：形状/字段类型/范围/摘要格式与时钟
            # 倒退已在解析期及共用时钟块判 INPUT，INPUT 先于预约匹配。身份
            # 判定：无预约或 digest、at 任一不符报 STATE 并保留预约。身份
            # 匹配后按操作开始快照检查预约保存的 items 与 before：未到期
            # （now<at）、后端已删除、当前端点既不等于 target 也不等于 base
            # 都不是错误，而是成功预演中各 item/整体 status 的一种状态；
            # 除共用块已推进的 now 外不改变任何运行态（端点、连接、排空、
            # 预约、队列、令牌、配额、指标、告警均不动，不关闭连接）。为
            # 现存预约后端建立 O(N) 槽，单遍扫描活动连接 O(C)：按全局建
            # 连序列出 opened<=before 且建连端点快照不等于该项 target 的
            # cids，无快照按 None 比较。整体不可执行时仍报告其余可计算
            # 项。O(N+C) 时间、O(N+C) 结果空间，N 为预约项数、C 为活动
            # 连接数。
            _, digest, at, now = op
            if ep_switch is None:
                fail(EXIT_STATE, "STATE")
            saved_items, saved_before, reserved_at, reserved_digest = ep_switch
            if reserved_digest != digest or reserved_at != at:
                fail(EXIT_STATE, "STATE")
            # 预约顺序逐项快照：record 为 None 表示建立后后端已被删除。
            item_records = []
            for backend_id, _base, _target in saved_items:
                item_records.append(backends.get(backend_id))
            index_by_id = {}
            groups = [[] for _ in saved_items]
            for idx, ((backend_id, _base, _target), record) in enumerate(
                zip(saved_items, item_records)
            ):
                # 删除项不可能再持有活动连接（remove 要求 conns==0），故
                # 仅为现存后端建立连接扫描槽；删除项 cids 恒为空。
                if record is not None:
                    index_by_id[backend_id] = idx
            for cid, connection in connections.items():
                idx = index_by_id.get(connection[0])
                if idx is None:
                    continue
                if connection[2] > saved_before:
                    continue
                target = saved_items[idx][2]
                snapshot = conn_endpoints.get(cid)
                if snapshot == target:
                    continue
                groups[idx].append(cid)
            result_items = []
            any_missing = False
            any_conflict = False
            closed_total = 0
            for (backend_id, base, target), record, cids in zip(
                saved_items, item_records, groups
            ):
                if record is None:
                    exists = False
                    current = None
                    status = "MISSING"
                    any_missing = True
                else:
                    exists = True
                    current = record["endpoint"]
                    if current == target:
                        status = "UNCHANGED"
                    elif current == base:
                        status = "APPLICABLE"
                    else:
                        status = "CONFLICT"
                        any_conflict = True
                closed_total += len(cids)
                result_items.append(
                    {
                        "id": backend_id,
                        "exists": exists,
                        "current": endpoint_json(current),
                        "target": endpoint_json(target),
                        "status": status,
                        "cids": cids,
                    }
                )
            # 整体状态优先级：未到期 EARLY；否则存在删除后端 MISSING；再
            # 否则存在端点冲突 CONFLICT；全部现存且当前端点等于各自 target
            # 或 base 时 READY。ready 仅在 READY 时为 true。
            if now < at:
                overall = "EARLY"
            elif any_missing:
                overall = "MISSING"
            elif any_conflict:
                overall = "CONFLICT"
            else:
                overall = "READY"
            results.append(
                {
                    "op": "ev",
                    "digest": digest,
                    "at": at,
                    "now": now,
                    "ready": overall == "READY",
                    "status": overall,
                    "items": result_items,
                    "closed": closed_total,
                }
            )

        elif op[0] == "ew":
            # 端点切换预约候选替换只读预演：形状/字段/数量/重复 id/端点/
            # 编码、时间范围与关系（now<=at、before<=at）及时钟倒退已在解
            # 析期及共用时钟块判 INPUT，INPUT 先于候选后端引用 BACKEND。执
            # 行期先按 items 顺序确认候选引用的后端全部存在（首个未知报
            # BACKEND），随后只读取当前预约做身份归类——任何身份（含无预
            # 约 MISSING 与冲突 CONFLICT）都是成功预演结果而非错误，身份不
            # 匹配不返回 STATE。除共用块已推进的 now 外不改变任何运行态：
            # 不写预约、不切换端点、不关闭连接，排空、队列、令牌、配额、指
            # 标、告警均不动。候选摘要、规范化与时间关系沿用 eo/es；现存后
            # 端按当前端点等于 target、等于 base 或均不等依次标为
            # UNCHANGED/APPLICABLE/CONFLICT，并单遍扫描活动连接 O(C)：按全
            # 局建连序列出 opened<=before 且建连端点快照不等于该项 target
            # 的 cids，无快照按 None 比较；ALREADY 也按当前快照重新计算。
            # O(N+C) 时间、O(N+C) 结果空间，N 为候选项数、C 为活动连接
            # 数。
            _, base, base_at, switch_items, before, at, now = op
            for backend_id, _b, _t in switch_items:
                if backends.get(backend_id) is None:
                    fail(EXIT_BACKEND, "BACKEND")
            digest = endpoint_switch_digest(switch_items, before)
            if ep_switch is None:
                status = "MISSING"
            else:
                _, _, reserved_at, reserved_digest = ep_switch
                if reserved_digest == digest and reserved_at == at:
                    # 已等于候选摘要与 at：按当前快照重新计算影响。
                    status = "ALREADY"
                elif reserved_digest == base and reserved_at == base_at:
                    status = "READY"
                else:
                    status = "CONFLICT"
            replaceable = status in ("READY", "ALREADY")
            # 候选项顺序逐项快照；删除后端不可能进入本分支（引用未知已
            # BACKEND），所有候选后端当前必然现存。
            index_by_id = {}
            groups = [[] for _ in switch_items]
            for idx, (backend_id, _base, _target) in enumerate(switch_items):
                index_by_id[backend_id] = idx
            for cid, connection in connections.items():
                idx = index_by_id.get(connection[0])
                if idx is None:
                    continue
                if connection[2] > before:
                    continue
                target = switch_items[idx][2]
                snapshot = conn_endpoints.get(cid)
                if snapshot == target:
                    continue
                groups[idx].append(cid)
            result_items = []
            closed_total = 0
            for (backend_id, item_base, target), cids in zip(
                switch_items, groups
            ):
                current = backends[backend_id]["endpoint"]
                if current == target:
                    item_status = "UNCHANGED"
                elif current == item_base:
                    item_status = "APPLICABLE"
                else:
                    item_status = "CONFLICT"
                closed_total += len(cids)
                result_items.append(
                    {
                        "id": backend_id,
                        "current": endpoint_json(current),
                        "target": endpoint_json(target),
                        "status": item_status,
                        "cids": cids,
                    }
                )
            results.append(
                {
                    "op": "ew",
                    "base": base,
                    "digest": digest,
                    "at": at,
                    "now": now,
                    "replaceable": replaceable,
                    "status": status,
                    "items": result_items,
                    "closed": closed_total,
                }
            )

        elif op[0] == "ek":
            # 端点切换预约生命周期审计游标查询（只读，不推进时钟、不改预约
            # 与审计窗口及下一 seq）：事件按 seq 升序、至多 limit 项。
            # latest 为已分配最大 seq（ep_audit_seq-1，初始 0），after>
            # latest（游标越过最新事件、指向尚未分配 seq）报 STATE。历史
            # 非空且 after 小于最旧保留事件 seq 减 1 时，after 与最旧事件
            # 之间必有已按 64 条窗口淘汰的事件，置 truncated=true 并自最旧
            # 事件读取；after 等于最旧 seq 减 1（缺口恰为淘汰区、不漏可读
            # 事件）或更大时不截断，仅取 seq>after 的事件。至多取 limit
            # 条；next 为末条 seq、无条目等于 after；more 表示该页之后是
            # 否仍有窗口内可读事件。响应固定键序
            # op,after,next,truncated,more,events；事件项固定键序
            # seq,now,action,before,after，before/after 为 null 或固定键序
            # digest,at 的预约身份。空历史 after=0 返回空数组、next=0、
            # truncated=false、more=false。ek 时间 O(A)（A≤64，常数）、结
            # 果空间 O(limit)；失败批次回滚且无 stdout。
            _, after, limit = op
            latest = ep_audit_seq - 1
            if after > latest:
                fail(EXIT_STATE, "STATE")
            truncated = False
            if ep_audit_events and after < ep_audit_events[0][0] - 1:
                truncated = True
                candidates = ep_audit_events
            else:
                candidates = (
                    event for event in ep_audit_events if event[0] > after
                )
            picked = []
            more = False
            for event in candidates:
                if len(picked) < limit:
                    picked.append(event)
                else:
                    more = True
                    break
            results.append(
                {
                    "op": "ek",
                    "after": after,
                    "next": picked[-1][0] if picked else after,
                    "truncated": truncated,
                    "more": more,
                    "events": [
                        {
                            "seq": seq,
                            "now": event_now,
                            "action": action,
                            "before": ep_identity_json(before),
                            "after": ep_identity_json(after_identity),
                        }
                        for seq, event_now, action, before, after_identity
                        in picked
                    ],
                }
            )

        elif op[0] == "el":
            # 端点切换预约生命周期审计只读筛选查询：在 ek 同口径的游标与
            # 64 条保留窗口之上，按动作集合 actions 与事件显式时钟闭区间
            # [since,until] 确定性筛选。不推进显式时钟、不改预约、审计窗
            # 口、事件生成/淘汰或下一 seq；重复查询逐字节一致。游标与截
            # 断规则同 ek：latest 为已分配最大 seq（初始 0），after>latest
            # 报 STATE；历史非空且 after 小于最旧保留事件 seq 减一时截断
            # 为 true 并自最旧事件读取，否则只取 seq>after。候选再要求
            # action 入选且 now∈[since,until]（端点闭区间），按 seq 升序
            # 至多取 limit 项；more 表示末项之后仍有符合相同条件的保留事
            # 件。next 取末项 seq、无结果等于 after。summary 仅统计本页
            # （被淘汰事件不参与）：total 为本页条数，余五项按动作计数。
            # 响应固定键序 op,after,next,truncated,more,events,summary；
            # 事件项与 ek 逐值同构；summary 固定键序
            # total,set,replace,cancel,apply,clear。单次时间 O(64)、结果
            # 额外空间 O(limit)；失败批次回滚且无 stdout。
            _, after, limit, actions, since, until = op
            latest = ep_audit_seq - 1
            if after > latest:
                fail(EXIT_STATE, "STATE")
            selected_actions = frozenset(actions)
            truncated = False
            if ep_audit_events and after < ep_audit_events[0][0] - 1:
                truncated = True
                candidates = ep_audit_events
            else:
                candidates = (
                    event for event in ep_audit_events if event[0] > after
                )
            picked = []
            more = False
            for event in candidates:
                _, event_now, action, _, _ = event
                if (
                    action in selected_actions
                    and since <= event_now <= until
                ):
                    if len(picked) < limit:
                        picked.append(event)
                    else:
                        more = True
                        break
            summary_counts = {
                "SET": 0, "REPLACE": 0, "CANCEL": 0, "APPLY": 0,
                "CLEAR": 0,
            }
            for event in picked:
                summary_counts[event[2]] += 1
            results.append(
                {
                    "op": "el",
                    "after": after,
                    "next": picked[-1][0] if picked else after,
                    "truncated": truncated,
                    "more": more,
                    "events": [
                        {
                            "seq": seq,
                            "now": event_now,
                            "action": action,
                            "before": ep_identity_json(before),
                            "after": ep_identity_json(after_identity),
                        }
                        for seq, event_now, action, before, after_identity
                        in picked
                    ],
                    "summary": {
                        "total": len(picked),
                        "set": summary_counts["SET"],
                        "replace": summary_counts["REPLACE"],
                        "cancel": summary_counts["CANCEL"],
                        "apply": summary_counts["APPLY"],
                        "clear": summary_counts["CLEAR"],
                    },
                }
            )

        elif op[0] == "em":
            # 端点切换预约身份追查（只读，不推进时钟、不改预约、审计窗口、
            # 下一 seq 或连接状态）：以 digest 与 at 组成预约身份，在
            # ep_audit 六十四条保留窗口内筛选 before 或 after 引用了该身份
            # 的事件。游标与截断规则同 ek/el：latest 为已分配最大 seq（初
            # 始 0），after>latest 报 STATE；历史非空且 after 小于最旧保留
            # 事件 seq 减一时置 truncated=true 并自最旧事件筛选（即使无匹
            # 配也保留该标志），否则只取 seq>after。side=BEFORE 仅匹配变
            # 更前身份，AFTER 仅匹配变更后身份，EITHER 匹配任一侧——事件
            # 两侧同时命中也只返回一次。按 seq 升序至多取 limit 项；more
            # 仅在末项之后仍有身份/side 匹配的保留事件时为 true，不匹配事
            # 件不置 more。next 取末条 seq、无结果保持 after。响应固定键序
            # op,digest,at,side,after,next,truncated,more,events；事件项与
            # ek 同构。空历史配 after=0 或窗口内无匹配时返回空 events、
            # next=after、more=false。单次时间 O(64)、结果额外空间
            # O(limit)；失败批次回滚且无 stdout。
            _, digest, at, side, after, limit = op
            target_identity = (digest, at)
            latest = ep_audit_seq - 1
            if after > latest:
                fail(EXIT_STATE, "STATE")
            match_before = side in ("BEFORE", "EITHER")
            match_after = side in ("AFTER", "EITHER")
            truncated = False
            if ep_audit_events and after < ep_audit_events[0][0] - 1:
                truncated = True
                candidates = ep_audit_events
            else:
                candidates = (
                    event for event in ep_audit_events if event[0] > after
                )
            picked = []
            more = False
            for event in candidates:
                _, _, _, before_identity, after_identity = event
                matched = (
                    (match_before and before_identity == target_identity)
                    or (
                        match_after
                        and after_identity == target_identity
                    )
                )
                if matched:
                    if len(picked) < limit:
                        picked.append(event)
                    else:
                        more = True
                        break
            results.append(
                {
                    "op": "em",
                    "digest": digest,
                    "at": at,
                    "side": side,
                    "after": after,
                    "next": picked[-1][0] if picked else after,
                    "truncated": truncated,
                    "more": more,
                    "events": [
                        {
                            "seq": seq,
                            "now": event_now,
                            "action": action,
                            "before": ep_identity_json(before),
                            "after": ep_identity_json(after_identity),
                        }
                        for seq, event_now, action, before, after_identity
                        in picked
                    ],
                }
            )

        elif op[0] == "eg":
            # 端点切换预约身份区间汇总（只读，不推进时钟、不改预约、审计窗
            # 口、下一 seq 或连接）：以 digest、at 组成预约身份，在
            # ep_audit 六十四条保留窗口内汇总左开右闭区间 (after,until] 的
            # 进出与终态。latest 为已分配最大 seq（初始 0），until>latest
            # 报 STATE；空历史只允许 until=0（latest=0 时 until>0 已被上
            # 一条判 STATE），返回 INACTIVE、空 events、全零 summary、
            # truncated=false。
            #
            # truncated 口径同 ek/el/em：历史非空且 after 小于最旧保留事件
            # seq 减一为 true，表示 after 与最旧事件之间已有事件被淘汰；与
            # events 是否为空无关。
            #
            # events 取 after<seq<=until 且 before 或 after 引用目标身份的
            # 保留事件，按 seq 升序、同一事件只列一次（结构同 ek）。
            # summary：total 为事件数；entered 统计 after==目标而
            # before!=目标，left 统计相反变化；其余五项按 action 计数。
            #
            # state 表示处理完 seq<=until 的事件后目标是否为当前预约：
            # 1. 区间内存在引用目标的事件——以末条事件 after 判定
            #    （ACTIVE/INACTIVE），即使此前前缀已淘汰，末条 after 仍直
            #    接决定终态；
            # 2. 区间无引用事件且 after==0（窗口始于 seq 1）且前缀完整
            #    （truncated=false，即最旧事件恰为 seq1）——seq1 之前无预
            #    约，窗口内又无目标变化，INACTIVE；
            # 3. 区间无引用事件但前缀完整（after>=最旧 seq-1）——seq=after
            #    处理后的身份可由保留事件确定（after 恰为最旧 seq-1 时取
            #    最旧事件的 before，否则取 seq=after 事件的 after），窗口
            #    内无目标事件故终态不变，逐值比较目标得 ACTIVE/INACTIVE；
            # 4. 所需前缀已淘汰（truncated=true）且无事件足以判定——
            #    UNKNOWN。
            _, digest, at, after, until = op
            target_identity = (digest, at)
            latest = ep_audit_seq - 1
            if until > latest:
                fail(EXIT_STATE, "STATE")
            action_counts = {
                "SET": 0, "REPLACE": 0, "CANCEL": 0, "APPLY": 0,
                "CLEAR": 0,
            }
            picked = []
            entered = 0
            left = 0
            if not ep_audit_events:
                # 空历史：latest=0，能走到这里必有 until=0。
                truncated = False
                state = "INACTIVE"
            else:
                oldest_seq = ep_audit_events[0][0]
                truncated = after < oldest_seq - 1
                last_match_after = None
                for event in ep_audit_events:
                    seq = event[0]
                    if seq <= after:
                        continue
                    if seq > until:
                        break
                    _, event_now, action, before_identity, after_identity = event
                    referenced = (
                        before_identity == target_identity
                        or after_identity == target_identity
                    )
                    if not referenced:
                        continue
                    picked.append(event)
                    action_counts[action] += 1
                    if (
                        after_identity == target_identity
                        and before_identity != target_identity
                    ):
                        entered += 1
                    elif (
                        before_identity == target_identity
                        and after_identity != target_identity
                    ):
                        left += 1
                    last_match_after = after_identity
                if last_match_after is not None:
                    state = (
                        "ACTIVE"
                        if last_match_after == target_identity
                        else "INACTIVE"
                    )
                elif not truncated:
                    # 窗口 (after,until] 内无引用事件，且判定 seq=after 处
                    # 状态所需的前缀完整保留：after==0 时 seq1 之前无预约；
                    # after==最旧 seq-1 时取最旧事件 before；否则取
                    # seq=after 事件的 after。终态在窗口内不变。
                    if after == 0:
                        identity_at_after = None
                    elif after == oldest_seq - 1:
                        identity_at_after = ep_audit_events[0][3]
                    else:
                        identity_at_after = ep_audit_events[after - oldest_seq][4]
                    state = (
                        "ACTIVE"
                        if identity_at_after == target_identity
                        else "INACTIVE"
                    )
                else:
                    state = "UNKNOWN"
            results.append(
                {
                    "op": "eg",
                    "digest": digest,
                    "at": at,
                    "after": after,
                    "until": until,
                    "truncated": truncated,
                    "state": state,
                    "events": [
                        {
                            "seq": seq,
                            "now": event_now,
                            "action": action,
                            "before": ep_identity_json(before),
                            "after": ep_identity_json(after_identity),
                        }
                        for seq, event_now, action, before, after_identity
                        in picked
                    ],
                    "summary": {
                        "total": len(picked),
                        "entered": entered,
                        "left": left,
                        "set": action_counts["SET"],
                        "replace": action_counts["REPLACE"],
                        "cancel": action_counts["CANCEL"],
                        "apply": action_counts["APPLY"],
                        "clear": action_counts["CLEAR"],
                    },
                }
            )

        elif op[0] == "ee":
            # 端点切换预约审计序号快照批量查询（只读，不接受 now、不推进时
            # 钟，不改预约、连接、后端、审计窗口或下一 seq，成功也不写审
            # 计）：按一批审计序号直接返回各序号处理完成后的预约快照。
            # latest 为已分配最大 seq（ep_audit_seq-1，初始 0）；任一 seq
            # 大于 latest 整批报 STATE（形状类 INPUT 已在解析期先行判过）。
            #
            # 快照口径：
            # 1. seq=0 恒为 EMPTY（任何审计事件发生前无预约）；
            # 2. seq 仍在 64 条保留窗口内（>=最旧保留 seq）——取该事件的
            #    after：非空为 ACTIVE 并回显 digest、at，为 null 为 EMPTY；
            # 3. seq 恰为最旧保留 seq 减一——取最旧事件的 before 作为该序
            #    号处理后的身份：null 为 EMPTY，非空为 ACTIVE；
            # 4. 其余更早序号（0<seq<最旧 seq-1）前缀已随窗口淘汰、无法
            #    由边界确定——返回 UNKNOWN 而非失败。
            # truncated 仅在本次至少一项 UNKNOWN 时为 true。items 与 seqs
            # 等长且保持输入顺序；窗口事件 seq 连续，物化一次后按
            # seq-最旧 seq 直接索引，单次时间 O(P+A)、结果额外空间 O(P)
            # （A≤64）。
            _, seqs = op
            latest = ep_audit_seq - 1
            if seqs[-1] > latest:
                fail(EXIT_STATE, "STATE")
            window = list(ep_audit_events)
            items = []
            truncated = False
            for seq in seqs:
                if seq == 0:
                    state = "EMPTY"
                    identity = None
                elif seq >= window[0][0]:
                    identity = window[seq - window[0][0]][4]
                    state = (
                        "ACTIVE" if identity is not None else "EMPTY"
                    )
                elif seq == window[0][0] - 1:
                    identity = window[0][3]
                    state = (
                        "ACTIVE" if identity is not None else "EMPTY"
                    )
                else:
                    state = "UNKNOWN"
                    identity = None
                    truncated = True
                if state == "ACTIVE":
                    item_digest, item_at = identity
                else:
                    item_digest, item_at = None, None
                items.append(
                    {
                        "seq": seq,
                        "state": state,
                        "digest": item_digest,
                        "at": item_at,
                    }
                )
            results.append(
                {
                    "op": "ee",
                    "latest": latest,
                    "truncated": truncated,
                    "items": items,
                }
            )

        elif op[0] == "ef":
            # 端点切换预约审计序号区间状态区段压缩（只读，不接受 now、不推
            # 进时钟，不改预约、连接、后端、审计窗口或下一 seq，成功也不
            # 写审计）：把闭区间 [first,last] 内各审计序号处理后的预约状态
            # 压缩为相邻同状态同身份的连续区段。latest 为已分配最大 seq
            # （ep_audit_seq-1，初始 0）；last>latest 报 STATE（形状类
            # INPUT 已在解析期先行判过）。空历史 latest=0，故仅 [0,0] 可
            # 达，返回单个 EMPTY 区段。
            #
            # 逐点口径与 ee 完全一致：
            # 1. seq=0 恒为 EMPTY；
            # 2. seq 仍在 64 条保留窗口内（>=最旧保留 seq）取该事件 after：
            #    null 为 EMPTY，非空为 ACTIVE（身份为 digest,at）；
            # 3. seq 恰为最旧保留 seq 减一取最旧事件 before；
            # 4. 其余更早序号前缀已淘汰、无法由窗口边界确定——UNKNOWN。
            # 全部序号空间 0..latest 上只有 O(A) 段恒值区间：点 0、
            # UNKNOWN 前缀（1..最旧-2）、before 边界点（最旧-1）及每条保
            # 留事件 after 生效到下一事件前（保留事件 seq 连续）。事件均
            # 真正改变预约身份，相邻区间身份必不同；交集后仍做一次相邻合
            # 并以兜底。区间与序号跨度无关：时间 O(A)、结果额外空间 O(A)
            # （A≤64），不逐点遍历（跨度可达 10^18）。
            _, first, last = op
            latest = ep_audit_seq - 1
            if last > latest:
                fail(EXIT_STATE, "STATE")
            regions = [(0, 0, "EMPTY", None)]
            window = list(ep_audit_events)
            if window:
                oldest_seq = window[0][0]
                if oldest_seq >= 3:
                    regions.append(
                        (1, oldest_seq - 2, "UNKNOWN", None)
                    )
                if oldest_seq >= 2:
                    before_identity = window[0][3]
                    regions.append(
                        (
                            oldest_seq - 1,
                            oldest_seq - 1,
                            "ACTIVE" if before_identity is not None
                            else "EMPTY",
                            before_identity,
                        )
                    )
                for index, event in enumerate(window):
                    event_seq = event[0]
                    after_identity = event[4]
                    region_end = (
                        window[index + 1][0] - 1
                        if index + 1 < len(window)
                        else latest
                    )
                    regions.append(
                        (
                            event_seq,
                            region_end,
                            "ACTIVE" if after_identity is not None
                            else "EMPTY",
                            after_identity,
                        )
                    )
            segments = []
            counts = {"UNKNOWN": 0, "EMPTY": 0, "ACTIVE": 0}

            def append_region(region_first, region_last, state, identity):
                length = region_last - region_first + 1
                counts[state] += length
                if (
                    segments
                    and segments[-1]["state"] == state
                    and segments[-1]["_identity"] == identity
                ):
                    segments[-1]["last"] = region_last
                    return
                if state == "ACTIVE":
                    seg_digest, seg_at = identity
                else:
                    seg_digest, seg_at = None, None
                segments.append(
                    {
                        "first": region_first,
                        "last": region_last,
                        "state": state,
                        "digest": seg_digest,
                        "at": seg_at,
                        "_identity": identity,
                    }
                )

            for region_lo, region_hi, state, identity in regions:
                overlap_first = max(first, region_lo)
                overlap_last = min(last, region_hi)
                if overlap_first <= overlap_last:
                    append_region(
                        overlap_first, overlap_last, state, identity
                    )
            for segment in segments:
                del segment["_identity"]
            points = last - first + 1
            results.append(
                {
                    "op": "ef",
                    "first": first,
                    "last": last,
                    "latest": latest,
                    "truncated": counts["UNKNOWN"] > 0,
                    "segments": segments,
                    "summary": {
                        "points": points,
                        "segments": len(segments),
                        "unknown": counts["UNKNOWN"],
                        "empty": counts["EMPTY"],
                        "active": counts["ACTIVE"],
                    },
                }
            )

        elif op[0] == "et":
            # 端点切换预约审计业务时间快照批量查询（只读，不接受 now、不推
            # 进时钟，不改预约、连接、后端、审计窗口或下一 seq，成功也不写
            # 审计）：以一批显式业务时间直接返回按 seq 升序处理完全部
            # event.now 不晚于该时刻的事件后的预约快照，调用方无需先把业务
            # 时间换算成审计 seq。latest 为已分配最大 seq（ep_audit_seq-1，
            # 初始 0）。操作开始时的全局逻辑时钟为 last_now（从未推进视为
            # 0）；任一 time 晚于它整批报 STATE（形状类 INPUT 已在解析期先
            # 行判过）。
            #
            # 逐时刻口径：
            # 1. 空历史——无事件可处理，任何不晚于时钟的合法 time 都是首条
            #    事件之前：EMPTY 且 seq=0（含时钟尚未推进、time=0 的情形）；
            # 2. time 早于最旧保留事件的 now——
            #    a. 最旧保留事件即 seq1（窗口从未淘汰、前缀完整）：该时刻确
            #       为完整历史的首条事件之前，EMPTY 且 seq=0；
            #    b. 最旧保留 seq 大于 1（此前已有事件随六十四条窗口淘汰）：
            #       判定该时刻所需前缀无法由窗口确定，UNKNOWN 且 seq=null；
            # 3. 否则以最旧保留事件的 before 为起点（time 等于最旧 now 时
            #    同样处理该时刻全部保留事件——起点 before 随即被同刻事件的
            #    after 覆盖），顺序应用所有 now<=time 的保留事件；事件 now
            #    非递减，同一 now 的事件全部应用，以其中最大 seq 的 after 为
            #    终态（null 为 EMPTY，非空为 ACTIVE 并回显 digest、at）；
            #    seq 返回最后应用的事件序号。
            # truncated 仅在本次至少一项 UNKNOWN 时为 true。times 严格递增
            # 且事件按 seq 升序、now 非递减：两个有序序列单遍归并即可，单
            # 次时间 O(P+A)、结果额外空间 O(P)（P≤64、A≤64）。
            _, times = op
            clock_now = 0 if last_now is None else last_now
            if times[-1] > clock_now:
                fail(EXIT_STATE, "STATE")
            window = list(ep_audit_events)
            items = []
            truncated = False
            event_index = 0
            for time_value in times:
                if not window:
                    # 空历史：首条事件之前恒 EMPTY、seq=0。
                    state = "EMPTY"
                    identity = None
                    last_seq = 0
                elif time_value < window[0][1]:
                    if window[0][0] == 1:
                        # 完整历史（从未淘汰）：首条事件之前无预约。
                        state = "EMPTY"
                        identity = None
                        last_seq = 0
                    else:
                        # 前缀已随窗口淘汰、该时刻状态无法由边界确定。
                        state = "UNKNOWN"
                        identity = None
                        last_seq = None
                        truncated = True
                else:
                    # 以最旧保留事件的 before 为起点，应用全部 now<=time 的
                    # 保留事件（event_index 随 time 单调前移）。
                    while (
                        event_index < len(window)
                        and window[event_index][1] <= time_value
                    ):
                        event_index += 1
                    last_event = window[event_index - 1]
                    last_seq = last_event[0]
                    identity = last_event[4]
                    state = (
                        "ACTIVE" if identity is not None else "EMPTY"
                    )
                if state == "ACTIVE":
                    item_digest, item_at = identity
                else:
                    item_digest, item_at = None, None
                items.append(
                    {
                        "time": time_value,
                        "seq": last_seq,
                        "state": state,
                        "digest": item_digest,
                        "at": item_at,
                    }
                )
            results.append(
                {
                    "op": "et",
                    "latest": ep_audit_seq - 1,
                    "truncated": truncated,
                    "items": items,
                }
            )

        elif op[0] == "se":
            # 运行态检查点导出：不推进时钟、不改状态。state 为规范化 JSON
            # 对象；紧凑 UTF-8 编码超 8MiB 报 OVERLOAD/7（无 stdout、整批
            # 回滚——se 本不改状态，故仅不产出结果）。digest 为紧凑编码
            # {"version":1,"state":...} 的小写 SHA-256。
            state = export_bundle(current_bundle())
            envelope = {"version": CHECKPOINT_VERSION, "state": state}
            try:
                state_bytes = json.dumps(
                    state, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                encoded = json.dumps(
                    envelope, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(state_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            digest = hashlib.sha256(encoded).hexdigest()
            results.append(
                {
                    "op": "se",
                    "version": CHECKPOINT_VERSION,
                    "digest": digest,
                    "state": state,
                }
            )

        elif op[0] == "sd":
            # 候选检查点只读差异：先按 si 的规则校验候选检查点（紧凑编码大
            # 小、摘要匹配、完整语义与规范化往返），再按 se 的规则取得当前
            # 状态的规范化内存快照；全程不推进逻辑时钟、不安装候选、不改变
            # 任何运行态或幂等缓存（候选束为局部对象）。候选校验成功后，当
            # 前状态紧凑编码超 se 的 8MiB 上限报 OVERLOAD/7。任何失败均无
            # stdout，整批操作（含此前操作的临时变化）回滚。
            _, version, digest, raw_state = op
            # —— 阶段一：按 si 规则校验候选（同 si 分支的错误优先级）——
            try:
                after_bytes = json.dumps(
                    raw_state, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                candidate_envelope = json.dumps(
                    {"version": version, "state": raw_state},
                    ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(after_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            if hashlib.sha256(candidate_envelope).hexdigest() != digest:
                fail(EXIT_INPUT, "INPUT")
            candidate = parse_checkpoint(raw_state)
            # 规范化往返：候选束重新规范化后的紧凑编码须与所给 state 逐字
            # 节一致（不强制所给 state 本身为规范化形态的语义偏差在此判
            # STATE）。after 原样返回输入摘要，故以所给 state 为 after。
            candidate_state = export_bundle(candidate)
            candidate_bytes = json.dumps(
                candidate_state, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if candidate_bytes != after_bytes:
                fail(EXIT_STATE, "STATE")
            # —— 阶段二：按 se 规则取得当前状态的规范化内存快照 ——
            before_state = export_bundle(current_bundle())
            try:
                before_bytes = json.dumps(
                    before_state, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(before_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            before_digest = hashlib.sha256(
                json.dumps(
                    {"version": CHECKPOINT_VERSION, "state": before_state},
                    ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            # —— 阶段三：按 se 顶层键序逐段比较 ——
            changes = []
            for section in before_state:
                before_value = before_state[section]
                after_value = raw_state[section]
                if before_value != after_value:
                    changes.append(
                        {
                            "section": section,
                            "before": section_digest(before_value),
                            "after": section_digest(after_value),
                        }
                    )
            results.append(
                {
                    "op": "sd",
                    "before": before_digest,
                    "after": digest,
                    "equal": before_bytes == after_bytes,
                    "changes": changes,
                    "summary": {
                        "sections": len(before_state),
                        "changed": len(changes),
                        "bytes_before": len(before_bytes),
                        "bytes_after": len(after_bytes),
                    },
                }
            )

        elif op[0] == "si":
            # 运行态检查点导入：四键无条件替换；五键 op,base,version,digest,
            # state 为乐观并发形式。parse_op 已校键序、version=1、digest/base
            # 格式。两种形式都先按规范化紧凑编码校验候选大小与摘要（超限
            # OVERLOAD/7、不符 INPUT/2），再完整解析语义（悬空引用/重复标
            # 识/矛盾计数/非法组合 STATE/4）并做规范化往返。五键形式在此之
            # 后再按 se 的规范化规则取得执行到本操作时的当前状态及摘要：当
            # 前摘要等于候选 digest 时视为成功的幂等重报（即使 base 已不等于
            # 当前摘要也不再替换）；否则仅当 base 等于当前摘要才原子导入，
            # base 不匹配报 STATE/4。当前状态紧凑编码超 se 的 8MiB 上限报
            # OVERLOAD/7。不推进显式时钟；任何失败均无 stdout、整批状态回
            # 滚（候选束为局部对象，未安装前不触碰闭包运行态）。
            _, base, version, digest, raw_state = op
            # 规范化摘要仅认固定键序的 {version,state} 紧凑编码：先把所给
            # state 紧凑编码（所给 state 必须本身即规范化形态；任何键序、
            # 类型或取值偏差都会令摘要不符判 INPUT）。
            try:
                state_bytes = json.dumps(
                    raw_state, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                envelope_bytes = json.dumps(
                    {"version": version, "state": raw_state},
                    ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(state_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            if hashlib.sha256(envelope_bytes).hexdigest() != digest:
                fail(EXIT_INPUT, "INPUT")
            # 摘要相符后做完整结构与语义校验，构造候选束；不触碰任何当前
            # 闭包状态，故失败天然回滚。
            candidate = parse_checkpoint(raw_state)
            # 规范化往返：候选束重新规范化后的紧凑编码必须与所给 state 的
            # 紧凑编码逐字节一致。这强制所给 state 本身即为规范化形态（逐层
            # 键序、数组排序、取值格式全部固定），从而恢复后 se 复现完全相同
            # 的 version/digest/state；任何键序或排序偏差在此判 STATE。
            re_state = export_bundle(candidate)
            re_bytes = json.dumps(
                re_state, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if re_bytes != state_bytes:
                fail(EXIT_STATE, "STATE")
            if base is not None:
                # 乐观并发形式：按 se 的规范化规则取得当前状态及摘要。
                current_state = export_bundle(current_bundle())
                try:
                    current_bytes = json.dumps(
                        current_state, ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    current_envelope = json.dumps(
                        {"version": CHECKPOINT_VERSION, "state": current_state},
                        ensure_ascii=False, separators=(",", ":"),
                    ).encode("utf-8")
                except UnicodeEncodeError:
                    fail(EXIT_INPUT, "INPUT")
                if len(current_bytes) > CHECKPOINT_LIMIT:
                    fail(EXIT_OVERLOAD, "OVERLOAD")
                current_digest = hashlib.sha256(
                    current_envelope
                ).hexdigest()
                if current_digest == digest:
                    # 成功的幂等重报：当前即候选状态，即使 base 已过期（base
                    # != 当前摘要）也不再次替换、不报冲突。
                    pass
                elif base != current_digest:
                    # 预览后运行态已变化且非同一检查点重报：拒绝覆盖。
                    fail(EXIT_STATE, "STATE")
                else:
                    install_bundle(candidate)
            else:
                install_bundle(candidate)
            results.append(
                {"op": "si", "digest": digest, "ok": True}
            )

        elif op[0] == "sx":
            # 三方检查点只读预演：以当前运行态（current）、基线检查点
            # （base）与候选检查点（target）区分并发变化，供调用方在执行 si
            # 前判断候选是否仍可安全使用。不合并/导入、不推进显式时钟、不
            # 改任何运行态或幂等缓存（候选束均为局部对象）。
            #
            # 校验顺序严格为 base、target、current：每份检查点各自沿用 si
            # 的规则与错误优先级（形状/键序/version/digest 格式 INPUT；紧
            # 凑编码 8MiB OVERLOAD；摘要不符 INPUT 先于语义；结构/类型/范
            # 围/UTF-8 INPUT；悬空引用/重复标识/矛盾计数/非法组合及规范化
            # 往返偏差 STATE）；两份检查点全部通过后再按 se 规则取当前规范
            # 化快照，当前编码超限报 OVERLOAD/7。任何失败均无 stdout 并回
            # 滚整批（本操作本不改状态）。
            _, raw_base, raw_target = op

            base_digest, base_state, _, _ = validate_checkpoint_object(
                raw_base)
            target_digest, target_state, target_bytes, _ = (
                validate_checkpoint_object(raw_target))

            # 两份检查点通过后按 se 规则取得当前规范化内存快照。
            current_state = export_bundle(current_bundle())
            try:
                current_bytes = json.dumps(
                    current_state, ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                current_envelope = json.dumps(
                    {"version": CHECKPOINT_VERSION, "state": current_state},
                    ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(current_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            current_digest = hashlib.sha256(current_envelope).hexdigest()

            # changes：按 state 顶层键序列出三份状态不全相同的段；三份完全
            # 相同时为空。即使当前与候选整体相同（SAME），基线独异的段仍以
            # kind=SAME 列出。
            changes = []
            conflict_sections = set()
            for section in current_state:
                cur_value = current_state[section]
                base_value = base_state[section]
                tgt_value = target_state[section]
                if cur_value == base_value:
                    if base_value == tgt_value:
                        # 三份完全相同：不列。
                        continue
                    # 当前等于基线而候选不同：候选单方面变更。
                    kind = "TARGET"
                elif tgt_value == base_value:
                    # 候选等于基线而当前不同：当前相对基线变化。
                    kind = "CURRENT"
                elif cur_value == tgt_value:
                    # 当前等于候选而基线不同。
                    kind = "SAME"
                else:
                    kind = "CONFLICT"
                    conflict_sections.add(section)
                changes.append(
                    {
                        "section": section,
                        "base": section_digest(base_value),
                        "current": section_digest(cur_value),
                        "target": section_digest(tgt_value),
                        "kind": kind,
                    }
                )

            # status：当前与候选规范化 state 逐字节相同为 SAME；否则有冲突
            # 段为 CONFLICT；其余为 CLEAN。
            if current_bytes == target_bytes:
                status = "SAME"
            elif conflict_sections:
                status = "CONFLICT"
            else:
                status = "CLEAN"

            # 段分类计数（仅统计三份不全相同的段，即 changes）；四类之和等
            # 于 changed。
            kind_counts = {"TARGET": 0, "CURRENT": 0, "SAME": 0,
                           "CONFLICT": 0}
            for change in changes:
                kind_counts[change["kind"]] += 1

            results.append(
                {
                    "op": "sx",
                    "current": current_digest,
                    "base": base_digest,
                    "target": target_digest,
                    "status": status,
                    "changes": changes,
                    "summary": {
                        "sections": len(current_state),
                        "changed": len(changes),
                        "target": kind_counts["TARGET"],
                        "current": kind_counts["CURRENT"],
                        "same": kind_counts["SAME"],
                        "conflict": kind_counts["CONFLICT"],
                    },
                }
            )

        elif op[0] == "sm":
            # 检查点三方合并：调用方提交基线检查点 base 与目标检查点
            # target，把 target 相对 base 的无冲突变化原子合入执行到本操作
            # 时的当前运行态。不接受 now，也不推进显式时钟。
            #
            # 校验顺序严格为 base、target、current（同 sx）：每份检查点沿用
            # si 的规则与错误优先级，两份全部通过后再按 se 规则取当前规范
            # 化快照，当前编码超限报 OVERLOAD/7。
            #
            # 合并以规范化 state 顶层段为最小单位，按固定键序逐段处理：
            # target==base 保留 current；target!=base 且 current 为 base 或
            # target 时采用 target；其余为冲突。存在冲突即拒绝整次合并、不
            # 安装任何段（STATE/4）。无冲突时把组合结果作为完整检查点再次
            # 校验（8MiB OVERLOAD/7；悬空引用/重复标识/矛盾计数/跨段非法组
            # 合及规范化往返偏差 STATE/4），全部通过后才原子安装。目标变
            # 化已存在（current==target）或目标未改变（target==base）时幂
            # 等成功：after==当前摘要、changed=0、sections=[]。任何失败均
            # 无 stdout；合并束安装前为局部对象，故失败天然不触碰闭包运行
            # 态，整批此前变化随 run 失败一并回滚。
            _, raw_base, raw_target = op

            _, base_state, _, _ = validate_checkpoint_object(raw_base)
            _, target_state, _, _ = validate_checkpoint_object(raw_target)

            # 两份检查点通过后按 se 规则取得当前规范化内存快照。
            before_state = export_bundle(current_bundle())
            try:
                before_bytes = json.dumps(
                    before_state, ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                before_envelope = json.dumps(
                    {"version": CHECKPOINT_VERSION, "state": before_state},
                    ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(before_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            before_digest = hashlib.sha256(before_envelope).hexdigest()

            # 逐段三方合并，段序沿用 se 的固定顶层键序；三份均为同构规范
            # 化 state，顶层键集与键序一致。
            merged_state = {}
            changed_sections = []
            for section in before_state:
                cur_value = before_state[section]
                base_value = base_state[section]
                tgt_value = target_state[section]
                if tgt_value == base_value:
                    # 目标未改变该段：保留 current（current 自身相对基线的
                    # 变化由此保留）。
                    merged_value = cur_value
                elif cur_value == base_value or cur_value == tgt_value:
                    # 仅目标变化或当前已含目标变化：采用 target；后者合并
                    # 后取值与 current 相同，不计入 changed。
                    merged_value = tgt_value
                else:
                    # 三方两两不一致：冲突，拒绝整次合并，不安装任何段。
                    fail(EXIT_STATE, "STATE")
                merged_state[section] = merged_value
                if merged_value != cur_value:
                    changed_sections.append(section)

            # 组合结果作为完整检查点校验：先紧凑编码与 8MiB 上限，再完整
            # 结构/语义解析与规范化往返。
            try:
                merged_bytes = json.dumps(
                    merged_state, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
            except UnicodeEncodeError:
                fail(EXIT_INPUT, "INPUT")
            if len(merged_bytes) > CHECKPOINT_LIMIT:
                fail(EXIT_OVERLOAD, "OVERLOAD")
            merged_bundle = parse_checkpoint(merged_state)
            re_state = export_bundle(merged_bundle)
            re_bytes = json.dumps(
                re_state, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if re_bytes != merged_bytes:
                fail(EXIT_STATE, "STATE")

            # 全部校验通过：原子安装；安装后按 se 规则取得 after 摘要。
            install_bundle(merged_bundle)
            after_state = export_bundle(current_bundle())
            after_digest = hashlib.sha256(
                json.dumps(
                    {"version": CHECKPOINT_VERSION, "state": after_state},
                    ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()

            results.append(
                {
                    "op": "sm",
                    "before": before_digest,
                    "after": after_digest,
                    "changed": len(changed_sections),
                    "sections": changed_sections,
                    "ok": True,
                }
            )

        else:  # get
            _, cid = op
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            results.append(
                {
                    "op": "get",
                    "cid": cid,
                    "flow": connection[1],
                    "backend": connection[0],
                    "opened_at": connection[2],
                }
            )

    output = {
        "results": results,
        "backends": [
            {"id": backend_id, "weight": record["weight"]}
            for backend_id, record in backends.items()
        ],
    }
    encoded = json.dumps(output, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return encoded + b"\n"


def execute(raw):
    """在全新状态执行 raw，返回 (退出码, stdout 字节, stderr 字节)。"""
    try:
        return 0, run(raw), b""
    except _Failure as error:
        return (
            error.exit_code,
            b"",
            ('{"error":"%s"}\n' % error.label).encode("utf-8"),
        )


def build_record(raw):
    """把原始 stdin 字节作为全新 run 输入执行，返回记录行（紧凑 JSON + 换行）。

    键序 version,stdin,exit,stdout,stderr；三个字节字段为带标准填充的
    RFC4648 Base64。底层成功或按既有错误失败均产出记录。
    """
    exit_code, out, err = execute(raw)
    record = {
        "version": 1,
        "stdin": base64.b64encode(raw).decode("ascii"),
        "exit": exit_code,
        "stdout": base64.b64encode(out).decode("ascii"),
        "stderr": base64.b64encode(err).decode("ascii"),
    }
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    return line.encode("utf-8") + b"\n"


def decode_base64_field(value):
    """校验并解码带标准填充的规范 Base64；类型、解码或规范性不符判 INPUT。"""
    if not isinstance(value, str):
        fail(EXIT_INPUT, "INPUT")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        fail(EXIT_INPUT, "INPUT")
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except ValueError:
        fail(EXIT_INPUT, "INPUT")
    # 重编码必须逐字节一致：拒绝非零填充位、缺/多填充等非规范形式。
    if base64.b64encode(decoded) != encoded:
        fail(EXIT_INPUT, "INPUT")
    return decoded


def replay(raw):
    """校验记录并在全新状态重执解码的 stdin，逐字节比对退出码与两路输出。

    一致时返回 (记录退出码, 记录 stdout, 记录 stderr)；记录非法判 INPUT，
    比对不符判 REPLAY/8。
    """
    try:
        text = raw.decode("utf-8")
        data = decode_json(text)
    except (UnicodeDecodeError, ValueError):
        fail(EXIT_INPUT, "INPUT")
    if not isinstance(data, dict) or set(data) != {
        "version", "stdin", "exit", "stdout", "stderr",
    }:
        fail(EXIT_INPUT, "INPUT")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) or version != 1:
        fail(EXIT_INPUT, "INPUT")
    exit_code = data["exit"]
    if (
        not isinstance(exit_code, int)
        or isinstance(exit_code, bool)
        or exit_code not in (0, 2, 3, 4, 5, 6, 7)
    ):
        fail(EXIT_INPUT, "INPUT")
    stdin_bytes = decode_base64_field(data["stdin"])
    stdout_bytes = decode_base64_field(data["stdout"])
    stderr_bytes = decode_base64_field(data["stderr"])
    actual_code, actual_out, actual_err = execute(stdin_bytes)
    if (
        actual_code != exit_code
        or actual_out != stdout_bytes
        or actual_err != stderr_bytes
    ):
        fail(EXIT_REPLAY, "REPLAY")
    return exit_code, stdout_bytes, stderr_bytes


def main(argv):
    try:
        if len(argv) != 2 or argv[1] not in ("run", "record", "replay"):
            fail(EXIT_INPUT, "INPUT")
        raw = sys.stdin.buffer.read()
        if argv[1] == "run":
            exit_code, out, err = execute(raw)
        elif argv[1] == "record":
            # 底层成败均退出 0、stderr 为空，stdout 输出记录。
            exit_code, out, err = 0, build_record(raw), b""
        else:
            exit_code, out, err = replay(raw)
    except _Failure as error:
        exit_code = error.exit_code
        out = b""
        err = ('{"error":"%s"}\n' % error.label).encode("utf-8")
    sys.stdout.buffer.write(out)
    sys.stderr.buffer.write(err)
    sys.exit(exit_code)


if __name__ == "__main__":
    main(sys.argv)
