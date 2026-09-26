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
加对应成本），否则 RATE/6。remove 同步删除其 B 配额，ci/cb 成功清空
全部配额，ce 导出不变；非法键集、类型、范围、编码或时钟倒退报
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
倒退报 INPUT。

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

后端采样历史：ms 键集 op,id,now（now 为 [0,10^9] 非 bool 整数，纳入共用
非递减时钟），id 须现存否则 BACKEND。每次采样记录该后端当时的活动连接数
与 removed——removed 沿用 mg 的取值与优先级（drain、health、circuit、
fault 或 null）。按 window=now//60 分窗保留最近 60 窗，每窗至多 60 个
不同 now（每后端至多 3600 样本）。同 (id, now) 重报：采样值（连接数与
removed）一致幂等，不一致属冲突重报报 STATE/4。ms 返回键序 op,ok，
ok=true。mx 精确键集 op,id,from,to,now，数值与关系约束同 mh（非法键集、
类型、范围、关系或时钟倒退报 INPUT/2，未知 id 报 BACKEND/3，from 早于
max(0,now//60-59) 报 STATE/4）。结果键序 op,id,windows，windows 含 from
至 to 所有窗并按窗升序，项键序 window,samples,peak,last,removed：
samples 为窗内采样数、peak 为窗内并发峰值、last 为末次采样的并发；空窗
samples/peak 为 0 且 last=null；removed 为键序 drain,health,circuit,
fault,none 的非负整数计数对象，removed=null 的样本计入 none。mx 不改变
采样，失败原子回滚；record/replay 逐字节覆盖。remove 后重加、ci 成功均
清空采样历史。ms 为 O(1)，mx 为 O(R+S)（R 为窗数、S 为区间内样本数），
仅用标准库，其余子命令与既有操作行为不变。

不可用原因分钟历史：按 window=now//60 记账，各后端只保留最近 60 窗，
空窗不预建。probe 使 healthy 转 unhealthy 时该后端 health 加 1；dr 使 A
转 D/X 时 drain 加 1；cr 使 C/H 转 O 时 circuit 加 1；oa 返回 Q 且所选
后端 conns≥os.cap 时该后端 overload 加 1（同次过载与其它原因重叠只记
overload 一次）。重报（probe/cr 幂等重报、D/X 再 dr）与无状态转换一律
不记；oa 报 OVERLOAD/7（P 态背压或队满尾拒绝）回滚整批且不记。各计数
封顶 10^18。rh 精确键序 op,id,from,to,now；from/to/now 为 0..10^9 非
bool 整数，now 进入共用非递减时钟，须 from≤to≤now//60 且 to-from<60。
返回键序 op,id,windows；windows 含 from 至 to 所有窗并按窗升序，项键序
window,health,drain,circuit,overload，值为非负整数，空窗全 0。键集、
类型、范围、关系或时钟倒退报 INPUT/2，未知 id 报 BACKEND/3，from 早于
max(0,now//60-59) 报 STATE/4。rh 只读，失败批次天然回滚；remove 后重加
与 ci 成功清空历史。记账 O(1)，rh 时间 O(R)（R 为窗数）、空间 O(60B)，
record/replay 逐字节覆盖；其他子命令行为不变。

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
scheduler,faults}：
version=7；backends 按加入序，项 {id,weight,d,fail,success,circuit,drain,
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
段 a 升序输出，空计划为 []，不含 effect 或运行态。ci 精确键集
op,config,now，结果 op,ok=true；now 为非负非 bool 整数并纳入共用非递减
时钟，亦接受 version=1 原结构（仅前五键）与 version=2 结构（追加三键），
两者 scheduler 缺省等价于 W；version=3 同为九键但 scheduler 仅收 W/R，
version=4 须含 scheduler 并收 W/R/L，version=5 收 W/R/L/H 且选 H 时
vnodes 须非 null；version=6 同 v5，且 backends 项须在既有七键后含
endpoint（v1..v5 不含该键，一律视为 null）；version=7 在既有九键末追加
faults 且须精确含该键（v1..v6 一律视 faults=[]），backends 项同 v6。
各值沿用 add/hset/ws/chash/cs/ds/ls/os/ss/ts/bp 与 fs/fp 的类型与范围。
scheduler
缺失（v1/v2）合法，v3 多键、类型错误或 pick 非 W/R，v4 的 pick 非 W/R/L，
v5/v6/v7 的 pick 非 W/R/L/H 或选 H 而 vnodes 为 null，
连同其余非法结构、键集、版本、类型、范围、重复后端/限流项/故障段、编码、
交叉约束（含同 id 段重叠）或时钟倒退判
INPUT/2，B 限流或 faults 引用未知后端判 BACKEND/3，有活动连接或排队项
判 STATE/4，依次判错。成功时原子替换配置并以 now 重建默认运行态（全部
healthy、d>0
自 now 起算预热、熔断 C 空窗、排空 A、桶满、队空、粘性清空、度量归零、
平滑 current 与轮询 ticket=0），并按 faults 载入各后端故障时间线
（fq/fx/fr/fi 立即按其生效；统计、分钟历史与恢复基线等运行态仍重置）：
sticky/idle 以登记值作用于新连接（idle
为新连接的空闲
时限，无连接故仅登记），backpressure 携带时置 N、未携带时取消；失败回滚
不变更。R 模式 pick 按既有健康、熔断闭合、排空 A 条件取得按加入序排列
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
均 O(B+M+T) 时空（M 为限流项数、T 为故障段数）；R/L 的 pick 均为 O(B)
时间、O(1) 额外
空间，H 的 pick 为 O(BV log(BV)) 时间、O(BV+S) 空间。

配置提交与回滚：ci 成功后把规范化 version=7 配置存为提交，rev 从 1 起
递增，仅保留最近 16 条；失败不分配、不改历史，初始无提交。cl 精确键集
仅 op，返回键序 op,current,commits：current 为最新 rev 或 null，
commits 按 rev 升序，项键序 rev,config，config 复用 ce 的逐层键序与
值格式（含 faults）。cb 精确键集 op,rev,now：rev 为 1..10^18 非 bool
整数且须仍被保留，now 沿用 ci 并进入共用非递减时钟；按目标快照执行 ci
的原子替换与默认运行态重建（恢复目标 faults 时间线并重置故障运行态），
成功另建新 rev，返回键序 op,target,rev,ok（ok=true），
原历史保留后再按 16 条淘汰。目标不存在或 rev 耗尽（下一个 rev 将超过
10^18）报 STATE/4；键集、rev 类型/范围或时钟非法报 INPUT/2；有活动
连接或排队项报 STATE/4。失败回滚时钟、配置、运行态、rev 与历史。
record/replay 逐字节覆盖；cl 与 cb 的额外时空上界 O(16(B+M+T))；其余
子命令与既有操作行为不变。

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
tk/tg/tx 报 STATE。close 与 dg 强关同样清理连接的空闲状态。ts/tk/tg 为
O(1)，tx 为 O(C)，额外空间 O(C)。

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
删除快照。ce 导出 version=7（在既有九键末追加 faults 数组，空计划 []），
backends 项保持在既有七键后追加 endpoint（null 或键序 host,port）；ci
兼容 version1..5 并视 endpoint=null，version6 须含 endpoint 但结构无
faults，version7 在九键后追加 faults 且项按 fp 同款校验，成功原子重建并
载入目标时间线、失败回滚。ep/fw
为 O(1)，额外空间 O(B+C)，仅用标准库；其余子命令与既有操作行为不变。
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
    v1..v6 一律视 faults 为空。"""
    if not isinstance(value, dict):
        fail(EXIT_INPUT, "INPUT")
    config_keys = set(value)
    v1_keys = {"version", "backends", "vnodes", "limits", "overload"}
    v2_keys = v1_keys | {"sticky", "idle", "backpressure"}
    v3_keys = v2_keys | {"scheduler"}
    # v7 在既有九键末追加 faults；十键结构只可能为 v7。
    v7_keys = v3_keys | {"faults"}
    if config_keys == v1_keys:
        version = 1
    elif config_keys == v2_keys:
        version = 2
    elif config_keys == v3_keys:
        # 九键结构为 v3/v4/v5/v6 共用，具体版本由 version 字段区分。
        version = None
    elif config_keys == v7_keys:
        version = 7
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
        if version in (6, 7):
            # v6/v7 项在既有七键后追加 endpoint；v1..v5 项精确为七键。
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
        if version in (6, 7):
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
        if scope not in scope_rank:
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
        # 字符串均报 INPUT；v3 仅收 W/R，v4 收 W/R/L，v5/v6/v7 收
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
        # v5/v6/v7 选 H 时 vnodes 须非 null（一致性哈希环必须已配置）。
        if scheduler == "H" and vnodes is None:
            fail(EXIT_INPUT, "INPUT")
    else:
        # v1/v2 旧结构等价于既有平滑加权 W。
        scheduler = "W"

    # faults：仅 version=7 的结构含该键且精确为数组；v1..v6 一律视为空
    # 计划（不校验段，不载入时间线）。
    faults_plan = parse_config_faults(value["faults"]) if version == 7 else {}

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
    }


def parse_op(raw_op):
    """校验单个操作的形状，返回规范化元组；不合格式直接 INPUT 退出。"""
    if not isinstance(raw_op, dict):
        fail(EXIT_INPUT, "INPUT")

    name = raw_op.get("op")
    if name not in (
        "add", "remove", "pick", "open", "close", "get",
        "hset", "probe", "hget", "chash", "route", "ws", "wg",
        "cs", "cr", "cg", "ds", "dr", "du", "dg",
        "ss",
        "ls", "la", "lg", "qs", "qg",
        "os", "oa", "ot", "og", "oc", "oh", "bp", "bq",
        "mr", "mg", "mh", "ms", "mx", "rh", "ra", "ma", "mo",
        "ce", "ci", "cl", "cb",
        "fs", "fx", "fr", "fi", "oi", "od",
        "fb", "fp", "fq",
        "hm", "fm", "fh",
        "fa", "fe", "ah",
        "ts", "tk", "tg", "tx",
        "ep", "fw",
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
        # 故障时间线原子替换：精确键序 op,items（键须按此序出现）；items 为
        # 0..4096 项数组（bool 不是数组），项精确键序 id,k,a,z,v（键须按此
        # 序出现），字段约束沿用 fs；同一 id 多段须按 a 升序且 [a,z) 互不
        # 重叠（相邻端点可接）。键序/容器/项数/字段/重叠判 INPUT，未知 id
        # 留执行期判 BACKEND。
        if list(raw_op) != ["op", "items"]:
            fail(EXIT_INPUT, "INPUT")
        raw_items = raw_op["items"]
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
            # 规范化：同 id 段先按 a 升序排序，再校验半开区间互不重叠
            # （z_i<=a_{i+1}，相等为相邻可接）。乱序但可排成不重叠序列的
            # 计划合法；同 a 或任何相交在此被拒。
            segments.sort(key=lambda segment: segment[1])
            for idx in range(len(segments) - 1):
                if segments[idx][2] > segments[idx + 1][1]:
                    fail(EXIT_INPUT, "INPUT")
            plan[item_id] = segments
        return ("fp", plan)

    if name == "fq":
        if keys != {"op", "now"}:
            fail(EXIT_INPUT, "INPUT")
        return ("fq", parse_now(raw_op["now"]))

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

    if name in ("ce", "ci"):
        if name == "ce":
            if keys != {"op"}:
                fail(EXIT_INPUT, "INPUT")
            return ("ce",)
        if keys != {"op", "config", "now"}:
            fail(EXIT_INPUT, "INPUT")
        # now 为非负非 bool 整数，时钟倒退在执行期与其余操作同序判定。
        parse_now(raw_op["now"])
        config = parse_config(raw_op["config"])
        return ("ci", config, raw_op["now"])

    if name == "cl":
        # 配置提交历史查询：精确键集仅 op，只读。
        if keys != {"op"}:
            fail(EXIT_INPUT, "INPUT")
        return ("cl",)

    if name == "cb":
        # 配置回滚：精确键集 op,rev,now；rev 为 1..10^18 非 bool 整数
        # （是否仍被保留留执行期判 STATE），now 沿用 ci 并进入共用非递减
        # 时钟（倒退在执行期与其余操作同序判 INPUT）。
        if keys != {"op", "rev", "now"}:
            fail(EXIT_INPUT, "INPUT")
        rev = raw_op["rev"]
        if (
            not isinstance(rev, int)
            or isinstance(rev, bool)
            or not 1 <= rev <= 10 ** 18
        ):
            fail(EXIT_INPUT, "INPUT")
        return ("cb", rev, parse_now(raw_op["now"]))

    if name == "ts":
        if keys != {"op", "ttl"}:
            fail(EXIT_INPUT, "INPUT")
        return ("ts", parse_idle_ttl(raw_op["ttl"]))

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

    # get
    if keys != {"op", "cid"}:
        fail(EXIT_INPUT, "INPUT")
    return ("get", parse_cid(raw_op["cid"]))


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
    # 过载分钟历史（oh）：window=now//60 -> [immediate, queued, dequeued,
    # expired, peak]，全池一份（不按后端分）。oa 返回 A/Q 在其 now 窗记
    # immediate/queued，ot 的接纳/过期项在其 now 窗记 dequeued/expired，
    # peak 为该窗入队后队长峰值；计数封顶 10^18，只保留最近 60 窗，空窗
    # 不预建。ci/cb 成功清空；oh 只读。
    overload_hist = {}
    # 确定性滞回背压：bp_cfg 未 bp 时为 None，否则为 (low, high)；bp_state
    # 为 N/P。首配或异参重配按当前队长 >=high 置 P，否则 N；同参幂等不改
    # 状态。low < high <= queue_cfg[1]（os.q）；登记值随 ce/ci 导出导入，
    # ci 携带时置 N、未携带时取消。
    bp_cfg = None
    bp_state = "N"
    # 连接空闲超时：ttl_cfg 未 ts 时为 None，否则为登记的全局空闲时限；
    # 异值重配报 STATE，登记值随 ce/ci 导出导入（ci 后作用于新连接）。
    ttl_cfg = None
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
    # 配置提交历史（cl/cb）：(rev, 规范化 version=7 配置快照) 按 rev 升序，
    # 仅保留最近 16 条；rev 由 next_rev 从 1 起递增分配，只增不复用。ci/cb
    # 成功才分配并追加，失败不分配、不改历史；初始无提交。快照为
    # export_config 产出的全新结构（含 faults 登记时间线），不随后续运行态
    # 变化。额外时空 O(16(B+M+T))。
    commit_history = []
    next_rev = 1
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

    def evaluate_admit(backend_id, cid, flow, c, s, costs, now):
        """对已路由的后端按 la 规则补充检查但不消费：先补充在配桶、推进在配
        固定窗（均不回写扣减），令牌不足、配额 used+成本>limit、目标非 A 或
        连接数达 cap 时返回 ("block", id)，全部满足才按成本原子耗令牌、配额
        used 加成本并建连接（opened_at=now），返回 ("admit", id)。costs 为
        (bc, cc, sc)。配额不足只导致排队阻塞，从不由 oa/ot 报 RATE。"""
        chosen = []
        chosen_quotas = []
        for scope, bucket_id, cost in (
            ("B", backend_id, costs[0]),
            ("C", c, costs[1]),
            ("S", s, costs[2]),
        ):
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
        if (
            not all(bucket["t"] >= cost for bucket, cost in chosen)
            or not all(
                quota["used"] + cost <= quota["limit"]
                for quota, cost in chosen_quotas
            )
            or record["drain"]["state"] != "A"
            or record["conns"] >= queue_cfg[0]
        ):
            return "block", backend_id
        # 接纳才按成本耗令牌、增配额 used 并按 open 建连接；全部满足后统一
        # 扣减，天然原子（其间无 fail 路径）。
        for bucket, cost in chosen:
            bucket["t"] -= cost
        for quota, cost in chosen_quotas:
            quota["used"] += cost
        establish_connection(cid, backend_id, flow, now)
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

    def record_reason(backend_id, reason, now):
        """不可用原因分钟历史记账（O(1)）：reason ∈ health/drain/circuit/
        overload，归属后端 backend_id 的 window=now//60 窗，对应计数加 1 并
        封顶 10^18。仅在确有状态转换（probe h→u、dr A→D/X、cr C/H→O）或
        oa 因连接达 cap 入队 Q 时由调用方调用；重报、无转换与 oa 的
        OVERLOAD 回滚均不调用。每后端只保留最近 60 窗，空窗不预建。"""
        history = backends[backend_id]["reason_hist"]
        window = now // 60
        counts = history.get(window)
        if counts is None:
            # 首次记账该窗：新建四项计数；时钟非递减，顺带丢弃 60 窗前旧窗。
            counts = {"health": 0, "drain": 0, "circuit": 0, "overload": 0}
            history[window] = counts
            cutoff = window - 59
            for old in [w for w in history if w < cutoff]:
                del history[old]
        counts[reason] = min(METRIC_CAP, counts[reason] + 1)

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

    def active_fault(record, now):
        """按 now 在故障时间线中取唯一活动段：段按 a 升序且 [a,z) 互不
        重叠，故至多一段满足 a<=now<z（O(log T_b)，fault_a 为与 faults
        平行的 a 列表，随替换原子更新）；未登记或处于段间隙时返回 None。"""
        a_values = record["fault_a"]
        if not a_values:
            return None
        idx = bisect.bisect_right(a_values, now) - 1
        if idx < 0:
            return None
        segment = record["faults"][idx]
        if now < segment[2]:
            return segment
        return None

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

    def simulate_fr(tokens, digests, key, timeout, max_attempts, now):
        """只读模拟一次 fr 的哈希遍历与 D/F/S 规则，返回
        (state, backend, attempts, latency)：自 key 哈希点按 fx 顺序遍历
        不同后端至多 max_attempts 个，D/故障相位 F 失败耗时 0、S 的
        v>timeout 失败耗时 timeout，否则成功耗时 v 或 0 并终止；不建连、
        不记度量与故障统计。tokens 非空（空环由调用方先报 STATE）。"""
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
            record = backends[backend_id]
            segment = active_fault(record, now)
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
            if record["conns"] >= queue_cfg[0]:
                # 连接数达 os 的 cap：capacity 失败。
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
            if record["conns"] >= queue_cfg[0]:
                # 连接数达 os 的 cap：capacity 失败。
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
        固定（version=7；backends 按加入序，项 id,weight,d,fail,success,
        circuit,drain,endpoint；limits 按 scope 的 B/C/S 序、id 的 UTF-8
        字节升序；overload/sticky/idle/backpressure 为 null 或登记值；
        scheduler 精确为 {"pick":...}；faults 末置，按后端加入序、段 a
        升序，项键序 id,k,a,z,v，空计划为 []，不含 effect 或运行态）。
        返回全新结构，调用方可安全存为快照（不随后续运行态变化）。"""
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
        return {
            "version": 7,
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
        }

    def apply_config(config, now):
        """ci/cb 共用的原子替换：以 now 重建默认运行态（全部 healthy、d>0
        自 now 起算预热、熔断 C 空窗、排空 A、桶满、配额清空、队空、粘性清空、
        度量归零、平滑 current 与轮询 ticket=0；sticky/idle 取登记值作用于新
        连接，backpressure 携带时置 N、未携带时取消；fe/ah 告警状态与历史
        清除；故障统计、分钟历史与恢复基线重置），并按 v7 faults 载入各后端
        登记时间线（v1..v6 为空计划）。调用方须已完成全部校验，本函数自身
        不再失败。"""
        nonlocal backends, buckets, quotas, ring_vnodes, queue_cfg, wait_queue
        nonlocal sticky_ttl, ttl_cfg, bp_cfg, bp_state, pick_mode, rr_ticket
        nonlocal sticky_map, alert, alert_events, overload_hist
        nonlocal mo_seq, mo_cache

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
        # ci/cb 成功清空全部固定窗口配额（ce 不导出配额，配置不携带）。
        quotas = {}
        ring_vnodes = config["vnodes"]
        queue_cfg = config["overload"]
        wait_queue = OrderedDict()
        # ci/cb 成功清空过载分钟历史。
        overload_hist = {}
        # 热加载三项：sticky/idle 取登记值（null 即未登记，idle 作用于
        # 此后新建连接）；backpressure 携带时置 N，未携带（含 v1）即取消。
        sticky_ttl = config["sticky"]
        ttl_cfg = config["idle"]
        bp_cfg = config["backpressure"]
        bp_state = "N"
        # 调度策略随配置原子替换（v1/v2 已规范化为 W），轮询游标复位。
        pick_mode = config["scheduler"]
        rr_ticket = 0
        sticky_map = {}
        # ci 成功清除全池故障告警状态（fe 回到未首评）与转换历史。
        alert = None
        alert_events = deque()
        # ci/cb 成功清 mo 游标与缓存、seq 重置为 1（各后端基线随新记录
        # 清零）；失败时调用方根本不会进入本函数，天然回滚。
        mo_seq = 1
        mo_cache = None

    for raw_op in ops:
        op = parse_op(raw_op)

        if op[0] in (
            "open", "close", "probe", "add", "ws", "wg", "cr", "cg",
            "dr", "du", "dg", "ls", "la", "lg", "qs", "qg", "oa", "ot",
            "mr", "mg", "mh",
            "ms", "mx", "rh", "ra", "ma",
            "ci", "cb", "fx", "fr", "fi", "oi", "od", "tk", "tg", "tx", "route", "fq", "pick", "fh",
            "fa", "fe", "ah", "oh",
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
                # 后端采样历史（ms/mx）：window -> {now: (活动连接数,
                # removed)}，内层按采样先后（now 升序）保序；按 now//60
                # 仅保留最近 60 窗，每窗至多 60 个不同 now。remove 后重加、
                # ci 成功即清空。
                "samples": {},
                # 不可用原因分钟历史（rh）：window -> 固定键序
                # health,drain,circuit,overload 的计数对象，仅保留最近 60
                # 窗，空窗不预建；各计数封顶 10^18，仅在状态转换或 oa 因
                # 连接达 cap 入队时记账。remove 后重加与 ci 成功即清空。
                "reason_hist": {},
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
            else:  # H
                if ok:
                    circuit["used"] += 1
                    if circuit["used"] >= q:
                        # 连续 q 次成功转 C 并清窗。
                        circuit["state"] = "C"
                        circuit["window"].clear()
                        circuit["used"] = 0
                else:
                    # H 中失败立即重开并重算恢复时刻。
                    circuit["state"] = "O"
                    circuit["next"] = now + w
                    circuit["used"] = 0
                    # 不可用原因历史：H→O 的重开同样记 circuit。
                    record_reason(backend_id, "circuit", now)
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
                results.append(
                    {"op": "oa", "cid": cid, "state": "A", "backend": backend_id}
                )
            else:
                if bp_cfg is not None and bp_state == "P":
                    # P 态本应排队即报 OVERLOAD：不耗令牌、不入队、无其他变更。
                    fail(EXIT_OVERLOAD, "OVERLOAD")
                if len(wait_queue) >= queue_cfg[1]:
                    # FIFO 已满，尾拒绝。
                    fail(EXIT_OVERLOAD, "OVERLOAD")
                # 不可用原因历史：Q 入队且所选后端连接数已达 os.cap 时记
                # overload（所选后端来自只含 healthy/C/A 的环，阻塞在此只可能
                # 因令牌不足、固定窗配额不足或连接达 cap；配额不足只入队，
                # 不报 RATE）；P 态/队满的 OVERLOAD 已在上方回滚，不记。同次
                # 至多记一次。
                if backends[routed[0]]["conns"] >= queue_cfg[0]:
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
            for queued_cid, item in list(wait_queue.items()):
                if now >= item[8] + ttl:
                    expired.append(queued_cid)
                    wait_queue.pop(queued_cid)
            # 再自队首重试接纳，至首个阻塞即停（每个键至多一次 route）。
            # 逐项以本次 ot 的 now 补充桶并按 window=now//span 推进固定窗，
            # 接纳才扣令牌、增 used 并建连；前序接纳的扣减对后续项可见，首个
            # 阻塞项连同其（已推进但未扣减的）桶/配额状态保留在队内。
            admitted = []
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
            wait_queue.pop(cid)
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
                # 3600 样本）。
                snapshot = {}
                history[window] = snapshot
                cutoff = window - 59
                for old in [w for w in history if w < cutoff]:
                    del history[old]
            # 采样值为当时的 (活动连接数, removed)，removed 沿用 mg 优先级。
            current = (record["conns"], removed_reason(record, now))
            previous = snapshot.get(now)
            if previous is not None:
                # 同 (id, now)：采样值相同幂等，不同即冲突重报，报 STATE。
                if previous != current:
                    fail(EXIT_STATE, "STATE")
            else:
                snapshot[now] = current
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
                peak = 0
                last = 0
                # 内层 dict 按采样先后（共用时钟非递减）保序，末次并发即
                # 末项；遍历同时求窗内峰值与 removed 分类计数。
                for _, (conns, reason) in snapshot.items():
                    if conns > peak:
                        peak = conns
                    last = conns
                    counts[reason if reason is not None else "none"] += 1
                windows.append(
                    {
                        "window": window,
                        "samples": len(snapshot),
                        "peak": peak,
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
                    health = counts["health"]
                    drain = counts["drain"]
                    circuit = counts["circuit"]
                    overload = counts["overload"]
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
                        value = 0 if counts is None else counts[reason]
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

        elif op[0] == "ci":
            _, config, now = op
            # B 限流与 faults 引用未知后端：BACKEND，先于活动状态判定。
            config_backend_ids = {entry[0] for entry in config["backends"]}
            for scope, bucket_id, _, _ in config["limits"]:
                if scope == "B" and bucket_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            for fault_id in config["faults"]:
                if fault_id not in config_backend_ids:
                    fail(EXIT_BACKEND, "BACKEND")
            # 有活动连接或排队项时拒绝热加载：STATE。
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")
            if next_rev > 10 ** 18:
                # rev 耗尽：不分配、不改历史。
                fail(EXIT_STATE, "STATE")
            # 校验全部通过，原子替换配置并以 now 重建默认运行态（v7 同步
            # 载入 faults 时间线，故障运行态统计/基线仍重置）。
            apply_config(config, now)
            # 成功后把规范化 version=7 配置存为提交：rev 从 1 起递增，
            # 仅保留最近 16 条；失败不分配、不改历史。
            commit_history.append((next_rev, export_config()))
            next_rev += 1
            if len(commit_history) > 16:
                commit_history.pop(0)
            results.append({"op": "ci", "ok": True})

        elif op[0] == "cl":
            # 配置提交历史（只读）：current 为最新 rev 或 null，commits 按
            # rev 升序（历史本就按分配序追加），项键序 rev,config，config
            # 复用 ce 的逐层键序与值格式。O(16(B+M))。
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

        elif op[0] == "cb":
            # 配置回滚：按目标快照执行 ci 的原子替换与默认运行态重建，成功
            # 另建新 rev；全部校验先于任何变更，失败天然回滚时钟、配置、
            # 运行态、rev 与历史。
            _, target_rev, now = op
            snapshot = None
            for rev, committed in commit_history:
                if rev == target_rev:
                    snapshot = committed
                    break
            if snapshot is None:
                # 目标不存在（从未分配或已按 16 条淘汰）。
                fail(EXIT_STATE, "STATE")
            if next_rev > 10 ** 18:
                # rev 耗尽。
                fail(EXIT_STATE, "STATE")
            # 有活动连接或排队项时拒绝回滚：STATE。
            if connections or wait_queue:
                fail(EXIT_STATE, "STATE")
            # 快照即规范化 version=7 配置，重解析后沿用 ci 的替换语义；
            # 快照来自 export_config，必然合法，不会抛 INPUT。
            apply_config(parse_config(snapshot), now)
            # 原历史保留，追加新 rev 后再按 16 条淘汰。
            commit_history.append((next_rev, export_config()))
            new_rev = next_rev
            next_rev += 1
            if len(commit_history) > 16:
                commit_history.pop(0)
            results.append(
                {"op": "cb", "target": target_rev, "rev": new_rev, "ok": True}
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

        elif op[0] == "tk":
            _, cid, now = op
            if ttl_cfg is None:
                fail(EXIT_STATE, "STATE")
            connection = connections.get(cid)
            if connection is None:
                fail(EXIT_CONNECTION, "CONNECTION")
            if now >= connection[3] + ttl_cfg:
                # 命中已到期连接同样报 CONNECTION，不刷新 last。
                fail(EXIT_CONNECTION, "CONNECTION")
            connection[3] = now
            results.append({"op": "tk", "ok": True})

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
            if ttl_cfg is None:
                fail(EXIT_STATE, "STATE")
            # 按建连顺序（dict 保序）删除全部到期连接并递减后端并发。
            expired = []
            for cid, connection in list(connections.items()):
                if now >= connection[3] + ttl_cfg:
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
