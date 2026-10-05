# 多租户 API 配额租约服务

平台把全局容量沿 **组织 → 项目 → 客户端** 三级树向下分配；子级只能在父级
*当前可用额度内*申请带有效期的租约。实际调用按**幂等用量回执**扣减，
归还 / 撤销 / 到期 / 离线只释放**尚未消费**的部分；已经确认的消费永不消失。
服务重启后从**只追加事件账本**重放重建当前余额。

零外部依赖：仅 Python 3.11 标准库（含 SQLite）。

---

## 只读灾备复制器（DR Replicator）

在主站之上内置一套**只读灾备复制器**：从主站持续拉取带序号与哈希链的
事件**分段**，本地校验后原子更新只读投影；支持受控切换演练、带任期
只读地址、故障分段隔离与离线审计。代码在 `quota/replica/`，操作页面
在 `quota/replica_static/`，测试见 `test_replica.py`（81 项）。

### 快速开始

```bash
# 一体化演示（主站 + feed + 灾备同进程；写接口 /api，灾备 /dr）
python3 -m quota.replica --mode demo --port 8765
#   灾备运维页面: http://localhost:8765/dr/

# 真实部署：主站（带复制 feed，进程重启自动换届）
python3 -m quota.replica --mode primary --quota-db ./quota.db --port 8765
# 独立灾备节点（跨进程 HTTP 拉取）
python3 -m quota.replica --mode dr --primary-url http://主站:8765 \
    --dr-db ./dr.db --port 8770 --lag-limit 1000

# 离线审计
python3 -m quota.replica.audit --db ./dr.db          # 文本
python3 -m quota.replica.audit --db ./dr.db --json   # JSON（失败退出码 2）
```

### 分段与双链

每条写命令的**原子批次**在主站 `append_batch` 的**同一个 SQLite 事务**
内被密封为一个分段：段与业务事件同时可见，主站崩溃不会产生「事件已
落账、分段缺失」的窗口。分段带：

- `term`（主站任期，主站进程重启 / 显式换届 +1）、`index`（全局严格
  连续，从 1 开始）、`from_seq/to_seq`、`batch_id`；
- **段链**：`prev_digest → digest`，摘要覆盖整段规范正文（含每事件的
  event_hash 与载荷）；
- **事件链**：分段内事件保留主站 per-event SHA-256 链，且段首事件
  `prev_hash` 锚定上段末事件 `event_hash` —— 两条链互相锚定。

### 原子可见的确认截面

复制器顺序执行「准入 → 双链校验 → 单 SQLite 事务落账 → 同一临界区
折叠内存投影」，确认边界只在事务提交后前进。只读接口
（`GET /dr/api/state`）只暴露已确认边界，因此主站持续写入时灾备查询
**绝不会**看到「事件已进入、但余额/租约/草案/命令结果只更新一部分」
的半成品截面。重复拉取内容一致即幂等忽略，绝不重复应用；传输中断或
进程重启后从最后确认的 `(index, seq)` 继续（重启时从已确认事件重放
重建投影）。

### 隔离与熔断（不污染最近可用截面）

下列分段一律写入**隔离区**（保留正文+原因+时间）并进入 `QUARANTINED`
熔断态，未经运维处置不再推进，最近可用截面保持不变：

| 原因码 | 触发条件 |
|---|---|
| `gap` | 分段缺失：index 不是确认边界的下一段 |
| `out_of_order` | 错序：index 落在已确认范围且无记录 |
| `same_index_diff_body` | 同序号正文不同（摘要不一致） |
| `digest_mismatch` | 段摘要自校验失败（正文/摘要被改） |
| `chain_broken` | 段链断裂：prev_digest 未指向上段 |
| `event_chain_broken` / `event_payload_tampered` | 事件 prev_hash 断链 / event_hash 重算不符 |
| `batch_split` | 同一 command_id 的事件跨越分段边界 |
| `seq_mismatch` | from/to_seq 或事件 seq 不连续 |
| `stale_term` | **旧任期进程迟到更新**（term 落后） |
| `new_term` | 主站已换届，未显式 `adopt-term` 前拒绝 |
| `malformed` | 信封结构非法 |

落后量超过 `--lag-limit` 时标记 `LAGGING`（截面仍完整但禁止演练），
追平后自动恢复。运维确认后 `POST /dr/api/control/resume` 解除熔断
（隔离记录保留用于审计）。

### 受控切换演练

1. `POST /dr/api/drills {"idem_key":..., "ttl_seconds":...}` ——
   原子锁定当前确认截面并**暂停拉取**；稳定窗口后复检主站水位，然后
   执行 7 项验证：锁定边界、段链、事件链（含跨段锚定）、重放等价、
   投影守恒（非负余额 + Σ可用+总消费==容量）、命令结果完整（用户命令
   批次不被拆分、回执齐备）、**验证期间无新分段**。
2. 通过后 `POST /dr/api/drills/{id}/issue` 发放**带任期的不透明只读
   地址** `/ro/{token}/state`（另附 `/audit` 报告）。地址只看到锁定
   截面（按 locked_seq 独立折叠并缓存），复制追赶与它无关。
3. **同一 `idem_key` 再次创建/发放返回原结果**（幂等）；
   `POST /dr/api/drills/{id}/cancel` 后地址立即失效（410）并恢复复制；
   TTL 到期地址 410。
4. **验证时主站又密封了新分段** → 演练失败（`new_segments_during_verification`）、
   不发地址、自动恢复复制；同键重试保持原失败结果。
5. 主站换届 / `adopt-term` 推进持久化 **fence** 时，所有未完成演练地址
   立即作废（`invalidated_by_term_change`）。

### HTTP 接口（灾备侧）

| 方法 / 路径 | 说明 |
|---|---|
| `GET /dr/api/status` | 复制进度、任期、确认边界、实时落后、隔离数、暂停/fence |
| `POST /dr/api/tick` | 立即执行一轮拉取（排障/测试） |
| `GET /dr/api/state` | **最近已确认只读截面**（节点余额/租约/草案视图） |
| `GET /dr/api/segments` | 已确认分段（段链） |
| `GET /dr/api/quarantine` | 隔离区（原因/正文/时间） |
| `GET /dr/api/audit` · `GET /dr/api/audit-log` | 在线一致性审计 · 任务审计日志 |
| `POST /dr/api/control/pause` · `resume` · `adopt-term` | 暂停 / 恢复 / 显式接管任期 |
| `POST /dr/api/drills` · `GET /dr/api/drills[/{id}]` | 创建（锁定+验证）/ 列表 / 报告 |
| `POST /dr/api/drills/{id}/issue` · `cancel` | 发放带任期只读地址 / 取消失效 |
| `GET /ro/{token}/state` · `/ro/{token}/audit` | 锁定截面只读查询 / 演练验证报告 |
| `GET /dr/` | 灾备运维操作页面 |

主站 feed（仅安装复制器后存在）：`GET /api/feed/status`、
`GET /api/feed/segments?after=&limit=`；一体化/`--allow-inject` 下另有
`POST /api/feed/bump-term`、`/api/feed/inject`（digest/payload/drop
故障注入，仅测试）、`/api/feed/clear-injection`。

### 代码结构（新增）

| 文件 | 职责 |
|---|---|
| `quota/ledger.py` | 新增事务内 `batch_hook`（默认不安装，主站行为不变） |
| `quota/replica/protocol.py` | 分段信封、段摘要、事件哈希重算、隔离原因码 |
| `quota/replica/feed.py` | 主站分段密封器（与事件同事务）、任期、故障注入 |
| `quota/replica/store.py` | 灾备任务库：确认边界/事件/回执/分段/隔离/演练/审计 |
| `quota/replica/replicator.py` | 拉取循环、准入校验、幂等、隔离熔断、任期栅栏、只读投影 |
| `quota/replica/drill.py` | 演练生命周期、锁定截面、带任期只读地址、TTL/取消 |
| `quota/replica/verify.py` | 演练截面 7 项验证 |
| `quota/replica/audit.py` · `audit_cli.py` | 在线/离线一致性审计与命令行 |
| `quota/replica/server.py` · `__main__.py` | 灾备 HTTP 接口与 primary/dr/demo 三种启动模式 |
| `quota/replica_static/index.html` | 灾备运维操作页面 |
| `test_replica.py` | 81 项灾备端到端测试（含真实 HTTP、重启、换届、故障注入） |

---

## 快速开始（主站）

```bash
python3 -m quota.http_server --port 8765 --db ./quota.db
# 打开 http://localhost:8765/ 即为租约控制台
```

跑测试（功能 / 并发 / 到期 / 离线 / 续约 / 重启重建 / 账本篡改 /
批量容量草案，共 220 项；灾备复制器另见 81 项 `test_replica.py`）：

```bash
python3 test_service.py
python3 test_replica.py
```

---

## 架构

```
HTTP (http_server.py)
  └─ QuotaService (service.py)        串行命令、幂等去重、到期 sweep、审计
       ├─ Projection (domain.py)      内存余额/租约投影（事件折叠）
       └─ EventStore  (ledger.py)     SQLite 只追加事件 + 命令回执 + 哈希链
```

账本是唯一事实来源。进程启动时清空内存、从 `events` 表完整重放；
任何业务状态都不做外部写入。

### 事件（append-only）

| 事件 | 含义 |
|---|---|
| `Bootstrapped` | 设置全局容量（一次性） |
| `NodeRegistered` | 注册组织 / 项目 / 客户端 |
| `CapacityAdjusted` | 调整全局容量（缩容防超卖） |
| `NodeCapacityAdjusted` | 调整节点显式容量：沿父子边移动余额（批量草案发布产生） |
| `LeaseGranted` | 父子节点间授予一条租约 |
| `UsageRecorded` | 幂等用量回执，按 `usage_event_id` 去重 |
| `LeaseExpired` | 租约到期，标记 `pending_expired`（不动余额） |
| `LeaseAmended` | 修订有效期：截止时间延后，租约修订号 +1（不动余额） |
| `LeaseClosed` | 结算租约，退回未消费余量（修订号同样 +1） |
| `ClientPresenceChanged` | 客户端上 / 下线 |
| `CapacityPlanCreated` | 批量容量调整草案创建（不动余额，记录创建时账本位置） |
| `CapacityPlanPreviewed` | 草案在某账本水位完成预览（不动余额） |
| `CapacityPlanPublished` | 草案发布完成（紧随整批 `NodeCapacityAdjusted`/`CapacityAdjusted`） |
| `CapacityPlanDiscarded` | 草案废弃 |

每条事件带 `prev_hash/event_hash`（SHA-256 链）与所属 `command_id`，
篡改任意载荷都会让审计的哈希链检查断裂。

### 记账规则（关键不变量）

每个节点一个余额账户，租约是父子之间的一条边：

```
授予 A：   lessor 余额 −A，holder 余额 +A
消费 C：   holder 余额 −C；C 在每个祖先层的活动入边上贪心吸收
           （祖先边的可退余量同步收缩 C，中间节点余额不变）
关闭：     holder −(A−consumed)，lessor +(A−consumed)   # 只退未消费
```

由此恒成立：

- **非负余额**：任何节点 `avail ≥ 0`（无超卖）；
- **全局守恒**：`Σ 可用 + 总消费 = 全局容量`；
- **消费不复活**：关闭只退 `A−consumed`，已消费永久保留为消费记录。

### 为什么到期要“挂起 → 延迟结算”

中间层租约（如项目租约）到期时，它的余量可能已经被持有者合法地分配给
了更下层的活动租约。若到期就强抽，会把持有者余额压成负。因此：

1. 到期先置为 `pending_expired`：停止接受新消费 / 新授约，但**不动余额**；
2. 后台 sweep 在每条命令前自底向上做固定点结算：一旦下游释放使持有者
   余额足以让出该租约剩余，才真正 `LeaseClosed` 退款。

这样中间层到期永远不会引发跨层超卖；重启后待结算租约会在重放+sweep 时
继续走完结算。

### 有效期修订（续约）与乐观修订号

每条租约带一个单调递增的 **revision（修订号）**：授予即为 `0`，任何成功
的状态变更都让它 `+1`（`LeaseAmended` 与 `LeaseClosed` 都推进版本）。
持有人为**仍在生效**的租约提交它所看到的 `expected_revision` 与一个更晚
的截止时间，服务端在锁内校验：

- `expected_revision == 当前修订号`，且新截止时间**严格晚于**当前截止时间；
- 租约必须 `active`（已 `closed`、或到期 `pending_expired` 等待结算都拒绝）；
- 持有人是已下线客户端时拒绝；
- 永久租约（授予时无 TTL）不能修订。

满足则追加一条 `LeaseAmended`（只改时间，不动任何余额），修订号递增。
冲突返回 `409` 并在 `details` 中回传租约最新状态（`current_revision` /
`expires_at` / `status`），失败方据此重试即可。截止时间二选一：
`extend_seconds`（在**当前截止时间**之上顺延的增量）或绝对
`new_expires_at`（Unix 时间戳）。

修订与**归还 / 撤销 / 用量**共享同一个版本号：这些命令都可携带
`expected_revision` 做乐观并发。因此「续约」与「归还」同时发生时，在锁内
只有一个变更能追加进事件账本，另一方必然收到 `409` 并看到最新状态
（不带 `expected_revision` 的旧调用保持原有行为，向后兼容）。相同
`command_id` 的重试由幂等命令表重放**第一次**的结果，包括返回过的截止时间，
不会因相对延长量而重新计算。修订号、截止时间与修订历史都只存在于事件账本，
重启重放后完整恢复。

### 批量容量调整草案（原子批次 + 乐观账本位置）

管理员可以把**多条节点容量变更**编辑成一份草案（`draft`），先预览再发布：

1. **创建** `POST /api/capacity-plans`：只做结构校验（节点存在、无重复、容量非负），
   记录创建时的**账本位置**（业务水位 `base_seq`）。草案不动任何余额。
2. **预览** `POST /api/capacity-plans/preview`：在内存副本上按顺序推演整批变更，
   返回逐项校验结果（每项的旧/新容量、Δ、**拒绝原因**）、**执行后的逐层余额**
   （平台→各变更节点的祖先链）与**受影响的未结算租约**（出租方/持有方角色、
   余额前后快照）。预览把「最后预览水位」与快照一起持久化。
3. **发布** `POST /api/capacity-plans/publish`：满足全部前提才把整批变更作为
   **一个原子批次**写入——业务事件（`NodeCapacityAdjusted`，平台项为
   `CapacityAdjusted`）与 `CapacityPlanPublished` 及草案表状态在**同一 SQLite
   事务**提交，**任一项不合法则整批回滚、没有任何变更生效**。
4. **废弃** `POST /api/capacity-plans/discard`：草案可废弃；废弃/发布后状态不可逆。

容量调整的记账只是沿**父子边移动余额**：扩容 `Δ>0` 时父级出 `Δ`（父级可用不足
拒绝），缩容 `Δ<0` 时节点退回 `−Δ`（节点当前可用不足、会被抽成负数时拒绝）。
因此「非负余额」与「全局守恒」对整批调整天然恒成立。

**陈旧版本（乐观账本位置）**：发布要求「最后一次预览的业务水位 == 当前水位」。
水位取 `MAX(seq)` 中**排除草案生命周期事件**的业务事件序号，所以创建/预览草案后
发生的任何**新授约、消费、续约、结算（含后台到期 sweep）、容量调整、上/下线**
都会推进水位，使发布返回 `409` 并在 `details.intervening_changes` 中列出发生了
哪类变更、提示需要**重新预览**；而另一份草案的创建/预览/废弃不会互相干扰。
重新预览即刷新基线，随后可发布。

**幂等与持久化**：重复发布（无论是否同一 `command_id`）只能得到**首次发布结果**；
草案创建支持显式 `command_id` 重试幂等；废弃幂等。草案权威行存于 `capacity_plans`
表，生命周期事件同时进哈希链，**服务重启后**草案、预览快照、首次发布结果与审计
轨迹均可查询（审计新增 `capacity_plan_consistency`：表行与账本折叠逐字段一致）。

### 防超卖 / 一致性的四道防线

1. **进程内串行**：所有命令在同一把锁内“到期结算 → 去重 → 校验 → 原子提交”；
2. **授予守卫**：`amount ≤ avail(lessor)`，跨层并发申请由锁保证读改写原子；
3. **关闭守卫**：显式归还 / 撤销要求持有者让出剩余后余额非负（防止父级缩容
   抽走已分配给孙级的额度）；到期走挂起+延迟结算，离线只关客户端叶子边；
4. **幂等命令表 + 用量回执**：相同 `command_id` / `Idempotency-Key` 重放原
   结果；相同 `usage_event_id` 绝不二次扣减。

---

## HTTP API

| 方法 / 路径 | 说明 |
|---|---|
| `POST /api/bootstrap` | `{capacity}` 初始化全局容量（一次性） |
| `POST /api/nodes` | `{type, parent_id, node_id?}` 注册节点 |
| `POST /api/capacity` | `{capacity}` 调整容量（低于“已承诺/消费下限”拒绝） |
| `POST /api/leases` | `{lessor_id, holder_id, amount, ttl_seconds?}` 申请租约 |
| `POST /api/leases/usage` | `{lease_id, amount, usage_event_id}` 幂等扣减 |
| `POST /api/leases/amend` | `{lease_id, expected_revision, extend_seconds}` 或 `{…, new_expires_at}` 修订有效期 |
| `POST /api/leases/release` | `{lease_id, expected_revision?}` 归还（释放未消费） |
| `POST /api/leases/revoke` | `{lease_id, expected_revision?}` 父级撤销 / 缩容 |
| `POST /api/clients/presence` | `{client_id, online}` 上 / 下线 |
| `POST /api/capacity-plans` | `{changes:[{node_id,capacity}], plan_id?}` 创建批量容量调整草案 |
| `POST /api/capacity-plans/preview` | `{plan_id}` 预览：逐层余额 / 受影响租约 / 逐项拒绝原因 |
| `POST /api/capacity-plans/publish` | `{plan_id}` 原子发布；陈旧版本 409 要求重新预览 |
| `POST /api/capacity-plans/discard` | `{plan_id}` 废弃草案 |
| `GET /api/capacity-plans` | 草案列表 |
| `GET /api/capacity-plans/{id}` | 草案详情（含预览快照 / 首次发布结果） |
| `GET /api/state` | 全量节点余额 + 租约视图 |
| `GET /api/audit` | 一致性审计（见下） |
| `GET /api/events` | 最近事件账本 |
| `GET /` | 租约控制台页面 |

所有写命令接受可选 `command_id`（或 `Idempotency-Key` 头）做幂等；
`usage` 必须提供 `usage_event_id`。错误返回结构化 JSON 与状态码：
`400` 参数错误 / `404` 不存在 / `409` 冲突（超卖、重复回执、状态不允许）。

### 审计 `GET /api/audit`

每次执行 8 项检查：哈希链完整、幂等命令表一致、非负余额、全局守恒、
层级边完整、租约记账合法、**从事件重放重建的状态与线上投影逐字段
一致**（`replay_equivalence`）、以及**容量调整草案表与账本折叠结果逐字段
一致**（`capacity_plan_consistency`：状态机、变更清单、创建水位、发布结果）。

---

## 代码结构

| 文件 | 职责 |
|---|---|
| `quota/ledger.py` | SQLite 事件存储、确定性哈希、批量原子提交、草案表同事务写入 |
| `quota/domain.py` | 事件折叠、命令校验、余额模型、到期结算 |
| `quota/plans.py` | 批量容量草案：结构校验、预览模拟、生命周期事件构造 |
| `quota/service.py` | 命令执行 / 幂等 / 重建 / 后台 sweep / 草案编排 / 审计 |
| `quota/http_server.py` | HTTP 路由与静态页面 |
| `static/index.html` | 租约控制台（节点树、租约操作、容量草案、审计、账本） |
| `test_service.py` | 220 项端到端测试（含真实重启、并发、续约竞争与草案全流程） |
