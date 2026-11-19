# service-endpoint-drain-coordinator

协调服务发现摘除、负载均衡器传播、连接排空与实例终止顺序的下线协调器。

## 边界：只生成协调计划

本库是一个**规划器与状态机**，不直接调用服务发现 API、不关闭 socket、不杀进程。

- `buildDrainPlan()` 是纯函数，输入端点状态与时间预算，输出 `ready -> draining -> terminated`
  的阶段时间线、剩余连接投影和可安全终止时间，不产生任何副作用。
- `DrainCoordinator` 只负责按计划推进阶段、处理暂停/恢复/失败重试；真正的摘除、关监听、
  强制断连动作通过构造函数注入的 `actions` 回调由调用方执行。
- 注入的动作失败只会让协调器暂停在当前阶段，重试时重新执行该阶段，不会跳过任何阶段，
  也不会跳过终止宽限期。

## 阶段顺序（强制约束）

1. `stop_accepting_new_requests`：端点标记为 draining 并从服务发现摘除，**先停止获得新请求**，
   监听保持可用。
2. `wait_load_balancer_propagation`：等待所有区域的健康检查间隔与负载均衡器规则传播完成。
3. `close_listener`：仅当所有区域确认摘除后才关闭监听器；计划中带 guard，违反顺序会失败暂停。
4. `drain_connections`：等待在途请求按 deadline 自然结束。
5. `force_close_connections`：达到 `maxDrainTimeMs` 强制边界时，关闭仍未结束的连接（仅在需要时出现）。
6. `termination_grace`：终止宽限期，`skippable: false`，暂停期间逻辑时钟冻结，恢复后必须补足。

先摘流量、再等传播、最后才关监听，避免“实例先停止监听但负载均衡器仍在转发”导致的新请求失败。

## 时间语义

- 计划内所有 `*Ms` 字段都是相对排空开始时刻的**逻辑毫秒数**，墙钟时间由 `t0Ms + 偏移量` 换算
  （`safeTerminateWallMs` 已直接给出）。
- 区域传播延迟按区域分别计算：`effectiveDelayMs = healthCheckIntervalMs + propagationDelayMs`，
  全局监听关闭时间取所有区域的最大值，处理分区域传播差异。
- `maxDrainTimeMs` 是强制终止边界，必须不小于最长区域传播延迟，否则规划器直接拒绝。
- `terminationGraceMs` 是排空完成后的固定宽限，不能与排空等待重叠或被跳过。

## 连接模型

连接通过 `kind` 与可选字段描述：

- `stateless`（默认，可重试）：无 deadline 时在监听关闭点放弃，交给负载均衡器重试到其他实例。
- `stateful`（默认不可重试）：无 deadline 时只能依赖 `maxDrainTimeMs` 强制关闭。
- `long_lived`：长连接（如 WebSocket），默认不可重试，强制边界兜底，避免发布永久挂起。
- `state: 'retrying'` 与 `retryCount` 表达重试请求；`requestDeadlineAtMs`（墙钟）或
  `requestDeadlineInMs`（相对）表达请求 deadline。

传播窗口内到达的“最后时刻新请求”：监听关闭前应被接受，用
`coordinator.adoptInFlightConnection()` 纳入计划重算；监听关闭后到达则抛出
`LISTENER_CLOSED`，由调用方返回 503 并依赖负载均衡器重试。

## 用法

```js
import { buildDrainPlan, DrainCoordinator, SystemClock } from './src/index.js';

const plan = buildDrainPlan({
  now: Date.now(),
  healthCheckIntervalMs: 1000,
  regions: [
    { id: 'cn-east', propagationDelayMs: 2000 },
    { id: 'eu-central', propagationDelayMs: 4000 },
  ],
  maxDrainTimeMs: 10000,
  terminationGraceMs: 3000,
  connections: [
    { id: 'req-1', kind: 'stateful', requestDeadlineInMs: 5000 },
    { id: 'ws-1', kind: 'long_lived' },
  ],
});

console.log(plan.listenerCloseAtMs);      // 5000（健康检查 + 最慢区域）
console.log(plan.drainCompleteAtMs);       // 10000（长连接强制边界）
console.log(plan.safeTerminateAtMs);       // 13000
console.log(plan.getRemainingConnectionsAt(6000));

const coordinator = new DrainCoordinator(
  {
    healthCheckIntervalMs: 1000,
    regions: [{ id: 'cn-east', propagationDelayMs: 2000 }],
    maxDrainTimeMs: 10000,
    terminationGraceMs: 3000,
    connections: [],
  },
  {
    clock: new SystemClock(),
    actions: {
      async stop_accepting_new_requests() { /* 调用服务发现摘除 */ },
      async close_listener() { /* 关闭 server 监听 */ },
      async force_close_connections(phase) { /* 关闭 phase.connectionIds */ },
    },
  },
);

const snapshot = await coordinator.run();
```

### 暂停、恢复与失败重试

```js
coordinator.pause('人工介入');          // 逻辑时钟冻结，宽限期计时暂停
coordinator.resume();                   // 从暂停的阶段继续，宽限期补足而非跳过
await coordinator.retryFailedPhase();   // 动作失败后重试当前阶段，不重放已完成阶段
```

`getSnapshot()` 返回当前状态、阶段状态表、事件时间线、剩余连接投影与完整计划；
`getRemainingConnections()` 给出当前逻辑时刻的剩余连接分类统计。

## 输出内容

- 阶段时间线：`plan.phases`（含每阶段 `startMs`/`endMs`/`actionRequired`/guard）与
  `plan.events`（区域摘除、强制断连、可终止等事件）。
- 剩余连接：`plan.getRemainingConnectionsAt(atMs)` 与协调器的 `getRemainingConnections()`，
  区分无状态、有状态、长连接、重试中与将被强制关闭的连接。
- 可安全终止时间：`plan.safeTerminateAtMs`（逻辑时间）与 `plan.safeTerminateWallMs`（墙钟时间）。

## 测试

测试使用内置 `node:test` 与可确定性推进的 `VirtualClock`，不依赖真实定时器：

```bash
node --test
```

覆盖场景：正常下线、分区域传播延迟、长请求与强制边界、最后时刻新请求、无连接、
多区域端点，以及阶段失败暂停/恢复与宽限期不可跳过。

## 环境要求

- Node.js `24.16.0`（`package.json` 的 `engines` 已声明），使用 ESM 与内置测试运行器。
