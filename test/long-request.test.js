import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import {
  PHASE,
  COORDINATOR_STATUS,
  CONNECTION_KIND,
  CONNECTION_STATE,
  buildDrainPlan,
} from '../src/index.js';
import { makeCoordinator, baseConfig } from './helpers.js';

test('长连接与无 deadline 请求在 maxDrainTimeMs 强制边界被关闭', () => {
  const plan = buildDrainPlan({
    ...baseConfig,
    maxDrainTimeMs: 10_000,
    connections: [
      { id: 'ws-1', kind: CONNECTION_KIND.LONG_LIVED },
      { id: 'slow-1', kind: CONNECTION_KIND.STATEFUL, requestDeadlineInMs: 30_000 },
      { id: 'fast-1', kind: CONNECTION_KIND.STATELESS, requestDeadlineInMs: 4000 },
    ],
  });

  assert.deepEqual(plan.forcedConnectionIds, ['ws-1', 'slow-1']);
  assert.equal(plan.drainCompleteAtMs, 10_000);
  assert.equal(plan.getRemainingConnectionsAt(9999).willForceClose, 2);
  const remaining = plan.getRemainingConnectionsAt(9999);
  assert.equal(remaining.longLived, 1);
  assert.equal(remaining.stateful, 1);
  assert.equal(remaining.stateless, 0);
  assert.equal(plan.getRemainingConnectionsAt(10_000).total, 0);

  const forcePhase = plan.phases.find((phase) => phase.id === PHASE.FORCE_CLOSE);
  assert.deepEqual(forcePhase.connectionIds, ['ws-1', 'slow-1']);
  assert.equal(forcePhase.startMs, 10_000);

  const gracePhase = plan.phases.find((phase) => phase.id === PHASE.TERMINATION_GRACE);
  assert.equal(gracePhase.startMs, 10_000);
  assert.equal(gracePhase.endMs, 13_000);
  assert.equal(gracePhase.skippable, false);
});

test('协调器在强制边界执行 force_close 动作，然后完整走完宽限期', async () => {
  const clock = new VirtualClock(0);
  const config = {
    ...baseConfig,
    maxDrainTimeMs: 10_000,
    terminationGraceMs: 3000,
    connections: [{ id: 'ws-1', kind: CONNECTION_KIND.LONG_LIVED }],
  };
  const { coordinator, calls } = makeCoordinator(config, { clock });

  const runPromise = coordinator.run();
  await clock.advance(13_000);
  const snapshot = await runPromise;

  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  const forceCall = calls.find((call) => call.phaseId === PHASE.FORCE_CLOSE);
  assert.deepEqual(forceCall.connectionIds, ['ws-1']);
  assert.equal(forceCall.at, 10_000);
  assert.equal(snapshot.plan.safeTerminateAtMs, 13_000);
});

test('重试请求：可重试的无状态连接在监听关闭后放弃并交给负载均衡器重试', () => {
  const plan = buildDrainPlan({
    ...baseConfig,
    maxDrainTimeMs: 10_000,
    connections: [
      {
        id: 'retry-1',
        kind: CONNECTION_KIND.STATELESS,
        state: CONNECTION_STATE.RETRYING,
        retryCount: 2,
      },
      { id: 'retry-2', kind: CONNECTION_KIND.STATEFUL },
    ],
  });

  const retryConnection = plan.connections.find((item) => item.id === 'retry-1');
  assert.equal(retryConnection.releasedAtListenerClose, true);
  assert.equal(retryConnection.effectiveEndMs, plan.listenerCloseAtMs);

  const statefulConnection = plan.connections.find((item) => item.id === 'retry-2');
  assert.equal(statefulConnection.forceClosed, true);
  assert.equal(plan.getRemainingConnectionsAt(plan.listenerCloseAtMs).total, 1);
  assert.equal(
    plan.getRemainingConnectionsAt(plan.listenerCloseAtMs).connectionIds[0],
    'retry-2',
  );
});

test('连接在强制边界前自然结束会重新缩短排空时间线', async () => {
  const clock = new VirtualClock(0);
  const config = {
    ...baseConfig,
    maxDrainTimeMs: 10_000,
    connections: [{ id: 'slow-1', kind: CONNECTION_KIND.STATEFUL, requestDeadlineInMs: 9000 }],
  };
  const { coordinator } = makeCoordinator(config, { clock });
  const runPromise = coordinator.run();

  await clock.advance(4999);
  coordinator.finishConnection('slow-1');
  assert.equal(coordinator.getSnapshot().plan.drainCompleteAtMs, 3000);

  await clock.advance(8000);
  const snapshot = await runPromise;
  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.equal(snapshot.plan.safeTerminateAtMs, 6000);
});
