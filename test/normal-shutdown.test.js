import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import { PHASE, COORDINATOR_STATUS, PHASE_STATUS, CONNECTION_KIND, buildDrainPlan } from '../src/index.js';
import { makeCoordinator, baseConfig } from './helpers.js';

test('正常下线：先摘流量、等传播、再关监听，宽限期完整经过后终止', async () => {
  const clock = new VirtualClock(10_000);
  const config = {
    ...baseConfig,
    connections: [
      { id: 'c1', kind: CONNECTION_KIND.STATELESS, requestDeadlineInMs: 2500 },
      { id: 'c2', kind: CONNECTION_KIND.STATEFUL, requestDeadlineInMs: 4000 },
    ],
  };
  const { coordinator, calls } = makeCoordinator(config, { clock });

  const runPromise = coordinator.run();
  await clock.advance(20_000);
  const snapshot = await runPromise;

  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.deepEqual(
    calls.map((call) => call.phaseId),
    [PHASE.STOP_ACCEPTING, PHASE.CLOSE_LISTENER],
  );
  assert.equal(calls[0].at, 0);
  assert.equal(calls[1].at, 3000);

  const { plan } = snapshot;
  assert.equal(plan.listenerCloseAtMs, 3000);
  assert.equal(plan.drainCompleteAtMs, 4000);
  assert.equal(plan.safeTerminateAtMs, 7000);
  assert.equal(plan.safeTerminateWallMs, 17_000);
  assert.deepEqual(plan.forcedConnectionIds, []);

  const phaseOrder = plan.phases.map((phase) => phase.id);
  assert.deepEqual(phaseOrder, [
    PHASE.STOP_ACCEPTING,
    PHASE.WAIT_PROPAGATION,
    PHASE.CLOSE_LISTENER,
    PHASE.DRAIN_CONNECTIONS,
    PHASE.TERMINATION_GRACE,
  ]);
  assert.deepEqual(
    [...snapshot.phaseStatuses.values()],
    phaseOrder.map(() => PHASE_STATUS.COMPLETED),
  );
  assert.equal(snapshot.remainingConnections.total, 0);

  const timelineTypes = snapshot.timeline.map((event) => event.type);
  assert.ok(timelineTypes.includes('terminated'));
  assert.equal(
    timelineTypes.indexOf('phase_completed') <
      timelineTypes.lastIndexOf('phase_completed'),
    true,
  );
});

test('规划器直接输出各时刻剩余连接', () => {
  const plan = buildDrainPlan({
    ...baseConfig,
    now: 0,
    connections: [
      { id: 'c1', kind: CONNECTION_KIND.STATELESS, requestDeadlineInMs: 2500 },
      { id: 'c2', kind: CONNECTION_KIND.STATEFUL, requestDeadlineInMs: 4000 },
    ],
  });
  assert.equal(plan.getRemainingConnectionsAt(0).total, 2);
  assert.equal(plan.getRemainingConnectionsAt(3500).total, 1);
  assert.deepEqual(plan.getRemainingConnectionsAt(3500).connectionIds, ['c2']);
  assert.equal(plan.getRemainingConnectionsAt(5000).total, 0);
});
