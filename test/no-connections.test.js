import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import { COORDINATOR_STATUS, buildDrainPlan } from '../src/index.js';
import { makeCoordinator, baseConfig } from './helpers.js';

test('无连接：监听关闭即排空完成，但仍完整经过终止宽限期', async () => {
  const plan = buildDrainPlan({ ...baseConfig, now: 0, connections: [] });
  assert.equal(plan.drainCompleteAtMs, plan.listenerCloseAtMs);
  assert.equal(plan.safeTerminateAtMs, plan.listenerCloseAtMs + plan.terminationGraceMs);
  assert.equal(plan.getRemainingConnectionsAt(0).total, 0);

  const clock = new VirtualClock(5000);
  const { coordinator } = makeCoordinator({ ...baseConfig, connections: [] }, { clock });
  const runPromise = coordinator.run();
  await clock.advance(6000);
  const snapshot = await runPromise;

  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.equal(snapshot.plan.listenerCloseAtMs, 3000);
  assert.equal(snapshot.plan.drainCompleteAtMs, 3000);
  assert.equal(snapshot.plan.safeTerminateAtMs, 6000);
  assert.equal(snapshot.plan.safeTerminateWallMs, 11_000);
  assert.equal(snapshot.remainingConnections.total, 0);
  assert.deepEqual(snapshot.plan.forcedConnectionIds, []);
});
