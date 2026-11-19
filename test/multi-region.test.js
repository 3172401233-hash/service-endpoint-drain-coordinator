import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import { PHASE, COORDINATOR_STATUS, CONNECTION_KIND } from '../src/index.js';
import { makeCoordinator } from './helpers.js';

test('多区域端点：最后一个区域摘除后才允许关闭监听', async () => {
  const clock = new VirtualClock(0);
  const config = {
    healthCheckIntervalMs: 1000,
    regions: [
      { id: 'us-west', propagationDelayMs: 1000 },
      { id: 'eu-central', propagationDelayMs: 6000 },
      { id: 'ap-south', propagationDelayMs: 3000 },
    ],
    maxDrainTimeMs: 20_000,
    terminationGraceMs: 2000,
    connections: [
      { id: 'req-1', kind: CONNECTION_KIND.STATEFUL, requestDeadlineInMs: 8000 },
    ],
  };
  const { coordinator, calls } = makeCoordinator(config, { clock });

  const runPromise = coordinator.run();

  await clock.advance(6999);
  const midSnapshot = coordinator.getSnapshot();
  assert.equal(midSnapshot.status, COORDINATOR_STATUS.DRAINING);
  assert.equal(
    midSnapshot.phaseStatuses.get(PHASE.CLOSE_LISTENER),
    'pending',
  );
  assert.ok(!calls.some((call) => call.phaseId === PHASE.CLOSE_LISTENER));

  await clock.advance(13_001);
  const snapshot = await runPromise;

  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.equal(calls.find((call) => call.phaseId === PHASE.CLOSE_LISTENER).at, 7000);
  assert.equal(snapshot.plan.listenerCloseAtMs, 7000);
  assert.equal(snapshot.plan.safeTerminateAtMs, 10_000);
  assert.equal(snapshot.plan.drainCompleteAtMs, 8000);
});
