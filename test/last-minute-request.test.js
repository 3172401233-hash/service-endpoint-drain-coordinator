import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import {
  COORDINATOR_STATUS,
  CONNECTION_KIND,
  DrainError,
} from '../src/index.js';
import { makeCoordinator, baseConfig } from './helpers.js';

test('最后时刻新请求：传播窗口内到达会被接受并纳入计划', async () => {
  const clock = new VirtualClock(0);
  const config = {
    ...baseConfig,
    maxDrainTimeMs: 10_000,
    connections: [
      { id: 'existing', kind: CONNECTION_KIND.STATELESS, requestDeadlineInMs: 3500 },
    ],
  };
  const { coordinator } = makeCoordinator(config, { clock });
  const runPromise = coordinator.run();

  await clock.advance(2000);
  const updated = await coordinator.adoptInFlightConnection({
    id: 'late-1',
    kind: CONNECTION_KIND.STATEFUL,
    requestDeadlineInMs: 6000,
  });
  assert.ok(updated.plan.connections.some((item) => item.id === 'late-1'));
  assert.equal(updated.plan.drainCompleteAtMs, 6000);
  assert.equal(updated.remainingConnections.total, 2);

  await clock.advance(12_000);
  const snapshot = await runPromise;
  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.equal(snapshot.plan.safeTerminateAtMs, 9000);
  assert.ok(snapshot.timeline.some((event) => event.type === 'connection_adopted'));
});

test('最后时刻新请求：监听关闭后到达必须拒绝，由负载均衡器重试到其他实例', async () => {
  const clock = new VirtualClock(0);
  const { coordinator } = makeCoordinator(baseConfig, { clock });
  const runPromise = coordinator.run();

  await clock.advance(3000);
  assert.throws(
    () =>
      coordinator.adoptInFlightConnection({
        id: 'too-late',
        kind: CONNECTION_KIND.STATELESS,
      }),
    (error) => error instanceof DrainError && error.code === 'LISTENER_CLOSED',
  );

  await clock.advance(10_000);
  const snapshot = await runPromise;
  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
});
