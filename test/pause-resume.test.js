import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import {
  PHASE,
  COORDINATOR_STATUS,
  PHASE_STATUS,
  DrainError,
} from '../src/index.js';
import { makeCoordinator, baseConfig } from './helpers.js';

test('阶段失败：动作抛错后暂停在当前阶段，retry 后继续而不是跳过', async () => {
  const clock = new VirtualClock(0);
  let attempts = 0;
  const { coordinator } = makeCoordinator(baseConfig, {
    clock,
    handlers: {
      [PHASE.STOP_ACCEPTING]: () => {
        attempts += 1;
        if (attempts === 1) {
          return new Error('服务发现接口暂时不可用');
        }
        return null;
      },
    },
  });

  const firstRun = coordinator.run();
  await clock.advance(100);
  const pausedSnapshot = await firstRun;

  assert.equal(pausedSnapshot.status, COORDINATOR_STATUS.PAUSED);
  assert.equal(
    pausedSnapshot.phaseStatuses.get(PHASE.STOP_ACCEPTING),
    PHASE_STATUS.FAILED,
  );
  assert.equal(pausedSnapshot.lastFailure.phaseId, PHASE.STOP_ACCEPTING);
  assert.equal(attempts, 1);

  const secondRun = coordinator.retryFailedPhase();
  await clock.advance(20_000);
  const finalSnapshot = await secondRun;

  assert.equal(attempts, 2);
  assert.equal(finalSnapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.equal(
    finalSnapshot.phaseStatuses.get(PHASE.STOP_ACCEPTING),
    PHASE_STATUS.COMPLETED,
  );
});

test('暂停与恢复：暂停期间逻辑时钟冻结，宽限期不会被跳过或缩短', async () => {
  const clock = new VirtualClock(0);
  const { coordinator } = makeCoordinator(
    {
      ...baseConfig,
      healthCheckIntervalMs: 1000,
      maxDrainTimeMs: 10_000,
      terminationGraceMs: 5000,
    },
    { clock },
  );
  const runPromise = coordinator.run();

  await clock.advance(3000);
  assert.equal(coordinator.getSnapshot().status, COORDINATOR_STATUS.DRAINING);
  assert.equal(coordinator.getSnapshot().logicalMs, 3000);

  coordinator.pause('模拟宽限期内人工介入');
  const paused = coordinator.getSnapshot();
  assert.equal(paused.status, COORDINATOR_STATUS.PAUSED);
  const logicalAtPause = paused.logicalMs;

  const advancedDuringPause = await clock.advance(7000);
  assert.equal(advancedDuringPause, false);
  assert.equal(coordinator.getSnapshot().logicalMs, logicalAtPause);

  coordinator.resume();
  assert.equal(coordinator.getSnapshot().status, COORDINATOR_STATUS.DRAINING);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await clock.advance(5000);
  const snapshot = await runPromise;
  assert.equal(snapshot.status, COORDINATOR_STATUS.TERMINATED);
  assert.equal(snapshot.plan.safeTerminateAtMs, 8000);
  assert.equal(snapshot.logicalMs, 8000);
  assert.equal(clock.now(), 8000);

  const graceEvents = snapshot.timeline.filter(
    (event) => event.type === 'phase_entered' && event.phaseId === PHASE.TERMINATION_GRACE,
  );
  assert.equal(graceEvents.length, 1);
});

test('非法状态转换会被拒绝', async () => {
  const clock = new VirtualClock(0);
  const { coordinator } = makeCoordinator(baseConfig, { clock });
  assert.throws(() => coordinator.resume(), DrainError);
  assert.throws(() => coordinator.pause(), DrainError);

  const runPromise = coordinator.run();
  await clock.advance(20_000);
  await runPromise;
  assert.throws(() => coordinator.pause(), DrainError);
});
