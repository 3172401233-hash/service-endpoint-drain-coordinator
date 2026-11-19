import test from 'node:test';
import assert from 'node:assert/strict';
import { VirtualClock } from '../src/clock.js';
import { buildDrainPlan, DrainError } from '../src/index.js';
import { baseConfig } from './helpers.js';

test('传播延迟：监听关闭时间 = 健康检查间隔 + 最长区域传播延迟', () => {
  const plan = buildDrainPlan({
    ...baseConfig,
    healthCheckIntervalMs: 1000,
    regions: [
      { id: 'region-a', propagationDelayMs: 500 },
      { id: 'region-b', propagationDelayMs: 4000 },
      { id: 'region-c', propagationDelayMs: 1500, healthCheckIntervalMs: 2000 },
    ],
  });

  assert.equal(plan.effectivePropagationDelayMs, 5000);
  assert.equal(plan.listenerCloseAtMs, 5000);
  assert.deepEqual(
    plan.regions.map((region) => [region.regionId, region.effectiveDelayMs]),
    [
      ['region-a', 1500],
      ['region-b', 5000],
      ['region-c', 3500],
    ],
  );
  assert.deepEqual(
    plan.events
      .filter((event) => event.type === 'region_removed')
      .map((event) => [event.regionId, event.atMs]),
    [
      ['region-a', 1500],
      ['region-c', 3500],
      ['region-b', 5000],
    ],
  );
});

test('传播延迟配置非法时直接拒绝生成计划', () => {
  assert.throws(
    () =>
      buildDrainPlan({
        ...baseConfig,
        maxDrainTimeMs: 2000,
        regions: [{ id: 'r', propagationDelayMs: 5000 }],
      }),
    (error) => error instanceof DrainError && error.code === 'INVALID_CONFIG',
  );
});

test('VirtualClock 推进期间暂停不会消耗逻辑时间', async () => {
  const clock = new VirtualClock(0);
  const sleep = clock.sleep(1000);
  await clock.advance(1000);
  await sleep;
  assert.equal(clock.now(), 1000);
});
