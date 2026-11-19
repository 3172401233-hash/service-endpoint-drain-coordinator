import { DrainError } from './errors.js';
import { PHASE, CONNECTION_KIND, CONNECTION_STATE } from './constants.js';

function assertNonNegativeNumber(value, name) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new DrainError(`${name} 必须是非负有限数字，实际收到 ${String(value)}`, {
      code: 'INVALID_CONFIG',
    });
  }
}

function normalizeRegion(region, globalHealthCheckIntervalMs, index) {
  if (!region || typeof region !== 'object') {
    throw new DrainError(`第 ${index} 个区域配置无效`, { code: 'INVALID_CONFIG' });
  }
  const regionId = region.id ?? `region-${index}`;
  const propagationDelayMs = region.propagationDelayMs ?? 0;
  const healthCheckIntervalMs = region.healthCheckIntervalMs ?? globalHealthCheckIntervalMs;
  assertNonNegativeNumber(propagationDelayMs, `regions[${index}].propagationDelayMs`);
  assertNonNegativeNumber(healthCheckIntervalMs, `regions[${index}].healthCheckIntervalMs`);
  return {
    regionId: String(regionId),
    propagationDelayMs,
    healthCheckIntervalMs,
    effectiveDelayMs: healthCheckIntervalMs + propagationDelayMs,
  };
}

function normalizeConnection(connection, t0Ms, listenerCloseAtMs) {
  if (!connection || typeof connection !== 'object') {
    throw new DrainError('连接配置必须是对象', { code: 'INVALID_CONNECTION' });
  }
  const id = String(connection.id ?? '');
  if (!id) {
    throw new DrainError('连接缺少 id', { code: 'INVALID_CONNECTION' });
  }
  const kind = connection.kind ?? CONNECTION_KIND.STATELESS;
  if (!Object.values(CONNECTION_KIND).includes(kind)) {
    throw new DrainError(`连接 ${id} 的 kind 无效: ${String(kind)}`, {
      code: 'INVALID_CONNECTION',
    });
  }
  const state = connection.state ?? CONNECTION_STATE.ACTIVE;
  if (!Object.values(CONNECTION_STATE).includes(state)) {
    throw new DrainError(`连接 ${id} 的 state 无效: ${String(state)}`, {
      code: 'INVALID_CONNECTION',
    });
  }

  const longLived = connection.longLived ?? kind === CONNECTION_KIND.LONG_LIVED;
  const retryable = connection.retryable ?? kind !== CONNECTION_KIND.STATEFUL;
  const observedAtMs = connection.observedAtMs ?? t0Ms;

  let requestDeadlineOffsetMs = null;
  if (connection.requestDeadlineAtMs !== undefined) {
    requestDeadlineOffsetMs = connection.requestDeadlineAtMs - t0Ms;
  } else if (connection.requestDeadlineInMs !== undefined) {
    requestDeadlineOffsetMs = connection.requestDeadlineInMs;
  }
  if (requestDeadlineOffsetMs !== null) {
    assertNonNegativeNumber(requestDeadlineOffsetMs, `连接 ${id} 的请求 deadline`);
  }

  let naturalEndMs;
  if (requestDeadlineOffsetMs !== null) {
    naturalEndMs = requestDeadlineOffsetMs;
  } else if (retryable && !longLived) {
    naturalEndMs = listenerCloseAtMs;
  } else {
    naturalEndMs = Number.POSITIVE_INFINITY;
  }

  return {
    id,
    kind,
    state,
    longLived,
    retryable,
    retryCount: connection.retryCount ?? 0,
    observedAtMs,
    requestDeadlineOffsetMs,
    naturalEndMs,
  };
}

/**
 * 生成排空协调计划。纯函数：不执行任何摘除、关监听或杀连接动作。
 * 所有时间戳字段均为相对排空开始时刻（now）的逻辑毫秒数，
 * 墙钟时间通过 t0Ms + 偏移量换算，暂停期间逻辑时钟不前进。
 */
export function buildDrainPlan(options = {}) {
  const {
    now = 0,
    healthCheckIntervalMs = 0,
    regions = [],
    connections = [],
    maxDrainTimeMs,
    terminationGraceMs = 0,
  } = options;

  assertNonNegativeNumber(healthCheckIntervalMs, 'healthCheckIntervalMs');
  assertNonNegativeNumber(maxDrainTimeMs, 'maxDrainTimeMs');
  assertNonNegativeNumber(terminationGraceMs, 'terminationGraceMs');

  if (!Array.isArray(regions) || regions.length === 0) {
    throw new DrainError('至少需要一个区域的传播延迟配置', { code: 'INVALID_CONFIG' });
  }

  const normalizedRegions = regions.map((region, index) =>
    normalizeRegion(region, healthCheckIntervalMs, index),
  );
  const effectivePropagationDelayMs = Math.max(
    ...normalizedRegions.map((region) => region.effectiveDelayMs),
  );

  if (maxDrainTimeMs < effectivePropagationDelayMs) {
    throw new DrainError(
      `maxDrainTimeMs(${maxDrainTimeMs}) 不能短于最长区域传播延迟(${effectivePropagationDelayMs})，` +
        '否则在负载均衡器完成摘除前就会触发强制终止',
      { code: 'INVALID_CONFIG' },
    );
  }

  const t0Ms = now;
  const listenerCloseAtMs = effectivePropagationDelayMs;

  const normalizedConnections = connections.map((connection) =>
    normalizeConnection(connection, t0Ms, listenerCloseAtMs),
  );

  const plannedConnections = normalizedConnections.map((connection) => {
    const forceEndMs = maxDrainTimeMs;
    const effectiveEndMs = Number.isFinite(connection.naturalEndMs)
      ? Math.min(connection.naturalEndMs, forceEndMs)
      : forceEndMs;
    return {
      ...connection,
      forceEndMs,
      effectiveEndMs,
      forceClosed: connection.naturalEndMs > forceEndMs,
      releasedAtListenerClose:
        connection.naturalEndMs === listenerCloseAtMs && connection.retryable,
    };
  });

  const forcedConnections = plannedConnections.filter(
    (connection) =>
      connection.forceClosed && connection.state !== CONNECTION_STATE.CLOSED,
  );
  const activeAtDrainStart = plannedConnections.filter(
    (connection) => connection.state !== CONNECTION_STATE.CLOSED,
  );
  const drainCompleteAtMs =
    activeAtDrainStart.length === 0
      ? listenerCloseAtMs
      : Math.max(
          listenerCloseAtMs,
          ...activeAtDrainStart.map((connection) => connection.effectiveEndMs),
        );
  const graceStartAtMs = drainCompleteAtMs;
  const safeTerminateAtMs = graceStartAtMs + terminationGraceMs;

  const phases = [
    {
      id: PHASE.STOP_ACCEPTING,
      startMs: 0,
      endMs: 0,
      actionRequired: true,
      description: '标记端点为 draining 并从服务发现摘除，停止获得新请求，监听保持可用',
    },
    {
      id: PHASE.WAIT_PROPAGATION,
      startMs: 0,
      endMs: effectivePropagationDelayMs,
      actionRequired: false,
      regions: normalizedRegions,
      description: '等待各区域健康检查与负载均衡器规则传播全部完成',
    },
    {
      id: PHASE.CLOSE_LISTENER,
      startMs: listenerCloseAtMs,
      endMs: listenerCloseAtMs,
      actionRequired: true,
      guard: {
        requiresPhaseCompleted: PHASE.WAIT_PROPAGATION,
        reason: '所有区域确认摘除前禁止关闭监听，避免新连接被拒绝后重试放大',
      },
      description: '所有区域摘除确认后关闭监听器，不再接受新 TCP 连接',
    },
    {
      id: PHASE.DRAIN_CONNECTIONS,
      startMs: listenerCloseAtMs,
      endMs: drainCompleteAtMs,
      actionRequired: false,
      description: '等待在途请求按 deadline 结束；无 deadline 的长连接由强制边界兜底',
    },
  ];

  if (forcedConnections.length > 0) {
    phases.push({
      id: PHASE.FORCE_CLOSE,
      startMs: drainCompleteAtMs,
      endMs: drainCompleteAtMs,
      actionRequired: true,
      connectionIds: forcedConnections.map((connection) => connection.id),
      description: '达到 maxDrainTimeMs 强制边界，关闭仍未结束的连接',
    });
  }

  phases.push({
    id: PHASE.TERMINATION_GRACE,
    startMs: graceStartAtMs,
    endMs: safeTerminateAtMs,
    durationMs: terminationGraceMs,
    actionRequired: false,
    skippable: false,
    description: '终止宽限期：等待对端 FIN、重试落地与进程清理，任何暂停都不会跳过该阶段',
  });

  const events = [
    { atMs: 0, type: 'drain_started' },
    { atMs: 0, type: 'phase_entered', phaseId: PHASE.STOP_ACCEPTING },
    ...normalizedRegions.map((region) => ({
      atMs: region.effectiveDelayMs,
      type: 'region_removed',
      regionId: region.regionId,
    })),
    { atMs: listenerCloseAtMs, type: 'phase_entered', phaseId: PHASE.CLOSE_LISTENER },
    ...forcedConnections.map((connection) => ({
      atMs: drainCompleteAtMs,
      type: 'force_close_connection',
      connectionId: connection.id,
    })),
    { atMs: safeTerminateAtMs, type: 'safe_to_terminate' },
  ].sort((left, right) => left.atMs - right.atMs);

  function projectRemainingAt(atMs) {
    assertNonNegativeNumber(atMs, 'projectRemainingAt 的时间参数');
    return plannedConnections.filter(
      (connection) =>
        connection.state !== CONNECTION_STATE.CLOSED && connection.effectiveEndMs > atMs,
    );
  }

  function getRemainingConnectionsAt(atMs) {
    const remaining = projectRemainingAt(atMs);
    const summary = {
      total: remaining.length,
      stateless: 0,
      stateful: 0,
      longLived: 0,
      retrying: 0,
      willForceClose: 0,
      connectionIds: remaining.map((connection) => connection.id),
    };
    const summaryKeyByKind = {
      [CONNECTION_KIND.STATELESS]: 'stateless',
      [CONNECTION_KIND.STATEFUL]: 'stateful',
      [CONNECTION_KIND.LONG_LIVED]: 'longLived',
    };
    for (const connection of remaining) {
      summary[summaryKeyByKind[connection.kind]] += 1;
      if (connection.state === CONNECTION_STATE.RETRYING) {
        summary.retrying += 1;
      }
      if (connection.forceClosed) {
        summary.willForceClose += 1;
      }
    }
    return summary;
  }

  return {
    t0Ms,
    effectivePropagationDelayMs,
    maxDrainTimeMs,
    terminationGraceMs,
    listenerCloseAtMs,
    drainCompleteAtMs,
    graceStartAtMs,
    safeTerminateAtMs,
    safeTerminateWallMs: t0Ms + safeTerminateAtMs,
    phases,
    events,
    regions: normalizedRegions,
    connections: plannedConnections,
    forcedConnectionIds: forcedConnections.map((connection) => connection.id),
    getRemainingConnectionsAt,
  };
}
