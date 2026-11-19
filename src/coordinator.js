import { buildDrainPlan } from './plan.js';
import { DrainError } from './errors.js';
import { SystemClock } from './clock.js';
import { PHASE, PHASE_STATUS, COORDINATOR_STATUS, CONNECTION_STATE } from './constants.js';

const ACTION_PHASES = new Set([
  PHASE.STOP_ACCEPTING,
  PHASE.CLOSE_LISTENER,
  PHASE.FORCE_CLOSE,
]);

export class DrainCoordinator {
  #config;

  #clock;

  #actions;

  #plan = null;

  #status = COORDINATOR_STATUS.READY;

  #phaseStatuses = new Map();

  #timeline = [];

  #pauseAccumMs = 0;

  #startedWallMs = null;

  #pauseStartedWallMs = null;

  #lastFailure = null;

  #terminatedAtLogicalMs = null;

  #currentSleepAbort = null;

  #runEpoch = 0;

  #finalResolve = null;

  #finalPromise = null;

  constructor(config, { clock = new SystemClock(), actions = {} } = {}) {
    if (!config || typeof config !== 'object') {
      throw new DrainError('缺少排空配置', { code: 'INVALID_CONFIG' });
    }
    if (config.now !== undefined) {
      throw new DrainError('配置中的 now 由时钟提供，请勿手动传入', {
        code: 'INVALID_CONFIG',
      });
    }
    this.#config = config;
    this.#clock = clock;
    this.#actions = actions;
  }

  #rebuildPlan(extraConnections = []) {
    const baseConnections = (this.#config.connections ?? []).map((connection) => ({
      ...connection,
      state: this.#connectionStates.get(String(connection.id)) ?? connection.state,
    }));
    const merged = [...baseConnections, ...extraConnections];
    this.#plan = buildDrainPlan({
      ...this.#config,
      now: this.#clock.now(),
      connections: merged,
    });
    for (const phase of this.#plan.phases) {
      if (!this.#phaseStatuses.has(phase.id)) {
        this.#phaseStatuses.set(phase.id, PHASE_STATUS.PENDING);
      }
    }
  }

  #connectionStates = new Map();

  #logicalNow() {
    if (!this.#plan) {
      return 0;
    }
    return this.#clock.now() - this.#pauseAccumMs - this.#startedWallMs;
  }

  #record(type, details = {}) {
    this.#timeline.push({
      atLogicalMs: this.#logicalNow(),
      atWallMs: this.#clock.now(),
      type,
      ...details,
    });
  }

  async #sleepLogical(targetLogicalMs, epoch) {
    let currentTarget = targetLogicalMs;
    while (this.#status === COORDINATOR_STATUS.DRAINING && epoch === this.#runEpoch) {
      const remaining = currentTarget - this.#logicalNow();
      if (remaining <= 0) {
        return false;
      }
      const sleep = this.#clock.sleep(remaining);
      this.#currentSleepAbort = sleep.abort;
      try {
        await sleep;
        return false;
      } catch (error) {
        if (error?.message === 'sleep aborted') {
          if (epoch !== this.#runEpoch) {
            return false;
          }
          if (this.#status === COORDINATOR_STATUS.PAUSED) {
            return true;
          }
          return false;
        }
        throw error;
      } finally {
        this.#currentSleepAbort = null;
      }
    }
    return this.#status === COORDINATOR_STATUS.PAUSED;
  }

  #fail(phaseId, error) {
    this.#status = COORDINATOR_STATUS.PAUSED;
    this.#phaseStatuses.set(phaseId, PHASE_STATUS.FAILED);
    const failure = {
      phaseId,
      error,
      atLogicalMs: this.#logicalNow(),
      atWallMs: this.#clock.now(),
    };
    this.#lastFailure = failure;
    this.#record('phase_failed', { phaseId, message: error.message });
  }

  async #runAction(phase) {
    const handler = this.#actions[phase.id];
    if (typeof handler !== 'function') {
      throw new DrainError(`阶段 ${phase.id} 需要注入执行动作，但未提供`, {
        code: 'MISSING_ACTION',
        phaseId: phase.id,
      });
    }
    const context = this.getSnapshot();
    await handler(phase, context);
  }

  async run() {
    if (this.#status === COORDINATOR_STATUS.TERMINATED) {
      return this.getSnapshot();
    }
    if (this.#status !== COORDINATOR_STATUS.READY && this.#status !== COORDINATOR_STATUS.DRAINING) {
      throw new DrainError(`协调器当前状态 ${this.#status} 无法运行`, { code: 'INVALID_STATE' });
    }
    if (!this.#plan) {
      this.#rebuildPlan();
      this.#startedWallMs = this.#clock.now();
      this.#record('drain_started', {
        safeTerminateAtLogicalMs: this.#plan.safeTerminateAtMs,
      });
    }
    this.#status = COORDINATOR_STATUS.DRAINING;
    const epoch = ++this.#runEpoch;
    const runPromise = this.#executePhases(epoch);
    return runPromise;
  }

  #whenFinal() {
    if (!this.#finalPromise) {
      this.#finalPromise = new Promise((resolve) => {
        this.#finalResolve = resolve;
      });
    }
    return this.#finalPromise;
  }

  async #executePhases(epoch) {
    for (const phase of this.#plan.phases) {
      if (epoch !== this.#runEpoch) {
        return this.#whenFinal();
      }
      const phaseStatus = this.#phaseStatuses.get(phase.id);
      if (phaseStatus === PHASE_STATUS.COMPLETED) {
        continue;
      }
      if (phaseStatus !== PHASE_STATUS.PAUSED) {
        this.#phaseStatuses.set(phase.id, PHASE_STATUS.ACTIVE);
        this.#record('phase_entered', { phaseId: phase.id });
      }

      if (phase.guard) {
        const guardStatus = this.#phaseStatuses.get(phase.guard.requiresPhaseCompleted);
        if (guardStatus !== PHASE_STATUS.COMPLETED) {
          this.#fail(
            phase.id,
            new DrainError(phase.guard.reason, {
              code: 'GUARD_VIOLATION',
              phaseId: phase.id,
            }),
          );
          return this.getSnapshot();
        }
      }

      if (ACTION_PHASES.has(phase.id)) {
        try {
          await this.#runAction(phase);
        } catch (error) {
          this.#fail(phase.id, error);
          return this.getSnapshot();
        }
      } else if (phase.endMs > this.#logicalNow()) {
        const paused = await this.#sleepLogical(phase.endMs, epoch);
        if (epoch !== this.#runEpoch) {
          return this.#whenFinal();
        }
        if (paused) {
          this.#record('paused_during_phase', { phaseId: phase.id });
          return this.#whenFinal();
        }
      }

      this.#phaseStatuses.set(phase.id, PHASE_STATUS.COMPLETED);
      this.#record('phase_completed', { phaseId: phase.id });
    }

    if (this.#logicalNow() < this.#plan.safeTerminateAtMs) {
      this.#fail(
        PHASE.TERMINATION_GRACE,
        new DrainError('内部错误：终止宽限期未完整经过，禁止进入 terminated', {
          code: 'GRACE_SKIPPED',
          phaseId: PHASE.TERMINATION_GRACE,
        }),
      );
      return this.getSnapshot();
    }

    this.#status = COORDINATOR_STATUS.TERMINATED;
    this.#terminatedAtLogicalMs = this.#logicalNow();
    this.#record('terminated', {
      atLogicalMs: this.#terminatedAtLogicalMs,
      safeTerminateAtLogicalMs: this.#plan.safeTerminateAtMs,
    });
    const finalSnapshot = this.getSnapshot();
    if (this.#finalResolve) {
      this.#finalResolve(finalSnapshot);
    }
    return finalSnapshot;
  }

  pause(reason = 'manual pause') {
    if (this.#status !== COORDINATOR_STATUS.DRAINING) {
      throw new DrainError(`只有 draining 状态可以暂停，当前为 ${this.#status}`, {
        code: 'INVALID_STATE',
      });
    }
    const activePhase = [...this.#phaseStatuses].find(
      ([, status]) => status === PHASE_STATUS.ACTIVE,
    )?.[0];
    if (activePhase !== undefined) {
      this.#phaseStatuses.set(activePhase, PHASE_STATUS.PAUSED);
    }
    this.#status = COORDINATOR_STATUS.PAUSED;
    this.#pauseStartedWallMs = this.#clock.now();
    this.#record('paused', { reason });
    if (this.#currentSleepAbort) {
      this.#currentSleepAbort();
    }
  }

  resume() {
    if (this.#status !== COORDINATOR_STATUS.PAUSED) {
      throw new DrainError(`只有 paused 状态可以恢复，当前为 ${this.#status}`, {
        code: 'INVALID_STATE',
      });
    }
    if (this.#pauseStartedWallMs !== null) {
      this.#pauseAccumMs += this.#clock.now() - this.#pauseStartedWallMs;
      this.#pauseStartedWallMs = null;
    }
    for (const [phaseId, status] of this.#phaseStatuses) {
      if (status === PHASE_STATUS.ACTIVE) {
        this.#phaseStatuses.set(phaseId, PHASE_STATUS.PAUSED);
      }
    }
    this.#status = COORDINATOR_STATUS.DRAINING;
    this.#record('resumed');
    this.run().catch((error) => this.#fail(PHASE.DRAIN_CONNECTIONS, error));
  }

  async retryFailedPhase() {
    if (this.#status !== COORDINATOR_STATUS.PAUSED || !this.#lastFailure) {
      throw new DrainError('没有等待重试的失败阶段', { code: 'INVALID_STATE' });
    }
    const failedPhaseId = this.#lastFailure.phaseId;
    this.#phaseStatuses.set(failedPhaseId, PHASE_STATUS.PENDING);
    this.#lastFailure = null;
    this.resume();
    return this.#whenFinal();
  }

  /**
   * 传播窗口内到达的“最后时刻新请求”：
   * 监听尚未关闭时应被接受，并纳入当前排空计划重新计算。
   */
  adoptInFlightConnection(connection) {
    if (!this.#plan) {
      this.#rebuildPlan([connection]);
      return this.getSnapshot();
    }
    if (this.#status === COORDINATOR_STATUS.TERMINATED) {
      throw new DrainError('实例已终止，无法再接纳连接', { code: 'INVALID_STATE' });
    }
    if (this.#logicalNow() >= this.#plan.listenerCloseAtMs) {
      throw new DrainError(
        '监听器已关闭，新连接必须由负载均衡器重试到其他实例（返回 503）',
        { code: 'LISTENER_CLOSED' },
      );
    }
    const existingIds = new Set(this.#plan.connections.map((item) => item.id));
    if (existingIds.has(String(connection.id))) {
      throw new DrainError(`连接 ${connection.id} 已存在`, { code: 'DUPLICATE_CONNECTION' });
    }
    this.#config = {
      ...this.#config,
      connections: [...(this.#config.connections ?? []), connection],
    };
    this.#refreshPlan();
    this.#record('connection_adopted', {
      connectionId: connection.id,
      atLogicalMs: this.#logicalNow(),
    });
    return this.getSnapshot();
  }

  #refreshPlan() {
    const completedPhaseIds = new Set(
      [...this.#phaseStatuses]
        .filter(([, status]) => status === PHASE_STATUS.COMPLETED)
        .map(([phaseId]) => phaseId),
    );
    const shouldRerun = this.#status === COORDINATOR_STATUS.DRAINING;
    this.#runEpoch += 1;
    this.#rebuildPlan();
    for (const phaseId of completedPhaseIds) {
      this.#phaseStatuses.set(phaseId, PHASE_STATUS.COMPLETED);
    }
    if (this.#currentSleepAbort) {
      this.#currentSleepAbort();
    }
    if (shouldRerun) {
      queueMicrotask(() => {
        this.run().catch((error) => this.#fail(PHASE.DRAIN_CONNECTIONS, error));
      });
    }
  }

  updateConnectionState(connectionId, state) {
    const id = String(connectionId);
    if (!this.#plan) {
      throw new DrainError('排空尚未开始，无法更新连接状态', { code: 'INVALID_STATE' });
    }
    this.#connectionStates.set(id, state);
    this.#refreshPlan();
    this.#record('connection_state_updated', { connectionId: id, state });
  }

  finishConnection(connectionId) {
    this.updateConnectionState(connectionId, CONNECTION_STATE.CLOSED);
  }

  getRemainingConnections() {
    if (!this.#plan) {
      return {
        total: 0,
        stateless: 0,
        stateful: 0,
        longLived: 0,
        retrying: 0,
        willForceClose: 0,
        connectionIds: [],
      };
    }
    return this.#plan.getRemainingConnectionsAt(this.#logicalNow());
  }

  getSnapshot() {
    const plan = this.#plan;
    return {
      status: this.#status,
      logicalMs: this.#logicalNow(),
      wallMs: this.#clock.now(),
      lastFailure: this.#lastFailure,
      timeline: [...this.#timeline],
      phaseStatuses: new Map(this.#phaseStatuses),
      remainingConnections: plan ? this.getRemainingConnections() : null,
      plan: plan
        ? {
            effectivePropagationDelayMs: plan.effectivePropagationDelayMs,
            listenerCloseAtMs: plan.listenerCloseAtMs,
            drainCompleteAtMs: plan.drainCompleteAtMs,
            graceStartAtMs: plan.graceStartAtMs,
            safeTerminateAtMs: plan.safeTerminateAtMs,
            safeTerminateWallMs: plan.safeTerminateWallMs,
            maxDrainTimeMs: plan.maxDrainTimeMs,
            terminationGraceMs: plan.terminationGraceMs,
            phases: plan.phases,
            events: plan.events,
            regions: plan.regions,
            forcedConnectionIds: plan.forcedConnectionIds,
            connections: plan.connections,
          }
        : null,
    };
  }
}
