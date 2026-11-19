import { DrainCoordinator } from '../src/coordinator.js';
import { VirtualClock } from '../src/clock.js';
import { PHASE } from '../src/constants.js';

export function makeActions(overrides = {}) {
  const calls = [];
  const handlers = {
    async [PHASE.STOP_ACCEPTING](phase, snapshot) {
      calls.push({ phaseId: phase.id, at: snapshot.logicalMs });
    },
    async [PHASE.CLOSE_LISTENER](phase, snapshot) {
      calls.push({ phaseId: phase.id, at: snapshot.logicalMs });
    },
    async [PHASE.FORCE_CLOSE](phase, snapshot) {
      calls.push({
        phaseId: phase.id,
        at: snapshot.logicalMs,
        connectionIds: [...phase.connectionIds],
      });
    },
  };
  for (const [phaseId, handler] of Object.entries(overrides)) {
    handlers[phaseId] = async (phase, snapshot) => {
      calls.push({ phaseId, at: snapshot.logicalMs, failed: true });
      const error = handler(phase, snapshot);
      if (error) {
        throw error;
      }
    };
  }
  return {
    handlers,
    calls,
  };
}

export function makeCoordinator(config, { clock, handlers } = {}) {
  const virtualClock = clock ?? new VirtualClock();
  const { handlers: actionHandlers, calls } = makeActions(handlers);
  const coordinator = new DrainCoordinator(config, {
    clock: virtualClock,
    actions: actionHandlers,
  });
  return { coordinator, clock: virtualClock, calls };
}

export const baseConfig = {
  healthCheckIntervalMs: 1000,
  regions: [{ id: 'cn-east', propagationDelayMs: 2000 }],
  maxDrainTimeMs: 10000,
  terminationGraceMs: 3000,
  connections: [],
};
