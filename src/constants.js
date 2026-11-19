export const PHASE = Object.freeze({
  STOP_ACCEPTING: 'stop_accepting_new_requests',
  WAIT_PROPAGATION: 'wait_load_balancer_propagation',
  CLOSE_LISTENER: 'close_listener',
  DRAIN_CONNECTIONS: 'drain_connections',
  FORCE_CLOSE: 'force_close_connections',
  TERMINATION_GRACE: 'termination_grace',
});

export const PHASE_STATUS = Object.freeze({
  PENDING: 'pending',
  ACTIVE: 'active',
  PAUSED: 'paused',
  COMPLETED: 'completed',
  FAILED: 'failed',
});

export const CONNECTION_KIND = Object.freeze({
  STATELESS: 'stateless',
  STATEFUL: 'stateful',
  LONG_LIVED: 'long_lived',
});

export const CONNECTION_STATE = Object.freeze({
  ACTIVE: 'active',
  RETRYING: 'retrying',
  CLOSED: 'closed',
});

export const COORDINATOR_STATUS = Object.freeze({
  READY: 'ready',
  DRAINING: 'draining',
  PAUSED: 'paused',
  TERMINATED: 'terminated',
});
