export class DrainError extends Error {
  constructor(message, { code, phaseId, cause } = {}) {
    super(message, cause ? { cause } : undefined);
    this.name = 'DrainError';
    this.code = code ?? 'DRAIN_ERROR';
    if (phaseId !== undefined) {
      this.phaseId = phaseId;
    }
  }
}
