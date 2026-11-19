export class SystemClock {
  now() {
    return Date.now();
  }

  sleep(ms) {
    let abort;
    const promise = new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, ms);
      if (typeof timer.unref === 'function') {
        timer.unref();
      }
      abort = () => {
        clearTimeout(timer);
        reject(new Error('sleep aborted'));
      };
    });
    promise.abort = abort;
    return promise;
  }
}

export class VirtualClock {
  #nowMs;

  #pending = [];

  #seq = 0;

  static #microtask() {
    return new Promise((resolve) => setTimeout(resolve, 0));
  }

  constructor(startMs = 0) {
    this.#nowMs = startMs;
  }

  now() {
    return this.#nowMs;
  }

  sleep(ms) {
    if (ms < 0) {
      throw new RangeError('sleep 时长不能为负数');
    }
    let abort;
    const promise = new Promise((resolve, reject) => {
      const entry = {
        runAt: this.#nowMs + ms,
        seq: this.#seq++,
        resolve,
        reject,
      };
      this.#pending.push(entry);
      abort = () => {
        const index = this.#pending.indexOf(entry);
        if (index !== -1) {
          this.#pending.splice(index, 1);
        }
        reject(new Error('sleep aborted'));
      };
    });
    promise.abort = abort;
    return promise;
  }

  async advance(ms) {
    if (ms < 0) {
      throw new RangeError('advance 时长不能为负数');
    }
    await VirtualClock.#microtask();
    const target = this.#nowMs + ms;
    while (this.#nowMs < target) {
      if (this.#pending.length === 0) {
        return false;
      }
      const next = this.#pending
        .filter((entry) => entry.runAt <= target)
        .sort((left, right) => left.runAt - right.runAt || left.seq - right.seq)[0];
      if (!next) {
        return false;
      }
      const jumpTo = Math.min(next.runAt, target);
      if (jumpTo > this.#nowMs) {
        this.#nowMs = jumpTo;
      }
      const due = this.#pending.filter((entry) => entry.runAt <= this.#nowMs);
      this.#pending = this.#pending.filter((entry) => entry.runAt > this.#nowMs);
      for (const entry of due.sort((left, right) => left.seq - right.seq)) {
        entry.resolve();
      }
      await VirtualClock.#microtask();
    }
    return true;
  }

  hasPendingTimers() {
    return this.#pending.length > 0;
  }

  pendingTimerCount() {
    return this.#pending.length;
  }

  abortAll(error) {
    const pending = this.#pending;
    this.#pending = [];
    for (const entry of pending) {
      entry.reject(error ?? new Error('virtual clock aborted'));
    }
  }
}
