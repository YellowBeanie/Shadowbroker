// Reconnect scheduling for the AIS WebSocket proxy.
//
// Extracted from ais_proxy.js after the 2026-07-20 reconnect storm: the
// upstream cert expired and every failed cycle scheduled TWO parallel
// reconnect loops (the failed probe socket's close handler plus the
// replacement socket's), doubling the number of concurrent connect loops
// each cycle. Combined with a fixed 5s retry this produced millions of
// connection attempts and log lines per hour.
//
// This scheduler enforces:
//   * single-flight — no matter how many handlers request a reconnect,
//     at most one connect() is ever pending;
//   * exponential backoff with jitter, capped at maxDelayMs;
//   * an explicit reset() so callers only clear the backoff after a
//     connection proved stable (not on every flapping open).

'use strict';

class ReconnectScheduler {
    constructor({
        baseDelayMs = 5000,
        maxDelayMs = 600000,
        jitterRatio = 0.2,
        random = Math.random,
        setTimeoutFn = setTimeout,
        clearTimeoutFn = clearTimeout,
    } = {}) {
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.jitterRatio = jitterRatio;
        this.random = random;
        this.setTimeoutFn = setTimeoutFn;
        this.clearTimeoutFn = clearTimeoutFn;
        this.timer = null;
        this.attempts = 0;
    }

    // Schedule connectFn after the current backoff delay. Returns the delay
    // in ms, or null when a reconnect is already pending (single-flight:
    // concurrent close/error handlers collapse into one attempt).
    schedule(connectFn) {
        if (this.timer !== null) return null;
        const expo = Math.min(this.baseDelayMs * 2 ** this.attempts, this.maxDelayMs);
        const delay = Math.round(expo + expo * this.jitterRatio * this.random());
        this.attempts += 1;
        this.timer = this.setTimeoutFn(() => {
            this.timer = null;
            connectFn();
        }, delay);
        return delay;
    }

    // Clear the backoff. Call only once a connection proved stable,
    // otherwise a flapping upstream resets itself back to the base delay.
    reset() {
        this.attempts = 0;
    }

    cancel() {
        if (this.timer !== null) {
            this.clearTimeoutFn(this.timer);
            this.timer = null;
        }
    }

    get pending() {
        return this.timer !== null;
    }
}

module.exports = { ReconnectScheduler };
