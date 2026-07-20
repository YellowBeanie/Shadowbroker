// Regression tests for the AIS proxy reconnect scheduler.
//
// Incident 2026-07-20: an expired upstream cert made every failed connect
// cycle schedule two parallel reconnect loops (probe socket close handler +
// replacement socket close handler), doubling concurrent loops each cycle.
// These tests pin the single-flight and backoff behaviour that prevents it.
//
// Run: node --test backend/

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { ReconnectScheduler } = require('./ais_reconnect');

function manualTimers() {
    // Deterministic stand-in for setTimeout/clearTimeout: callbacks fire
    // only when flush() is called, and scheduled delays are recorded.
    const queue = new Map();
    let nextId = 1;
    return {
        delays: [],
        setTimeoutFn(fn, delay) {
            const id = nextId++;
            queue.set(id, fn);
            this.delays.push(delay);
            return id;
        },
        clearTimeoutFn(id) {
            queue.delete(id);
        },
        flush() {
            const fns = [...queue.values()];
            queue.clear();
            fns.forEach((fn) => fn());
        },
        pendingCount() {
            return queue.size;
        },
    };
}

function makeScheduler(timers, overrides = {}) {
    return new ReconnectScheduler({
        baseDelayMs: 5000,
        maxDelayMs: 600000,
        jitterRatio: 0,        // deterministic delays for assertions
        setTimeoutFn: timers.setTimeoutFn.bind(timers),
        clearTimeoutFn: timers.clearTimeoutFn.bind(timers),
        ...overrides,
    });
}

test('single-flight: concurrent schedule() calls collapse into one connect', () => {
    const timers = manualTimers();
    const sched = makeScheduler(timers);
    let connects = 0;
    const connect = () => { connects += 1; };

    // Storm regression: the 2026-07-20 bug had N event handlers all
    // requesting a reconnect for the same outage. Only the first may win.
    const first = sched.schedule(connect);
    assert.equal(first, 5000);
    for (let i = 0; i < 100; i++) {
        assert.equal(sched.schedule(connect), null);
    }
    assert.equal(timers.pendingCount(), 1);

    timers.flush();
    assert.equal(connects, 1);
    assert.equal(sched.pending, false);
});

test('backoff doubles per attempt and caps at maxDelayMs', () => {
    const timers = manualTimers();
    const sched = makeScheduler(timers, { maxDelayMs: 60000 });
    const connect = () => {};

    const observed = [];
    for (let i = 0; i < 8; i++) {
        observed.push(sched.schedule(connect));
        timers.flush();
    }
    assert.deepEqual(observed, [5000, 10000, 20000, 40000, 60000, 60000, 60000, 60000]);
});

test('reset() returns the delay to base after a stable connection', () => {
    const timers = manualTimers();
    const sched = makeScheduler(timers);
    const connect = () => {};

    sched.schedule(connect);
    timers.flush();
    sched.schedule(connect);
    timers.flush();
    assert.equal(sched.schedule(connect), 20000);
    timers.flush();

    sched.reset();
    assert.equal(sched.schedule(connect), 5000);
});

test('jitter stays within [expo, expo * (1 + jitterRatio)]', () => {
    const timers = manualTimers();
    const low = makeScheduler(timers, { jitterRatio: 0.2, random: () => 0 });
    const high = makeScheduler(timers, { jitterRatio: 0.2, random: () => 1 });
    assert.equal(low.schedule(() => {}), 5000);
    assert.equal(high.schedule(() => {}), 6000);
});

test('cancel() clears the pending attempt without firing it', () => {
    const timers = manualTimers();
    const sched = makeScheduler(timers);
    let connects = 0;

    sched.schedule(() => { connects += 1; });
    sched.cancel();
    timers.flush();
    assert.equal(connects, 0);
    assert.equal(sched.pending, false);

    // After cancel, a new schedule is accepted again.
    assert.notEqual(sched.schedule(() => {}), null);
});
