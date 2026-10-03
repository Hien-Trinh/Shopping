"""The group-commit appender, driven with a fake commit: no Delta table (docs/specs/step-4c.md)."""

import asyncio
import threading

from catalog.landing import Appender


class Commits:
    """A fake commit: records each group it is given and returns its version."""

    def __init__(self):
        self.groups = []

    def __call__(self, entries):
        self.groups.append(list(entries))
        return len(self.groups)


def run(scenario, commit, **options):
    """Runs `scenario(appender)` with the appender's loop going; a hang fails fast."""

    async def main():
        appender = Appender(commit, **options)
        loop = asyncio.create_task(appender.run())
        try:
            return await asyncio.wait_for(scenario(appender), 5)
        finally:
            loop.cancel()

    return asyncio.run(main())


async def until(done):
    """Yields to the loop's other tasks until `done()`: ordering by awaits, never by wall clock."""
    while not done():
        await asyncio.sleep(0)


def test_requests_within_the_window_share_one_commit_in_arrival_order():
    commits = Commits()

    async def scenario(appender):
        first = asyncio.create_task(appender.submit(["a0", "a1"]))
        await asyncio.sleep(0)  # `first` is queued
        await until(appender.queue.empty)  # and the appender has picked it up: it is waiting
        second = asyncio.create_task(appender.submit(["b0"]))
        return await asyncio.gather(first, second)

    assert run(scenario, commits, window=0.5) == [1, 1]
    assert commits.groups == [["a0", "a1", "b0"]]


def test_a_request_after_the_window_goes_into_the_next_commit():
    commits = Commits()

    async def scenario(appender):
        return [await appender.submit(["a0"]), await appender.submit(["b0"])]

    assert run(scenario, commits, window=0.01) == [1, 2]
    assert commits.groups == [["a0"], ["b0"]]


def test_the_window_counts_from_when_the_oldest_request_arrived():
    # It arrived at 0 and the appender looks at 1000: a 600 s window has long passed, so the commit
    # is due at once (as for requests that queued behind a slow commit). Waiting a full window from
    # pickup instead would hang past run()'s 5 s limit.
    times = iter([0, 1000])
    commits = Commits()

    async def scenario(appender):
        return await appender.submit(["a0"])

    assert run(scenario, commits, window=600, clock=lambda: next(times)) == 1


def test_a_failed_commit_fails_every_request_in_it_and_the_appender_goes_on():
    groups = []

    def commit(entries):
        groups.append(list(entries))
        if len(groups) == 1:
            raise OSError(28, "No space left on device")
        return 7

    async def scenario(appender):
        both = [appender.submit(["a0"]), appender.submit(["b0"])]
        failed = await asyncio.gather(*both, return_exceptions=True)
        return failed, await appender.submit(["c0"])

    failed, version = run(scenario, commit, window=0.01)
    assert [type(e) for e in failed] == [OSError, OSError]
    assert (version, groups) == (7, [["a0", "b0"], ["c0"]])


def test_a_request_cancelled_while_waiting_still_lands_and_breaks_no_other():
    commits = Commits()

    async def scenario(appender):
        gone = asyncio.create_task(appender.submit(["a0"]))
        stays = asyncio.create_task(appender.submit(["b0"]))
        await asyncio.sleep(0)  # both are queued
        gone.cancel()  # its client disconnected; its rows are already queued
        return await stays

    assert run(scenario, commits, window=0.1) == 1
    assert commits.groups == [["a0", "b0"]]  # it got no 202, so its Merchant retries (A3)


def test_the_commit_runs_off_the_event_loop():
    # The commit only finishes once the loop sets `release`, which it can do only if the commit
    # isn't blocking it: an inline commit times out instead.
    started, release = threading.Event(), threading.Event()

    def commit(entries):
        started.set()
        if not release.wait(2):
            raise TimeoutError("the commit blocked the event loop")
        return 1

    async def scenario(appender):
        waiting = asyncio.create_task(appender.submit(["a0"]))
        await asyncio.to_thread(started.wait, 2)
        release.set()
        return await waiting

    assert run(scenario, commit, window=0) == 1
