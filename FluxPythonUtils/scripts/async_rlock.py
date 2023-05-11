import asyncio
import re
import functools

AsyncLock = asyncio.Lock


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


async def _cancel_and_wait(fut, loop):
    """Cancel the *fut* future or task and wait until it completes."""

    waiter = loop.create_future()
    cb = functools.partial(_release_waiter, waiter)
    fut.add_done_callback(cb)

    try:
        fut.cancel()
        # We cannot wait on *fut* directly to make
        # sure _cancel_and_wait itself is reliably cancellable.
        await waiter
    finally:
        fut.remove_done_callback(cb)


async def wait_for(fut, timeout):
    """Wait for the single Future or coroutine to complete, with timeout.
    Coroutine will be wrapped in Task.
    Returns result of the Future or coroutine.  When a timeout occurs,
    it cancels the task and raises TimeoutError.  To avoid the task
    cancellation, wrap it in shield().
    If the wait is cancelled, the task is also cancelled.
    This function is a coroutine.
    """
    loop = asyncio.get_running_loop()

    if timeout is None:
        return await fut

    if timeout <= 0:
        fut = asyncio.ensure_future(fut, loop=loop)

        if fut.done():
            return fut.result()

        await _cancel_and_wait(fut, loop=loop)
        try:
            return fut.result()
        except asyncio.CancelledError as exc:
            raise asyncio.TimeoutError() from exc

    waiter = loop.create_future()
    timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    cb = functools.partial(_release_waiter, waiter)

    fut = asyncio.ensure_future(fut, loop=loop)
    fut.add_done_callback(cb)

    try:
        # wait until the future completes or the timeout
        try:
            await waiter
        except asyncio.CancelledError:
            if fut.done():
                # [PATCH]
                # Applied patch to not swallow the outer cancellation.
                # See: https://github.com/python/cpython/pull/26097
                # and https://github.com/python/cpython/pull/28149

                # Even though the future we're waiting for is already done,
                # we should not swallow the cancellation.
                raise
                # [/PATCH]
            else:
                fut.remove_done_callback(cb)
                # We must ensure that the task is not running
                # after wait_for() returns.
                # See https://bugs.python.org/issue32751
                await _cancel_and_wait(fut, loop=loop)
                raise

        if fut.done():
            return fut.result()
        else:
            fut.remove_done_callback(cb)
            # We must ensure that the task is not running
            # after wait_for() returns.
            # See https://bugs.python.org/issue32751
            await _cancel_and_wait(fut, loop=loop)
            # In case task cancellation failed with some
            # exception, we should re-raise it
            # See https://bugs.python.org/issue40607
            try:
                return fut.result()
            except asyncio.CancelledError as exc:
                raise asyncio.TimeoutError() from exc
    finally:
        timeout_handle.cancel()


class AsyncRLock(asyncio.Lock):
    """Reentrant asyncio.lock
    Inspired by Python's RLock implementation
    .. warning::
        In async Python there are no threads. This implementation uses
        :meth:`asyncio.current_task()` to determine the owner of the lock. This
        means that the owner changes when using :meth:`asyncio.wait_for` or
        any other method that wraps the work in a new :class:`asyncio.Task`.
    """

    _WAITERS_RE = re.compile(r"(?:\W|^)waiters[:=](\d+)(?:\W|$)")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._owner = None
        self._count = 0

    def __repr__(self):
        res = object.__repr__(self)
        lock_repr = super().__repr__()
        extra = "locked" if self._count > 0 else "unlocked"
        extra += f" count={self._count}"
        waiters_match = self._WAITERS_RE.search(lock_repr)
        if waiters_match:
            extra += f" waiters={waiters_match.group(1)}"
        if self._owner:
            extra += f" owner={self._owner}"
        return f'<{res[1:-1]} [{extra}]>'

    def is_owner(self, task=None):
        if task is None:
            task = asyncio.current_task()
        return self._owner == task

    async def _acquire_non_blocking(self, me):
        if self.is_owner(task=me):
            self._count += 1
            return True
        acquire_coro = super().acquire()
        task = asyncio.ensure_future(acquire_coro)
        # yielding one cycle is as close to non-blocking as it gets
        # (at least without implementing the lock from the ground up)
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            # This is emulating non-blocking. There is no cancelling this!
            # Still, we don't want to silently swallow the cancellation.
            # Hence, we flag this task as cancelled again, so that the next
            # `await` will raise the CancelledError.
            asyncio.current_task().cancel()
        if task.done() and task.exception() is None:
            self._owner = me
            self._count = 1
            return True
        task.cancel()
        return False

    async def _acquire(self, me):
        if self.is_owner(task=me):
            self._count += 1
            return
        await super().acquire()
        self._owner = me
        self._count = 1

    async def acquire(self, blocking=True, timeout=-1):
        """Acquire the lock."""
        me = asyncio.current_task()
        if timeout < 0 and timeout != -1:
            raise ValueError("timeout value must be positive")
        if not blocking and timeout != -1:
            raise ValueError("can't specify a timeout for a non-blocking call")
        if not blocking:
            return await self._acquire_non_blocking(me)
        if blocking and timeout == -1:
            await self._acquire(me)
            return True
        try:
            fut = asyncio.ensure_future(self._acquire(me))
            try:
                await wait_for(fut, timeout)
            except asyncio.CancelledError:
                already_finished = not fut.cancel()
                if already_finished:
                    # Too late to cancel the acquisition.
                    # This can only happen in Python 3.7's asyncio
                    # as well as in our wait_for shim.
                    self._release(me)
                raise
            return True
        except asyncio.TimeoutError:
            return False

    __aenter__ = acquire

    def _release(self, me):
        if not self.is_owner(task=me):
            if self._owner is None:
                raise RuntimeError("Cannot release un-acquired lock.")
            raise RuntimeError("Cannot release foreign lock.")
        self._count -= 1
        if not self._count:
            self._owner = None
            super().release()

    def release(self):
        """Release the lock"""
        me = asyncio.current_task()
        return self._release(me)

    async def __aexit__(self, t, v, tb):
        self.release()
