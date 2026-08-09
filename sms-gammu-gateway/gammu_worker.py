"""Synchronous facade over gammu.asyncworker.GammuAsyncWorker.

python-gammu's StateMachine is not thread safe, and a call blocked inside libGammu
cannot be cancelled from Python. The previous design submitted every operation to a
throwaway ThreadPoolExecutor and, on timeout, called shutdown(wait=False): the timed
out call kept running against the serial port while the lock it was nominally
protected by had already been released, so the next operation ran concurrently with
it. That is a data race on both the port and libGammu's internal state.

GammuAsyncWorker owns the StateMachine on a single thread for the process lifetime
and feeds it from a queue. A stalled call therefore delays queued work instead of
running alongside it. Nothing else ever touches the StateMachine.

The add-on is threaded rather than asyncio, so the worker runs on a private event
loop in a dedicated thread and callers submit work with run_coroutine_threadsafe.
Attribute access returns a callable that dispatches the matching StateMachine method
by name, so existing `machine.SendSMS(...)` call sites work unchanged.

Owning the queue is also what makes recovery possible. A caller that gives up cancels
its command, so an abandoned backlog is dropped rather than replayed against a modem
that has just come back. And because a worker that never returns would swallow every
later command -- including the soft reset that recovers the modem -- a run of
consecutive timeouts exits the process so the Supervisor restarts the add-on.

Re-entrancy: incoming SMS and call callbacks registered with SetIncomingCallback are
invoked *on the worker thread*, from inside ReadDevice. A callback that calls back
into this proxy would enqueue work behind itself and block forever. Callbacks must
hand off to another thread; mqtt_publisher does this with a threading.Timer.
"""

import asyncio
import logging
import os
import threading

import gammu
import gammu.worker
from gammu.asyncworker import GammuAsyncThread, GammuAsyncWorker

logger = logging.getLogger(__name__)

# Upper bound on queue wait plus execution for a single command. gammu's own
# commtimeout cannot serve here for two reasons: SetConfig rejects any key outside
# {Model, DebugLevel, Device, Connection, DebugFile, UseGlobalDebugFile, LockDevice,
# StartInfo, SyncTime} with ValueError, and libGammu never reads commtimeout anyway
# -- it appears only in smsd/core.c, the SMS daemon's own configuration.
DEFAULT_TIMEOUT = int(os.environ.get("GAMMU_COMMAND_TIMEOUT", "60"))

# Init and Terminate talk to the modem, so they get the same allowance as any other
# command rather than a short startup timeout.
STARTUP_TIMEOUT = 60

# Consecutive timeouts before the add-on gives up and restarts. libGammu normally
# bounds its own waits -- the SMS prompt stall returns TIMEOUT[14] after 29s -- and
# the queue drains once it does. A worker that never returns is a different failure:
# every later command times out behind it, including the soft reset that would
# otherwise recover the modem. Nothing inside the process can clear that, because the
# stuck call cannot be interrupted from Python.
#
# Both this and DEFAULT_TIMEOUT read the environment so the end-to-end tests can
# reach the give-up path in seconds instead of three minutes. Operators can use
# them too: a slow modem on a long multipart send may want a larger timeout.
STALL_LIMIT = int(os.environ.get("GAMMU_STALL_LIMIT", "3"))

# EX_SOFTWARE. Any non-zero exit makes the Supervisor restart the add-on.
STALL_EXIT_CODE = 70


def restart_process(timeouts):
    """Exit so the Supervisor restarts the add-on.

    os._exit rather than sys.exit: this runs on a caller's thread, and SystemExit
    raised off the main thread only unwinds that thread. The gammu thread is blocked
    inside libGammu and cannot be joined, so an orderly shutdown is not on offer.
    Logging is flushed first because os._exit skips interpreter cleanup.
    """
    logger.critical(
        "Gammu worker stalled for %d consecutive commands; restarting the add-on", timeouts
    )
    logging.shutdown()
    os._exit(STALL_EXIT_CODE)


class _ExceptionPreservingThread(GammuAsyncThread):
    """Worker thread that reports the original gammu exception.

    GammuAsyncThread._do_command converts a GSMError into its error *name* and
    GammuAsyncWorker.worker_callback re-wraps that string as a plain gammu.GSMError,
    which discards the specific subclass. Callers written against python-gammu expect
    to catch gammu.ERR_NOSIM or gammu.ERR_NOTSUPPORTED, and support.py does exactly
    that. Passing the exception object through keeps `except gammu.ERR_*` working,
    along with the error dict in args[0].
    """

    def _do_command(self, future, cmd, params, percentage=100):
        # A caller that timed out cancels its future. Running the command anyway would
        # drive the modem for a result nobody is waiting for, and after a long stall
        # the entire backlog would replay in a burst against a modem that just came
        # back. Init and Terminate arrive as plain strings and are never skipped.
        #
        # Reading cancelled() from this thread is a state read on the asyncio future;
        # the worst case is executing a command abandoned microseconds earlier.
        if hasattr(future, "cancelled") and future.cancelled():
            logger.debug("Skipping abandoned gammu command %s", cmd)
            return

        func = getattr(self._sm, cmd)
        try:
            result = gammu.worker._execute_command(func, params)
        except Exception as exception:  # noqa: BLE001 - relayed to the caller verbatim
            self._callback(future, None, exception, percentage)
        else:
            self._callback(future, result, None, percentage)


class _Worker(GammuAsyncWorker):
    """GammuAsyncWorker using the exception preserving thread."""

    def worker_callback(self, name, result, error, percents):
        """Deliver a result, unless the caller has already given up on it.

        A command that was mid-flight when its caller timed out still runs to
        completion, and the base implementation would then call set_result on a
        cancelled future -- InvalidStateError, once per timeout, surfacing as a
        loop exception rather than anywhere useful. The check runs on the loop
        thread so it cannot race the cancellation it is testing for.
        """
        if not hasattr(name, "set_result"):
            super().worker_callback(name, result, error, percents)
            return

        def deliver():
            if name.done():
                logger.debug("Discarding gammu result for an abandoned command")
            elif error is None:
                name.set_result(result)
            elif isinstance(error, Exception):
                name.set_exception(error)
            else:
                name.set_exception(gammu.GSMError(error))

        self._loop.call_soon_threadsafe(deliver)

    async def init_async(self):
        self._init_future = self._loop.create_future()
        self._thread = _ExceptionPreservingThread(
            self._queue, self._config, self._callback, self._pull_func
        )
        self._thread.start()
        await self._init_future
        self._init_future = None


class GammuWorkerProxy:
    """Presents the StateMachine API, executes every call on the worker thread."""

    def __init__(
        self,
        config,
        timeout=DEFAULT_TIMEOUT,
        worker_factory=_Worker,
        stall_limit=STALL_LIMIT,
        on_stall=restart_process,
    ):
        self._timeout = timeout
        self._worker_factory = worker_factory
        self._stall_limit = stall_limit
        self._on_stall = on_stall
        self._timeouts = 0
        self._timeouts_lock = threading.Lock()
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, name="gammu-loop", daemon=True)
        self._thread.start()
        self._ready.wait()

        # GammuAsyncWorker.__init__ calls asyncio.get_event_loop(), which outside a
        # running loop is deprecated in 3.10+ and an error in 3.14. Constructing it
        # inside a coroutine means get_event_loop() returns our running loop.
        self._worker = self._submit(self._create(config), STARTUP_TIMEOUT)

    def init(self):
        """Connect to the phone.

        Separate from construction so a caller can tolerate an Init failure and keep
        using the proxy, which is what support.py does for a missing SIM. The worker
        thread survives a failed Init and stays ready for further commands.
        """
        self._submit(self._worker.init_async(), STARTUP_TIMEOUT)

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def _submit(self, coro, timeout):
        pending = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            result = pending.result(timeout)
        except TimeoutError:
            # Cancelling drops the command if the worker has not reached it yet; a
            # command already executing runs to completion, because libGammu cannot
            # be interrupted. Either way it never races the next one.
            pending.cancel()
            pending.add_done_callback(self._discard_late_result)
            self._record_timeout()
            raise
        else:
            self._record_success()
            return result

    def _record_timeout(self):
        with self._timeouts_lock:
            self._timeouts += 1
            timeouts = self._timeouts
        if self._stall_limit and timeouts >= self._stall_limit:
            self._on_stall(timeouts)

    def _record_success(self):
        with self._timeouts_lock:
            self._timeouts = 0

    @staticmethod
    def _discard_late_result(pending):
        try:
            pending.result()
        except (Exception, asyncio.CancelledError) as exc:  # noqa: BLE001 - caller gave up
            logger.debug("Late gammu result discarded: %s", type(exc).__name__)

    async def _create(self, config):
        worker = self._worker_factory()
        worker.configure(config)
        return worker

    async def _dispatch(self, name, args, kwargs):
        future = self._loop.create_future()
        # GammuThread._execute_command calls func(**params) for a dict, func(*params)
        # for anything else, and func() for None. It cannot do both at once.
        if args and kwargs:
            raise TypeError(f"{name}() takes positional or keyword arguments, not both")
        self._worker.enqueue(future, commands=[(name, kwargs if kwargs else args)])
        return await future

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        def call(*args, **kwargs):
            return self._submit(self._dispatch(name, args, kwargs), self._timeout)

        call.__name__ = name
        return call

    def terminate(self):
        """Stop the worker thread and the private event loop."""
        try:
            if self._worker is not None:
                self._submit(self._worker.terminate_async(), STARTUP_TIMEOUT)
        except Exception:
            logger.exception("Gammu worker did not terminate cleanly")
        finally:
            self._worker = None
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
