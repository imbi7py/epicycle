import attr
import heapq
from hypothesis import stateful, strategies as st
import pytest
from timing_wheel import Empty, ITimerModule, TimingWheel
from zope.interface import implementer, verify


@implementer(ITimerModule)
@attr.s
class SchedulingHeap(object):
    """
    A min-heap-based implementation of :py:class:`ITimerModule`.
    """
    _heap = attr.ib(default=attr.Factory(list))
    _time = attr.ib(default=0)
    _last_id = attr.ib(default=0)

    def _make_id(self):
        self._last_id += 1
        return self._last_id

    def add(self, interval, f, *args, **kwargs):
        request_id = self._make_id()
        action = (f, args, kwargs)
        heapq.heappush(
            self._heap,
            (self._time + interval, request_id, action)
        )
        return request_id

    def remove(self, request_id):
        heap_prime = [
            (deadline, pending_request_id, action) for
            deadline, pending_request_id, action in self._heap
            if pending_request_id != request_id
        ]
        heapq.heapify(heap_prime)
        self._heap = heap_prime

    def tick(self, now):
        while self._heap and self._heap[0][0] <= now:
            interval, request_id, action = heapq.heappop(self._heap)
            f, args, kwargs = action
            f(*args, **kwargs)
        self._time = now

    def when(self):
        if not self._heap:
            raise Empty()
        else:
            return self._heap[0][0]


class TestAPI(object):
    """
    API Tests for L{ITimerModule} implementations.
    """

    @pytest.fixture(params=[
        SchedulingHeap,
        lambda: TimingWheel(128)
    ])
    def timer(self, request):
        """
        :py:class:`ITimerModule` implementations.
        """
        return request.param()

    def test_provides_interface(self, timer):
        """
        The timer provides py:cls:`ITimerModule`.
        """
        verify.verifyObject(ITimerModule, timer)

    def test_add_returns_unique_request_id(self, timer):
        """
        Adding an action returns a unique ``request_id``.
        """
        assert timer.add(1, lambda: None) != timer.add(1, lambda: None)

    def test_empty_when(self, timer):
        """
        A timer with no pending actions raises :py:exc:`Empty` from
        its :py:meth:`ITimerModule.when` method.
        """
        with pytest.raises(Empty):
            timer.when()

    def test_when(self, timer):
        """
        The time at which the next event should run is returned.
        """
        timer.add(1, lambda: None)
        timer.add(2, lambda: None)
        assert timer.when() == 1

    def test_remove(self, timer):
        """
        An added action can be removed.
        """
        assert timer.add(1, lambda: None) != timer.add(1, lambda: None)

    def test_tick(self, timer):
        """
        All actions whose deadline is prior or equal to the provided
        absolute time are run.
        """
        run = []

        timer.add(1, run.append, 1)
        timer.add(2, run.append, 2)
        timer.add(3, run.append, 3)

        timer.tick(2)

        assert run == [1, 2]

        timer.tick(3)

        assert run == [1, 2, 3]


@attr.s
class Action(object):
    request_id = attr.ib(default=None, init=False)
    wheel_called = attr.ib(default=False, init=False)
    heap_called = attr.ib(default=False, init=False)

    def call_from_wheel(self):
        self.wheel_called = True

    def call_from_heap(self):
        self.heap_called = True

    def equivalent(self):
        assert self.wheel_called == self.heap_called


@attr.s
class TimerState(object):
    wheel = attr.ib()
    heap = attr.ib()
    now = attr.ib(default=0)
    request_ids = attr.ib(default=attr.Factory(dict))


class VerificationStateMachine(stateful.RuleBasedStateMachine):
    """
    Verify the implementation of a timing wheel against the
    :py:class:`SchedulingHeap`.
    """
    states = stateful.Bundle("state")

    def make_timing_wheel(self):
        """
        Make a timing wheel instance to test.
        """
        raise NotImplementedError

    @stateful.rule(target=states)
    def inital_state(self):
        return TimerState(wheel=self.make_timing_wheel(),
                          heap=SchedulingHeap())

    def assert_whens(self, wheel, heap):
        empty_raised = 'empty raised'
        try:
            wheel_when = wheel.when()
        except Empty:
            wheel_when = empty_raised

        try:
            heap_when = heap.when()
        except Empty:
            heap_when = empty_raised

        assert wheel_when == heap_when

    @stateful.rule(interval=st.integers(min_value=0, max_value=16),
                   state=states, target=states)
    def add(self, interval, state):
        self.assert_whens(state.wheel, state.heap)
        action = Action()
        wheel_request_id = state.wheel.add(interval, action.call_from_wheel)
        heap_request_id = state.heap.add(interval, action.call_from_heap)

        assert wheel_request_id == heap_request_id
        self.assert_whens(state.wheel, state.heap)

        action.request_id = wheel_request_id
        state.request_ids[wheel_request_id] = action
        return state

    @stateful.rule(data=st.data(), state=states, target=states)
    def remove(self, data, state):
        if state.request_ids:
            request_id = data.draw(st.sampled_from(list(state.request_ids)))
            state.request_ids.pop(request_id)
            state.wheel.remove(request_id)
            state.heap.remove(request_id)
        return state

    @stateful.rule(now=st.integers(min_value=1, max_value=16), state=states,
                   target=states)
    def tick(self, now, state):
        state.now += now

        state.wheel.tick(state.now)
        state.heap.tick(state.now)
        for request_id, action in state.request_ids.items():
            action.equivalent()
        return state


class VerifyTimingWheelStateMachine(VerificationStateMachine):
    def make_timing_wheel(self):
        return TimingWheel(128)


VerifyTimingWheel = VerifyTimingWheelStateMachine.TestCase
