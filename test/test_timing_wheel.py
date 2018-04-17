import attr
import heapq
from hypothesis import stateful, strategies as st
import pytest
from pyrsistent import pvector
from timing_wheel import Empty, ITimerModule, TimingWheel
from zope.interface import implementer, verify


@implementer(ITimerModule)
@attr.s
class TimerHeap(object):
    """
    A min-heap-based implementation of :py:class:`ITimerModule`.
    """
    _heap = attr.ib(default=attr.Factory(list))
    _time = attr.ib(default=0)
    _last_id = attr.ib(default=0)

    def _make_id(self):
        self._last_id += 1
        return self._last_id

    def add(self, deadline, f, *args, **kwargs):
        request_id = self._make_id()
        action = (f, args, kwargs)
        heapq.heappush(
            self._heap,
            (deadline, request_id, action)
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
            deadline, request_id, action = heapq.heappop(self._heap)
            f, args, kwargs = action
            f(*args, **kwargs)

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
        TimerHeap,
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
class _Action(object):
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
    now = attr.ib(default=0)
    request_ids = attr.ib(default=attr.Factory(dict))

    def make_action(self):
        return _Action()

    def record_action(self, request_id, action):
        action.request_id = request_id
        self.request_ids[request_id] = action


class VerificationStateMachine(stateful.RuleBasedStateMachine):
    """
    Verify the implementation of a timing wheel against the
    :py:class:`TimerHeap`.
    """
    scripts = stateful.Bundle("scripts")

    def make_timing_wheel(self):
        """
        Make a timing wheel instance to test.
        """
        raise NotImplementedError

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

    def play_script(self, script):
        wheel = self.make_timing_wheel()
        heap = TimerHeap()
        state = TimerState()
        for step in script:
            step(wheel, heap, state)

    no_empty_script = True

    @stateful.precondition(lambda self: self.no_empty_script)
    @stateful.rule(target=scripts)
    def inital_script(self):
        self.no_empty_script = False
        return pvector()

    @stateful.rule(interval=st.integers(min_value=0, max_value=16),
                   script=scripts, target=scripts)
    def add(self, interval, script):
        def perform_add(wheel, heap, state):
            deadline = state.now + interval
            action = state.make_action()

            self.assert_whens(wheel, heap)
            wheel_request_id = wheel.add(deadline, action.call_from_wheel)
            heap_request_id = heap.add(deadline, action.call_from_heap)
            self.assert_whens(wheel, heap)

            assert wheel_request_id == heap_request_id
            state.record_action(wheel_request_id, action)

        script_with_add = script.append(perform_add)
        self.play_script(script_with_add)
        return script_with_add

    @stateful.rule(data=st.data(), script=scripts, target=scripts)
    def remove(self, data, script):
        def perform_remove(wheel, heap, state):
            if state.request_ids:
                request_id = data.draw(
                    st.sampled_from(list(state.request_ids)))
                state.request_ids.pop(request_id)

                self.assert_whens(wheel, heap)
                wheel.remove(request_id)
                heap.remove(request_id)
                self.assert_whens(wheel, heap)

        script_with_remove = script.append(perform_remove)
        self.play_script(script_with_remove)
        return script_with_remove

    @stateful.rule(now=st.integers(min_value=1, max_value=127),
                   script=scripts, target=scripts)
    def tick(self, now, script):
        def perform_tick(wheel, heap, state):
            state.now += now
            wheel.tick(state.now)
            heap.tick(state.now)
            for action in state.request_ids.values():
                action.equivalent()

        script_with_tick = script.append(perform_tick)
        self.play_script(script_with_tick)
        return script_with_tick


class VerifyTimingWheelStateMachine(VerificationStateMachine):
    def make_timing_wheel(self):
        return TimingWheel(128)


VerifyTimingWheel = VerifyTimingWheelStateMachine.TestCase
