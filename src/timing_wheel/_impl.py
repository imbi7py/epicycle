import attr
from collections import deque
from zope.interface import Interface, implementer


class Empty(Exception):
    """
    Raised when there are
    """


class ITimerModule(Interface):
    """
    A timing module as described in:

    Hashed and Hierarchical Timing Wheels: Data Structures for the
    Efficient Implementation of a Timer Facility

    Varghese & Lauck 1987
    """

    def add(interval, f, *args, **kwargs):
        """
        From the paper:

        The client calls this routine to start a timer that will
        expire after "Interval" units of time.  The client supplies a
        Request_ID which is used to distinguish this timer from other
        timers that the client has outstanding.  Finally, the client
        can specify what action must be taken on expiry: for instance,
        calling a client-specified routine, or setting an event flag.

        :param interval: When to run the action.
        :type interval: :py:cls:`int`.

        :param f: The action to run after the interval has elapsed.
        :type f: :py:cls:`callable`.

        :return: An opaque request ID that can be passed to
                 :py:meth:`ITimerModule.remove`.
        """

    def remove(request_id):
        """
        From the paper:

        This routine uses its knowledge of the client and Request_ID
        to locate the timer and stop it.

        :param request_id: An identifier returned by
            :py:meth:`ITimerModule.add`.
        """

    def tick(now):
        """
        Let the granularity of the timer-be T units.  Then every T
        units this routine checks whether any outstanding timers have
        expired; if so, it calls stop, which in turn calls the next
        routine.

        :param now: The current absolute time.
        :type now: A number; must be the same time as the ``interval``
            argument to :py:meth:`ITimerModule.add`.
        """

    def when():
        """
        Return the absolute time at which the soonest action should
        run.

        :return: A number; must be the same time as the ``interval``
                 argument to :py:meth:`ITimerModule.add`.

        :raises Empty: When there are no pending actions.
        """


@attr.s
class _Cell(object):
    value = attr.ib()
    successor = attr.ib(default=None, repr=False)
    predecessor = attr.ib(default=None, repr=False)

    def add(self, successor):
        if self.successor is not None:
            self.successor.predecessor = successor
        successor.predecessor = self
        self.successor = successor

    def remove(self):
        if self.predecessor:
            self.predecessor.successor = self.successor
        if self.successor:
            self.successor.predecessor = self.predecessor
        self.successor = None
        self.predecessor = None


@attr.s
class _List(object):
    head = attr.ib()
    tail = attr.ib()
    deadline = attr.ib()

    def add_to_front(self, value):
        cell = _Cell(value)
        cell.successor = self.head.successor
        cell.predecessor = self.head
        self.head.successor.predecessor = cell
        self.head.successor = cell
        return cell

    def empty(self):
        return self.head.successor is self.tail

    def consume(self):
        cell = self.head.successor
        while cell is not self.tail:
            next_cell = cell.successor
            cell.remove()
            yield cell.value
            cell = next_cell


def make_list(deadline):
    head = _Cell(None)
    tail = _Cell(None)
    head.add(tail)
    tail.add(head)
    return _List(head, tail, deadline)


@implementer(ITimerModule)
@attr.s
class TimingWheel(object):
    """
    Scheme 4 from:

    Hashed and Hierarchical Timing Wheels: Data Structures for the
    Efficient Implementation of a Timer Facility

    Varghese & Lauck 1987
    """
    _max_interval = attr.ib()
    _time = attr.ib(default=0)
    _last_id = attr.ib(default=0)

    _schedule = attr.ib(default=attr.Factory(list), repr=False)
    _actions = attr.ib(default=attr.Factory(dict))

    def __attrs_post_init__(self):
        self._schedule = deque(make_list(i) for i in range(self._max_interval))

    def _make_id(self):
        self._last_id += 1
        return self._last_id

    def add(self, interval, f, *args, **kwargs):
        request_id = self._make_id()
        action = (f, args, kwargs)
        timing_list = self._schedule[interval]
        timing_list.deadline = self._time + interval
        cell = timing_list.add_to_front((request_id, action))
        self._actions[request_id] = cell
        return request_id

    def remove(self, request_id):
        if request_id in self._actions:
            cell = self._actions.pop(request_id)
            cell.remove()

    def tick(self, time):
        offset = time - self._time
        assert 0 <= offset < self._max_interval
        for i in range(offset + 1):
            timing_list = self._schedule.popleft()
            for (request_id, action) in timing_list.consume():
                (f, args, kwargs) = action
                f(*args, **kwargs)
                del self._actions[request_id]
            timing_list.deadline = i
            self._schedule.append(timing_list)
        self._time = time

    def when(self):
        pending = [
            i for i in range(self._max_interval)
            if not self._schedule[i].empty()
        ]
        if not pending:
            raise Empty()
        return self._schedule[min(pending)].deadline
