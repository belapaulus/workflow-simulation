import heapq


class Event:
    def __init__(self, time, callback, data):
        self.time = time
        self.callback = callback
        self.data = data

    def __lt__(self, other):
        return self.time < other.time


class Simulation:
    def __init__(self):
        self.queue = []
        self.time = 0

    def next_event(self):
        assert len(self.queue) > 0
        event = heapq.heappop(self.queue)
        assert event.time >= self.time
        self.time = event.time
        # print(f'{event.time}: {event.callback.__name__}')
        event.callback(event.data)

    def add_event(self, event):
        assert type(event) is Event
        heapq.heappush(self.queue, event)
