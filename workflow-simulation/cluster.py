from .simulation import Event


class Machine:
    def __init__(self, id, cores, memory):
        self.id = id
        self.cores_total = cores
        self.cores_available = cores
        self.memory_total = memory
        self.memory_available = memory


class Cluster:
    def __init__(self, simulation, machines):
        self.simulation = simulation
        self.machines = machines
        self.task_finish_cb = None
        self.task_start_cb = None

    def get_machines(self):
        return self.machines

    def start_task(self, task):
        # print(f'{self.time}: starting {task.name}')
        machine = self.machines[task.machine]
        assert machine.memory_available >= task.memory
        assert machine.cores_available >= task.cores
        machine.memory_available -= task.memory
        machine.cores_available -= task.cores
        task.start_time = self.simulation.time
        self.task_start_cb(task)
        # + 1 for overhead
        self.simulation.add_event(Event(self.simulation.time + task.run_time + 1,
                                        self.finish_task, task))

    def finish_task(self, task):
        # print(f'{self.time}: finishing {task_name}')
        machine = self.machines[task.machine]
        assert machine.memory_total >= machine.memory_available + task.memory
        assert machine.cores_total >= machine.cores_available + task.cores
        machine.memory_available += task.memory
        machine.cores_available += task.cores
        self.task_finish_cb(task)

    def register_task_finish_cb(self, callback):
        self.task_finish_cb = callback

    def register_task_start_cb(self, callback):
        self.task_start_cb = callback
