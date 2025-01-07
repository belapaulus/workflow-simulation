from enum import Enum, auto


class TaskState(Enum):
    # CREATED = auto()
    BLOCKED = auto()
    READY = auto()
    RUNNING = auto()
    DONE = auto()
    # FAILED = auto()


class WorkflowState(Enum):
    CREATED = auto()
    RUNNING = auto()
    DONE = auto()
    FAILED = auto()


class TaskInstance:
    def __init__(self, name, abstract, parents, run_time, memory):
        self.name = name
        self.abstract = abstract
        self.parents = parents
        self.run_time = run_time
        self.memory = memory
        # TODO make cores changeable
        self.cores = 1
        self.machine = None
        self.state = TaskState.BLOCKED
        self.start_time = None
        assert self.name not in self.parents


class Runtime:
    def __init__(self, scheduler, task_instances):
        self.scheduler = scheduler
        self.scheduler.register_task_finish_cb(self.task_finish_cb)
        self.scheduler.register_task_start_cb(self.mark_task_running)
        self.tasks = task_instances
        self.state = WorkflowState.CREATED
        self.blocked = {t for t in self.tasks.values()}
        self.ready = set()
        self.running = set()
        self.done = set()

    def start(self):
        self.state = WorkflowState.RUNNING
        self.operate()

    def operate(self):
        if len(self.get_tasks(TaskState.DONE)) == len(self.get_tasks()):
            self.state = WorkflowState.DONE
            return
        new = []
        for task in self.get_tasks(TaskState.BLOCKED):
            ready = True
            for parent in task.parents:
                if not self.tasks[parent].state is TaskState.DONE:
                    ready = False
                    break
            if ready:
                new.append(task)
        for task in new:
            self.mark_task_ready(task)
        self.scheduler.add_to_batch(self.get_tasks(TaskState.READY))

    def get_tasks(self, task_state=None):
        if task_state is None:
            return self.tasks.values()
        lookup = {
            TaskState.BLOCKED: self.blocked,
            TaskState.READY: self.ready,
            TaskState.RUNNING: self.running,
            TaskState.DONE: self.done,
        }
        return lookup[task_state]

    def mark_task_ready(self, task):
        task.state = TaskState.READY
        self.blocked.remove(task)
        self.ready.add(task)

    def mark_task_running(self, task):
        task.state = TaskState.RUNNING
        self.ready.remove(task)
        self.running.add(task)

    def mark_task_done(self, task):
        task.state = TaskState.DONE
        self.running.remove(task)
        self.done.add(task)

    def task_finish_cb(self, task):
        self.mark_task_done(task)
        self.operate()
