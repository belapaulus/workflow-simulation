import random
import numpy as np
from ..runtime import TaskInstance as RuntimeTaskInstance

# Store workflow more semantically ie list of tasks (each with their own
# abstract task) and information on where loops should be added maybe as a
# dict

class Task:
    def __init__(self, name, abstract, parents, children):
        self.name = name
        self.abstract = abstract
        self.parents = set(parents)
        self.children = set(children)
        self.run_time = None
        self.memory = None

    def copy(self):
        new = Task(self.name, self.abstract,
                   self.parents.copy(), self.children.copy())
        new.run_time = self.run_time
        new.memory = self.memory
        return new

    def to_dict(self):
        return {
            'name': self.name,
            'abstract': self.abstract,
            'parents': list(self.parents),
            'children': list(self.children),
            'run_time': self.run_time,
            'memory': self.memory,
        }


class AbstractTask:
    def __init__(self, task):
        self.name = task.abstract
        self.instances = {task.name}
        self.parents = task.parents.copy()
        self.children = task.children.copy()

    def add_children(self, children):
        self.children |= set(children)

    def add_parents(self, parents):
        self.parents |= set(parents)


class Workflow:
    '''
    Workflow object instantiated from a list of tasks.
    For now assuming that no two tasks have the same abstract task.
    '''
    def __init__(self, tasks):
        self.tasks = tasks
        self.abstract_tasks = {t.abstract: AbstractTask(t) for t in tasks.values()}
        self.loop_name = None

    def get_copies_with_single_loop(self, n_reps):
        copies = []
        for name in self.abstract_tasks.keys():
            copy = self.copy()
            copy._add_loop(name, n_reps)
            copies.append(copy)
        return copies

    def _add_loop(self, name, n_reps):
        assert self.loop_name == None
        abstract = self.abstract_tasks[name]
        task = self.tasks[name]
        assert len(abstract.instances) == 1
        previous = task
        for repetition_number in range(n_reps):
            new_instance = Task(f'{task.name}-rep-{repetition_number}',
                                task.abstract,
                                [previous.name],
                                previous.children.copy())
            new_instance.run_time = task.run_time
            new_instance.memory = task.memory
            previous.children = {new_instance.name}
            self.tasks[new_instance.name] = new_instance
            abstract.instances.add(new_instance.name)
            previous = new_instance
        for child in previous.children:
            self.tasks[child].parents.remove(task.name)
            self.tasks[child].parents.add(previous.name)
        assert len(abstract.instances) == n_reps + 1
        abstract.add_parents({name})
        abstract.add_children({name})
        self.loop_name = name

    def get_min_mem(self):
        '''
        figure out the minimum memory required for this workflow.
        ie the amount of memory that the task with the highest memory requires
        '''
        min_memory = 0
        for task in self.tasks.values():
            if task.memory > min_memory:
                min_memory = task.memory
        return min_memory

    def get_loop_name(self):
        return self.loop_name

    def to_runtime(self):
        tasks = {}
        for task in self.tasks.values():
            tasks[task.name] = RuntimeTaskInstance(task.name,
                                                   task.abstract,
                                                   task.parents,
                                                   task.run_time,
                                                   task.memory)
        return tasks

    def to_scheduler(self):
        return {t.name: t.parents for t in self.abstract_tasks.values()}

    def copy(self):
        task_copies = [t.copy() for t in self.tasks.values()]
        return Workflow({t.name: t for t in task_copies})

    def to_dict(self):
        return {t.name: t.to_dict() for t in self.tasks.values()}
