import sys
import random
from .workflow import Workflow
from ..simulation import Event


class Scheduler:
    def __init__(self, simulation, wf, cluster, strategy):
        self.simulation = simulation
        self.wf = Workflow(wf)
        self.cluster = cluster
        strategies = {
            'random': self.prioritize_random,
            'cpe': self.prioritize_cpe,
            'lid': self.prioritize_lid,
            'rcpe': self.prioritize_rcpe,
            'cper': self.prioritize_cper,
            # 'cpcl': self.prioritize_cpcl,
        }
        self.prioritize = lambda tasks: strategies[strategy](self.prioritize_random(tasks))
        self.batch = None

    def register_task_finish_cb(self, callback):
        self.cluster.register_task_finish_cb(callback)

    def register_task_start_cb(self, callback):
        self.cluster.register_task_start_cb(callback)

    def add_to_batch(self, tasks):
        if len(list(tasks)) == 0:
            return
        if self.batch is None:
            self.batch = set()
            self.simulation.add_event(Event(self.simulation.time + 1,
                                            self.schedule_batch_cb, None))
        self.batch |= set(tasks)

    def schedule_batch_cb(self, data):
        tasks = self.prioritize(list(self.batch))
        self.assign(tasks)
        self.batch = None

    '''
    def prioritize(self, tasks):
        # self.prioritize = lambda tasks: strategies[strategy](self.prioritize_random(tasks))
        tasks = self.prioritize_random(tasks)
        t0 = self.prioritize_cpe(tasks.copy())
        t1 = self.prioritize_cpcl(tasks.copy())
        t2 = self.prioritize_rcpe(tasks.copy())
        if not t0 == t1:
            def get_info(task):
                t = self.wf.get_task(task.abstract)
                loo = self.wf.num_loops
                return (len(t.critical_path), loo(t.critical_path),
                        len(t.repetitive_critical_path), loo(t.repetitive_critical_path),
                        len(t.path_to_repetition or []))
            print('---')
            print([get_info(t) for t in t0])
            print([get_info(t) for t in t1])
            print([get_info(t) for t in t2])
            print('xxx')
        return t1
    '''

    def assign(self, tasks):
        for task in tasks:
            assert task.machine is None
            machines = self.cluster.get_machines()
            for machine in machines:
                if (task.cores <= machine.cores_available) and (
                        task.memory <= machine.memory_available):
                    task.machine = machine.id
                    self.cluster.start_task(task)
                    break

    def prioritize_random(self, tasks):
        random.shuffle(tasks)
        return tasks

    def prioritize_cpe(self, tasks):
        # critical path to exit
        tasks.sort(key=lambda task: (
            len(self.wf.get_task(task.abstract).critical_path)),
            reverse=True)
        return tasks

    def prioritize_lid(self, tasks):
        # loops in descendants
        tasks.sort(key=lambda task: (
            self.wf.num_loops(self.wf.get_task(task.abstract).descendants)),
            reverse=True)
        return tasks

    def prioritize_rcpe(self, tasks):
        tasks.sort(key=lambda task: (
            self.wf.num_loops(self.wf.get_task(task.abstract).repetitive_critical_path),
            len(self.wf.get_task(task.abstract).repetitive_critical_path)),
            reverse=True)
        return tasks

    def prioritize_cper(self, tasks):
        tasks.sort(key=lambda task: (
            len(self.wf.get_task(task.abstract).critical_path),
            self.wf.num_loops(self.wf.get_task(task.abstract).critical_path)),
            reverse=True)
        return tasks

    '''
    def prioritize_cpcl(self, tasks):
        # critical path to closest loop
        loop_bucket = set()
        non_loop_bucket = set()
        for task in tasks:
            if self.wf.get_task(task.abstract).path_to_repetition is not None:
                assert self.wf.num_loops(self.wf.get_task(task.abstract).repetitive_critical_path) > 0 or (
                        self.wf.get_task(task.abstract).loop)
                loop_bucket.add(task)
            else:
                non_loop_bucket.add(task)
        ret = []
        ret += list(sorted(loop_bucket,
                           key=lambda task: len(self.wf.get_task(
                               task.abstract).path_to_repetition),
                           reverse=True))
        for i in range(1, len(ret)):
            t1 = self.wf.get_task(ret[i-1])
            t2 = self.wf.get_task(ret[i])
            assert len(t1.repetitive_critical_path) >= len(t2.repetitive_critical_path)
        ret += self.prioritize_cpe(list(non_loop_bucket))
        return ret
    '''
