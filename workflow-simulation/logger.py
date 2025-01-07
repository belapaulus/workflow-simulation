import os
import time
import hashlib


def uid(s=None):
    if s == None:
        return hashlib.md5(f'{time.time()}'.encode()).hexdigest()
    return hashlib.md5(s.encode()).hexdigest()


class Logger:
    def __init__(self, log_tasks):
        self.log_tasks = log_tasks
        self.output_dir = uid()
        assert not os.path.exists(self.output_dir)
        os.mkdir(self.output_dir)
        # create output files and write headers
        self.instances = open(os.path.join(self.output_dir, 'instances.csv'), 'w')
        self.instances.write('id;workflow;loop;repetitions\n')
        self.executions = open(os.path.join(self.output_dir, 'executions.csv'), 'w')
        self.executions.write('id;instance;strategy;duration\n')
        self.tasks = open(os.path.join(self.output_dir, 'tasks.csv'), 'w')
        self.tasks.write('id;execution;task;abstract;start;duration;memory;machine\n')
        self.current_instance = None

    def __del__(self):
        self.instances.close()
        self.executions.close()
        self.tasks.close()

    def new_workflow_instance(self, workflow_name, loop_task, repetitions):
        id = uid()
        print(workflow_name, loop_task, repetitions)
        self.instances.write(f'{id};{workflow_name};{loop_task};{repetitions}\n')
        self.current_instance = id

    def new_workflow_execution(self, strategy, runtime):
        assert self.current_instance is not None
        execution_id = uid()
        self.executions.write(f'{execution_id};{self.current_instance};{strategy};{runtime.scheduler.simulation.time}\n')
        if self.log_tasks:
            for task in runtime.tasks.values():
                self.tasks.write(f'{uid()};{execution_id};{task.name};{task.abstract};{task.start_time};{task.run_time};{task.memory};{task.machine}\n')
