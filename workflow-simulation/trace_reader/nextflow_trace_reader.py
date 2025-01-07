from os import path, listdir
import json
from ..constants import NEXTFLOW_TRACE_DIR
from .workflow import Workflow, Task


class NextflowTraceReader:
    def __init__(self, trace_dir=NEXTFLOW_TRACE_DIR):
        self.workflows = {}
        for file in listdir(trace_dir):
            file_path = path.join(trace_dir, file)
            if path.isfile(file_path) and file[-5:] == '.json':
                self.workflows[file.split('-')[0]] = file_path
        self.file_cache = {}

    def get_workflows(self):
        return sorted(list(self.workflows.keys()))

    def get_workflow(self, workflow):
        if workflow not in self.file_cache:
            with open(self.workflows[workflow]) as file:
                self.file_cache[workflow] = json.loads(file.read())
        tasks = self.parse_traces(self.file_cache[workflow])
        return Workflow(tasks)

    @staticmethod
    def parse_traces(traces):
        tasks = {}
        for t in traces['workflow']['specification']['tasks']:
            id = t['id']
            # abstract = t['name']
            # make each tasks its own abstract task
            tasks[id] = Task(id, id, t['parents'], t['children'])
        # read run time and memory data
        for t in traces['workflow']['execution']['tasks']:
            task = tasks[t['id']]
            task.run_time = t['runtimeInSeconds']
            task.memory = t['memoryInBytes']
        return tasks
