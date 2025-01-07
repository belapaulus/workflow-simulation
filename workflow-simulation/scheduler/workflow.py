class Task:
    def __init__(self, name, parents):
        self.name = name
        self.parents = parents - {name}
        self.critical_path = []
        self.repetitive_critical_path = []
        # self.path_to_repetition = None
        self.descendants = None
        self.loop = name in parents


class Workflow:
    '''
    Workflow as understood by the scheduler. Receives a workflow represented
    by a dictionary where the keys are the tasks and the values to a given
    key are the parents of that task. Translates the received workflow object
    to a list of tasks objects that store information about the workflow
    structure.

    Assumes that the only type of circular dependency is a task being dependant
    on itself. Information about such tasks is stored in the self.loops field.
    '''
    def __init__(self, wf):
        self.tasks = {}
        for task, parents in wf.items():
            assert task not in self.tasks.keys()
            self.tasks[task] = Task(task, parents)
        for task in self.tasks.keys():
            self._update_descendants(task, set())
            self._update_critical_path(task, [task])
            self._update_repetitive_critical_path(task, [task])
            # if self.tasks[task].loop:
            #     self._update_path_to_repetition(task, [])

    def num_loops(self, path):
        return len([t for t in path if self.tasks[t].loop])

    def _update_descendants(self, task_name, descendants):
        task = self.tasks[task_name]
        if task.descendants is None:
            task.descendants = descendants
            for parent in task.parents:
                self._update_descendants(parent, {task.name} | task.descendants)
            return
        if len(descendants - task.descendants) == 0:
            return
        task.descendants |= descendants
        for parent in task.parents:
            self._update_descendants(parent, {task.name} | task.descendants)

    def _update_critical_path(self, task_name, path):
        task = self.tasks[task_name]
        if len(task.critical_path) > len(path):
            return
        if (len(task.critical_path) == len(path)) and (
                self.num_loops(task.critical_path) >= self.num_loops(path)):
            return
        task.critical_path = path
        for parent in task.parents:
            self._update_critical_path(parent, [parent] + task.critical_path)

    def _update_repetitive_critical_path(self, task_name, path):
        task = self.tasks[task_name]
        if self.num_loops(task.repetitive_critical_path) > self.num_loops(path):
            return
        if (self.num_loops(task.repetitive_critical_path) == self.num_loops(path)) and (
                len(task.repetitive_critical_path) >= len(path)):
            return
        task.repetitive_critical_path = path
        for parent in task.parents:
            self._update_repetitive_critical_path(parent, [parent] + task.repetitive_critical_path)
    '''
    def _update_path_to_repetition(self, task_name, path):
        task = self.tasks[task_name]
        if task.path_to_repetition is None:
            task.path_to_repetition = path
            for parent in task.parents:
                self._update_repetitive_critical_path(parent, [task.name] + task.path_to_repetition)
            return
        if len(task.path_to_repetition) >= len(path):
            return
        task.path_to_repetition = path
        for parent in task.parents:
            self._update_repetitive_critical_path(parent, [task.name] + task.path_to_repetition)
    '''

    def get_task(self, task_name):
        return self.tasks[task_name]

if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..')))
    from workflow_simulation.trace_reader import NextflowTraceReader
    tr = NextflowTraceReader()
    wf = tr.get_workflow('bacass')
    wfs = wf.get_copies_with_single_loop(3)
    wf = wfs[6].to_scheduler()
    wf = {
        1: set(),
        2: {1},
        3: {1, 3},
        4: {2},
        5: {4},
        }
    wf = Workflow(wf)
    for task in wf.tasks.values():
        print(task.name)
        print('\t', task.parents)
        print('\t', len(task.critical_path), wf.num_loops(task.critical_path), task.critical_path)
        print('\t', len(task.repetitive_critical_path), wf.num_loops(task.repetitive_critical_path), task.repetitive_critical_path)
        print('\t', task.descendants)
        print('\t', task.loop)
