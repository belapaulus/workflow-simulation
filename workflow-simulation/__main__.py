import hashlib
import sys
import os
import json
import argparse
from graphviz import Digraph
from .logger import Logger
from .cluster import Machine, Cluster
from .scheduler import Scheduler
from .scheduler.workflow import Workflow as SchedulerWorkflow
from .runtime import Runtime, WorkflowState
from .simulation import Simulation
from .trace_reader import NextflowTraceReader
from .trace_reader.argo_translator import generate_argo_workflow
from .analysis.analysis import analysis


def run_simulation(workflow, n_machines, cores, mem, strategy):
    simulation = Simulation()
    machines = [Machine(id, cores, mem) for id in range(n_machines)]
    cluster = Cluster(simulation, machines)
    scheduler = Scheduler(
        simulation, workflow.to_scheduler(), cluster, strategy)
    runtime = Runtime(scheduler, workflow.to_runtime())
    runtime.start()
    while runtime.state == WorkflowState.RUNNING:
        simulation.next_event()
    return runtime

def store_loop(args):
    parser = argparse.ArgumentParser(prog='python -m workflow-simulation store_loop')
    parser.add_argument('-l', '--loop-workflow-task', action='append')
    parser.add_argument('-o', '--output-dir', default='loop-workflows')
    args = parser.parse_args(args)
    if os.path.exists(args.output_dir):
        print('refusing to overwrite existing path', file=sys.stderr)
        exit(-1)
    os.mkdir(args.output_dir)
    loop_length = 20
    trace_reader = NextflowTraceReader()
    for task_name in args.loop_workflow_task:
        workflow_name = task_name.split('.')[0].split('_')[1].lower()
        workflow = trace_reader.get_workflow(workflow_name)
        workflow._add_loop(task_name, loop_length)
        d = workflow.to_dict()
        with open(os.path.join(args.output_dir, task_name), 'w') as f:
            f.write(json.dumps(d))

def run_loop(args):
    parser = argparse.ArgumentParser(prog='python -m workflow-simulation run_loop')
    parser.add_argument('-s', '--strategy', action='append', required=True)
    parser.add_argument('-l', '--loop-workflow-task', action='append')
    args = parser.parse_args(args)
    n_machines = 3
    n_cores = 2
    loop_length = 20
    logger = Logger(True)
    trace_reader = NextflowTraceReader()
    for task_name in args.loop_workflow_task:
        workflow_name = task_name.split('.')[0].split('_')[1].lower()
        workflow = trace_reader.get_workflow(workflow_name)
        workflow._add_loop(task_name, loop_length)
        loop_workflow = workflow
        logger.new_workflow_instance(workflow_name, loop_workflow.get_loop_name(), loop_length)
        for strategy in args.strategy:
            runtime = run_simulation(loop_workflow, n_machines, n_cores,
                                     loop_workflow.get_min_mem(),
                                     strategy)
            logger.new_workflow_execution(strategy, runtime)

def run_single(args):
    n_machines = 3
    n_cores = 2
    loop_length = 20
    strategies = ['random', 'cpe']
    logger = Logger(False)
    trace_reader = NextflowTraceReader()
    workflows = trace_reader.get_workflows()
    for workflow_name in workflows:
        workflow = trace_reader.get_workflow(workflow_name)
        logger.new_workflow_instance(workflow_name, None, 0)
        for strategy in strategies:
            for _ in range(100):
                runtime = run_simulation(workflow,
                                         n_machines,
                                         n_cores,
                                         workflow.get_min_mem(),
                                         strategy)
                logger.new_workflow_execution(strategy, runtime)



def run(args):
    parser = argparse.ArgumentParser(prog='python -m workflow-simulation run')
    # cluster args
    parser.add_argument('-c', '--cores', type=int, required=True)
    parser.add_argument('-m', '--memory', type=float)
    parser.add_argument('--use-min-mem', action='store_true', default=False)
    parser.add_argument('-n', '--n-machines', type=int, required=True)
    # workflow args
    # parser.add_argument('-l', '--loops', type=int, required=True)
    parser.add_argument('-r', '--repetitions', type=int,
                        required=True, action='append')
    parser.add_argument('-w', '--workflow', action='append')
    parser.add_argument('--all-workflows', action='store_true', default=False)
    # scheduler args
    parser.add_argument('-s', '--strategy', action='append', required=True)
    args = parser.parse_args(args)
    print(args)
    assert bool(args.workflow) != bool(args.all_workflows)
    assert bool(args.memory) != bool(args.use_min_mem)
    # assert args.trace_reader == 'nextflow'
    logger = Logger()
    trace_reader = NextflowTraceReader()
    workflows = args.workflow or trace_reader.get_workflows()
    strategies = args.strategy
    for workflow_name in workflows:
        workflow = trace_reader.get_workflow(workflow_name)
        for repetitions in args.repetitions:
            loop_workflows = workflow.get_copies_with_single_loop(repetitions)
            for loop_workflow in loop_workflows:
                logger.new_workflow_instance(workflow_name, loop_workflow.get_loop_name(), repetitions)
                for strategy in strategies:
                    for _ in range(10):
                        runtime = run_simulation(loop_workflow,
                                                 args.n_machines,
                                                 args.cores,
                                                 args.memory or loop_workflow.get_min_mem(),
                                                 strategy)
                        logger.new_workflow_execution(strategy, runtime)


def list(args):
    parser = argparse.ArgumentParser(prog='python -m workflow-simulation list')
    parser.add_argument('-n', '--names-only', action='store_true', default=False)
    args = parser.parse_args(args)
    trace_reader = NextflowTraceReader()
    if args.names_only:
        for wf_name in trace_reader.get_workflows():
            print(wf_name)
        return

    print('name', '#abstract tasks', '#task instances',
          'sum of task runtimes in minutes', 'circular deps')
    for wf_name in trace_reader.get_workflows():
        workflow = trace_reader.get_workflow(wf_name)
        print(f'{wf_name:15s} {len(workflow.abstract_tasks):3d} {len(workflow.tasks):3d} {int(sum([t.run_time for t in workflow.tasks.values()]) / 60):3d}')


def show(args):
    parser = argparse.ArgumentParser(prog='python -m workflow-simulation show')
    parser.add_argument('-w', '--workflow')
    args = parser.parse_args(args)
    trace_reader = NextflowTraceReader
    workflow = trace_reader.get_workflow(args.workflow)
    dot = Digraph()
    for name, task in workflow.task_instances.items():
        dot.node(name, f'{task.run_time}')
        for parent in task.parents:
            dot.edge(parent, name)
    # print(dot.source)
    dot.view()


def translate(args):
    parser = argparse.ArgumentParser(prog='python -m workflow-simulation translate')
    parser.add_argument('-s', '--scheduler-name',
                        required=True, action='append')
    parser.add_argument('--all-workflows', action='store_true')
    parser.add_argument('-o', '--output-dir', default='generated-workflows')
    parser.add_argument('-l', '--loop-workflow-task', action='append')
    # parser.add_argument('-w', '--workflow', action='append')
    args = parser.parse_args(args)
    assert bool(args.loop_workflow_task) != bool(args.all_workflows)
    print(args)
    if os.path.exists(args.output_dir):
        print('refusing to overwrite existing path', file=sys.stderr)
        exit(-1)
    os.mkdir(args.output_dir)
    trace_reader = NextflowTraceReader()
    if args.all_workflows:
        workflows = trace_reader.get_workflows()
        for scheduler_name in args.scheduler_name:
            for workflow_name in workflows:
                workflow = trace_reader.get_workflow(workflow_name)
                semantic_name = f'{workflow_name}-{scheduler_name}'
                # TODO pass semantic scheduler name for workflows with loops
                out = generate_argo_workflow( workflow, semantic_name, scheduler_name)
                with open(os.path.join(args.output_dir, f'{semantic_name}.yaml'), 'w') as file:
                    file.write(out)
    else:
        for scheduler_name in args.scheduler_name:
            for task_name in args.loop_workflow_task:
                workflow_name = task_name.split('.')[0].split('_')[1].lower()
                workflow = trace_reader.get_workflow(workflow_name)
                semantic_name = f'{workflow_name}-{uid(task_name)}-{scheduler_name}'
                out = generate_argo_workflow(
                    workflow, semantic_name, scheduler_name, loop_name=task_name)
                with open(os.path.join(args.output_dir, f'{semantic_name}.yaml'), 'w') as file:
                    file.write(out)


def uid(s=None):
    if s == None:
        return hashlib.md5(f'{time.time()}'.encode()).hexdigest()
    return hashlib.md5(s.encode()).hexdigest()


if __name__ == '__main__':
    cmd = sys.argv[1]
    args = sys.argv[2:]
    cmds = {
        'run': run,
        'run_loop': run_loop,
        'run_single': run_single,
        'store_loop': store_loop,
        'list': list,
        'show': show,
        'analysis': analysis,
        'translate': translate,
    }
    try:
        func = cmds[cmd]
    except KeyError:
        print('unkown command', file=sys.stderr)
        exit(-1)
    func(args)
