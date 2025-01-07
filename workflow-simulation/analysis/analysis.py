import os
from matplotlib.ticker import PercentFormatter
from os import path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ..trace_reader import NextflowTraceReader
from ..scheduler.workflow import Workflow as SchedulerWorkflow

PATH = 'simulation-december-overhead'
WORKFLOWS = {
    'airrflow': True,
    'atacseq': True,
    'bacass': False,
    'chipseq': True,
    'cutandrun': False,
    'fetchngs': False,
    'hic': False,
    'mag': True,
    'methylseq': False,
    'rnaseq': True,
    'sarek': False,
    'scrnaseq': False,
    'smrnaseq': True,
    'taxprofiler': True,
    'viralrecon': True,
}

def analysis(args):
    # defaultanalysis()
    # non_critical_path_analysis()
    # show_plots()
    overview()
    # paths()

def paths():
    rel_path_lengths = get_paths()
    df1 = pd.DataFrame({'task': rel_path_lengths.keys(), 'value': rel_path_lengths.values()})
    df1['kind'] = 'unweighted'
    rel_path_lengths = get_weighted_paths()
    df2 = pd.DataFrame({'task': rel_path_lengths.keys(), 'value': rel_path_lengths.values()})
    df2['kind'] = 'weighted'
    df = pd.concat([df1, df2])
    print(df)

    df['workflow'] = df['task'].map(lambda task: task.split('.')[0].split('_')[1].lower())
    df = df[df['workflow'].map(lambda wf: WORKFLOWS[wf])]
    # sns.displot(kind='ecdf', data=df, x='value', col='workflow', hue='kind', col_wrap=4)
    sns.ecdfplot(data=df, x='value', hue='kind')
    plt.show()


def overview():
    print('hello')
    executions = pd.read_csv(path.join(PATH, 'executions.csv'), sep=';', index_col='id')
    assert len(executions.groupby('instance')['duration'].count().unique()) == 1
    instances = pd.read_csv(path.join(PATH, 'instances.csv'), sep=';', index_col='id')
    executions = executions.merge(instances, left_on='instance', right_on='id')
    executions = executions[executions['workflow'].map(lambda row: WORKFLOWS[row])]
    print('world')
    if True:
        relevant = get_non_critical_path_tasks(0.3)
        executions = executions[executions['loop'].map(lambda t: t in relevant)]
        long = get_long_tasks(10)
        executions = executions[executions['loop'].map(lambda t: t in long)]
        executions = executions[executions['repetitions'] == 20]
        # executions = executions[executions['workflow'] == 'viralrecon']
    medians = executions.groupby(['instance', 'strategy'])['duration'].median()
    def get_relative_makespan(row):
        median = medians.loc[(row['instance'], 'random')]
        return (row['duration'] - median) / median
    executions['relative_duration'] = executions.apply(get_relative_makespan, axis=1)
    executions['Strategy'] = executions['strategy'].replace({'random': 'Random', 'cpe': 'Rank', 'lid': 'LID', 'rcpe': 'RCPE', 'cper': 'CPER'})
    print(executions)
    print(executions['workflow'].unique())
    print(executions['repetitions'].unique())
    # sns.boxplot(data=executions, x='strategy', y='relative_duration')

    if False:
        os.mkdir('instance-distributions')
        for instance in executions['instance'].unique():
            data = executions[executions['instance'] == instance]
            sns.violinplot(data=data, x='relative_duration', y='strategy', split=True, inner='quart', fill=False)
            workflow = instances.loc[instance, 'workflow']
            plt.savefig(f'instance-distributions/{workflow}-{instance}.svg')
            plt.cla()
    if False:
        os.mkdir('smrnaseq-instance-distributions')
        data = executions[executions['workflow'] == 'smrnaseq']
        rel_path_lengths = get_paths()
        relevant = [t for t, v in rel_path_lengths.items() if v > 0.8]
        data = data[data['loop'].map(lambda t: t in relevant)]
        for instance in data['instance'].unique():
            data = executions[executions['instance'] == instance]
            sns.violinplot(data=data, x='relative_duration', y='strategy', split=True, inner='quart', fill=False)
            workflow = instances.loc[instance, 'workflow']
            loop = instances.loc[instance, 'loop']
            plt.savefig(f'smrnaseq-instance-distributions/{workflow}-{instance}-{loop}.svg')
            plt.cla()
    if True:
        ax = sns.violinplot(data=executions, x='relative_duration', y='Strategy', split=True, inner='quart', fill=False)
        ax.set_xlabel('Relative makespan difference')
        # ax.set_ylabel('Strategy')
        # ax.set_yticks(['random', 'Rank', 'LID', 'RCPE', 'CPER'])
        ax.xaxis.set_major_formatter(PercentFormatter(1))
        ax.figure.tight_layout()
        plt.savefig('simulation-results-subset.svg')
        plt.show()
    if False:
        # data = executions[executions['workflow'].map(lambda wf: wf in ['atacseq', 'mag'])]
        data = executions
        ax = sns.catplot(kind='violin', col='workflow', col_wrap=4, data=data, x='relative_duration', y='strategy', split=True, inner='quart', fill=False)
        ax.set_xlabels('relative makespan')
        ax.figure.tight_layout()
        # plt.savefig('simulation-results-split.svg')
        plt.show()
    if False:
        # data = executions[executions['workflow'].map(lambda wf: wf in ['atacseq', 'mag'])]
        data = executions
        workflow_improvements = {}
        for workflow in data['workflow'].unique():
            instance = data[data['workflow'] == workflow]['instance'].unique()[0]
            default = medians.loc[(instance, 'random')]
            rank = medians.loc[(instance, 'cpe')]
            workflow_improvements[workflow] = (rank - default) / default

        print(data.groupby(['workflow', 'strategy'])['duration'].count())
        ax = sns.boxplot(data=data, x='workflow', y='relative_duration', hue='strategy',
                         order=list(sorted(workflow_improvements.keys(), key=lambda k: workflow_improvements[k])),
                         hue_order=['random', 'cpe'])
        ax.axhline(0, color=".3", dashes=(2, 2))
        for i in range(0, len(data['workflow'].unique()), 2):
            ax.axvspan(xmin=i-.5, xmax=i+.5, fill=True, zorder=-10, alpha=.1)
        ax.set_ylabel('relative makespan')
        ax.tick_params(axis='x', labelrotation=90)
        ax.figure.tight_layout()
        # plt.savefig('evaluation-2-overview.svg')
        plt.show()

'''
                              instance strategy  duration    workflow  loop  repetitions  relative_duration
0     ef5c69fb7f7c82491c6bcfa3175b36b4   random   823.000    airrflow   NaN            0          -0.007436
1     ef5c69fb7f7c82491c6bcfa3175b36b4   random   820.000    airrflow   NaN            0          -0.011054
2     ef5c69fb7f7c82491c6bcfa3175b36b4   random   791.066    airrflow   NaN            0          -0.045950
3     ef5c69fb7f7c82491c6bcfa3175b36b4   random   830.000    airrflow   NaN            0           0.001006
4     ef5c69fb7f7c82491c6bcfa3175b36b4   random   830.000    airrflow   NaN            0           0.001006
...                                ...      ...       ...         ...   ...          ...                ...
2995  b1fd64e1883e4d3a516993819f5458b9      cpe   563.459  viralrecon   NaN            0          -0.044812
2996  b1fd64e1883e4d3a516993819f5458b9      cpe   564.893  viralrecon   NaN            0          -0.042381
2997  b1fd64e1883e4d3a516993819f5458b9      cpe   566.893  viralrecon   NaN            0          -0.038990
2998  b1fd64e1883e4d3a516993819f5458b9      cpe   515.193  viralrecon   NaN            0          -0.126633
2999  b1fd64e1883e4d3a516993819f5458b9      cpe   514.878  viralrecon   NaN            0          -0.127167
'''

def get_task_lengths():
    task_lengths = {}
    trace_reader = NextflowTraceReader()
    for workflow in trace_reader.get_workflows():
        workflow = trace_reader.get_workflow(workflow)
        for task in workflow.tasks.values():
            task_lengths[task.name] = task.run_time
    return task_lengths

def get_long_tasks(cutoff):
    non_critical_path_tasks = []
    trace_reader = NextflowTraceReader()
    for workflow in trace_reader.get_workflows():
        workflow = trace_reader.get_workflow(workflow)
        non_critical_path_tasks += [t.name for t in workflow.tasks.values() if t.run_time > cutoff]
    return non_critical_path_tasks


def get_paths():
    # rel_path_length maps from each task to the relative length of the longest paht its
    # on (relative to the longest path in the workflow
    rel_path_length = {}
    trace_reader = NextflowTraceReader()
    for workflow in trace_reader.get_workflows():
        workflow = trace_reader.get_workflow(workflow)
        scheduler_workflow = SchedulerWorkflow(workflow.to_scheduler())
        paths = [[task.name] for task in scheduler_workflow.tasks.values()
                 if len(task.parents) == 0]
        i = 0
        # len(paths) is reevaluated each iteration
        while i < len(paths):
            for child in workflow.abstract_tasks[paths[i][-1]].children:
                if child not in paths[i]:
                    paths.append(paths[i] + [child])
            i += 1
        # longest_paths maps from each task to the length of the longest path
        # it is on
        longest_paths = {}
        tasks_set = 0
        for p in paths:
            for task in p:
                if (task not in longest_paths) or (longest_paths[task] <= len(p)):
                    longest_paths[task] = len(p)
        longest_path = max(longest_paths.values())
        for task in workflow.abstract_tasks.keys():
            rel_path_length[task] = longest_paths[task] / longest_path
    return rel_path_length

def get_weighted_paths():
    # rel_path_length maps from each task to the relative length of the longest paht its
    # on (relative to the longest path in the workflow
    rel_path_length = {}
    trace_reader = NextflowTraceReader()
    for workflow in trace_reader.get_workflows():
        workflow = trace_reader.get_workflow(workflow)
        scheduler_workflow = SchedulerWorkflow(workflow.to_scheduler())
        paths = [[task.name] for task in scheduler_workflow.tasks.values()
                 if len(task.parents) == 0]
        i = 0
        # len(paths) is reevaluated each iteration
        while i < len(paths):
            for child in workflow.abstract_tasks[paths[i][-1]].children:
                assert child not in paths[i]
                # if child not in paths[i]:
                paths.append(paths[i] + [child])
            i += 1
        print(len(paths))
        # longest_paths maps from each task to the length of the longest path
        # it is on
        def path_len(path):
            return sum([workflow.tasks[t].run_time + 30 for t in path])
        longest_paths = {}
        tasks_set = 0
        for p in paths:
            for task in p:
                if (task not in longest_paths) or (longest_paths[task] <= path_len(p)):
                    longest_paths[task] = path_len(p)
        longest_path = max(longest_paths.values())
        for task in workflow.abstract_tasks.keys():
            rel_path_length[task] = longest_paths[task] / longest_path
    return rel_path_length

def get_non_critical_path_tasks(cutoff):
    rel_path_length = get_paths()
    return [t for t, v in rel_path_length.items() if v < cutoff]

def show_plots():
    instances = pd.read_csv(path.join(PATH, 'instances.csv'), sep=';', index_col='id')
    executions = pd.read_csv(path.join(PATH, 'executions.csv'), sep=';', index_col='id')

    all_strategies = list(executions['strategy'].unique())
    reference_strategy = 'random'
    strategies = set(all_strategies) - {reference_strategy}

    # calculate per strategy the mean duration for the executions of each instance
    # sets the index to the workflow instance and the columns to the different strategies
    df = executions.groupby(['instance', 'strategy'])['duration'].mean().reset_index().pivot(columns='strategy', index='instance')
    # drops 'duration' from the multiindex to flatten it
    df = df.droplevel(0, axis=1)
    # join with instances df to get repetition information
    df = instances.join(df).sort_values('workflow')
    # only consider non critical path tasks
    relevant = get_non_critical_path_tasks()
    df = df[df['loop'].map(lambda t: t in relevant)]
    # makes the mean durations relative
    for s in strategies:
        df[s] = (df[s] - df[reference_strategy]) / df[reference_strategy] * 100
    # adapt schema for plotting:
    # current schema: index: instance id, columns: workflow, loop, repetitions, strategy_a strategy_b ...
    # desired schema: index: any id, columns: workflow, instance id, loop, repetitions, strategy, value
    print(df.columns)
    new_schema = ['workflow', 'instance', 'loop', 'repetitions', 'strategy', 'value']
    plot_data = None
    for s in strategies:
        new_df = df.copy()
        new_df.drop(columns=set(all_strategies) - {s}, inplace=True)
        new_df['strategy'] = s
        new_df.reset_index(inplace=True)
        new_df.rename(columns={s: 'value', 'id': 'instance'}, inplace=True)
        # reorder columns to match schema
        new_df = new_df[new_schema]
        if plot_data is None:
            plot_data = new_df
        else:
            plot_data = pd.concat([plot_data, new_df], ignore_index=True)
    print(plot_data)
    sns.catplot(kind='box', data=plot_data, x='repetitions', y='value', hue='strategy', col='workflow', col_wrap=5)
    # sns.catplot(kind='box', data=plot_data, x='workflow', y='value')
    plt.show()


def non_critical_path_analysis():
    instances = pd.read_csv(
        path.join(PATH, 'instances.csv'), sep=';', index_col='id')
    instances['workflow'] = instances['loop'].apply(
        lambda task: task.split('.')[0][7:])
    # execution ids are unfortunately not unique
    executions = pd.read_csv(path.join(PATH, 'executions.csv'), sep=';')
    strategies = list(executions['strategy'].unique())
    reference_strategy = 'random'
    strategies.remove(reference_strategy)
    df = executions.groupby(['instance', 'strategy'])[
        'duration'].mean().reset_index().pivot(columns='strategy', index='instance')
    df = df.droplevel(0, axis=1)
    df = instances.join(df, lsuffix='_task').sort_values('workflow')
    relevant = get_non_critical_path_tasks()
    df = df[df['loop'].map(lambda t: t in relevant)]
    for s in strategies:
        df[s] = (df[s] - df[reference_strategy]) / df[reference_strategy] * 100
    df = df.groupby(['workflow', 'repetitions'])[strategies].mean(
    ).reset_index().pivot(columns=['repetitions'], index='workflow')  # .swaplevel(axis=1))
    df = df.swaplevel(axis=1).swaplevel(
        i=0, j=-1, axis=1).sort_values('repetitions', axis=1)
    print(df)
    df.to_csv(path.join('output', 'non_critical_path_analysis.csv'),
              float_format='%.1f')


def defaultanalysis():
    '''
    Create a table (.csv) summarizing the results of the simulation.
    '''
    instances = pd.read_csv(
        path.join(PATH, 'instances.csv'), sep=';', index_col='id')
    instances['workflow'] = instances['loop'].apply(
        lambda task: task.split('.')[0][7:])
    # execution ids are unfortunately not unique
    executions = pd.read_csv(path.join(PATH, 'executions.csv'), sep=';')
    strategies = list(executions['strategy'].unique())
    reference_strategy = 'random'
    strategies.remove(reference_strategy)
    df = executions.groupby(['instance', 'strategy'])[
        'duration'].mean().reset_index().pivot(columns='strategy', index='instance')
    df = df.droplevel(0, axis=1)
    df = instances.join(df, lsuffix='_task').sort_values('workflow')
    for s in strategies:
        df[s] = (df[s] - df[reference_strategy]) / df[reference_strategy] * 100
    df = df.groupby(['workflow', 'repetitions'])[strategies].mean(
    ).reset_index().pivot(columns=['repetitions'], index='workflow')  # .swaplevel(axis=1))
    df = df.swaplevel(axis=1).swaplevel(
        i=0, j=-1, axis=1).sort_values('repetitions', axis=1)
    print(df)
    df.to_csv(path.join('output', 'default_analysis.csv'), float_format='%.1f')
