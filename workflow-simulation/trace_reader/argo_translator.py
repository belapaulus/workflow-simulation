import hashlib
from jinja2 import Template

# TODO properly translate loops

TEMPLATE = '''
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: {{ workflow }}-
spec:
  entrypoint: graph
  schedulerName: {{ scheduler }}
  templates:
  - name: graph
    dag:
      tasks:
      {% for task in tasks %}
      - name: {{ task.id }}
        template: task
        {% if task.parents %}
        dependencies: [{% for dep in task.parents %}{{ dep }}{{ ", " if not loop.last else "" }}{% endfor %}]
        {% endif %}
        arguments:
            parameters: [{name: task-info, value: '{{ task.id }}'}, {name: runtime, value: {{ task.run_time }}}]
      {% endfor %}
  - name: task
    inputs:
      parameters:
      - name: task-info
      - name: runtime
    nodeSelector:
      cws: worker
    script:
      image: alpine:3.7
      command: [sh]
      source: |
        START=$(date -Iseconds -u)
        sleep {{ '{{ inputs.parameters.runtime }}' }}
        echo "{{ '{{ inputs.parameters.task-info }}' }},$START,$(date -Iseconds -u),{{ '{{ inputs.parameters.runtime}}'}}"
      resources:
        requests:
          cpu: 1
'''

LOOP_TEMPLATE = '''
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: {{ workflow }}-
spec:
  entrypoint: graph
  schedulerName: {{ scheduler }}
  templates:
  - name: graph
    dag:
      tasks:
      {% for task in tasks %}
      - name: {{ task.id }}
        template: task
        {% if task.parents %}
        dependencies: [{% for dep in task.parents %}{{ dep }}{{ ", " if not loop.last else "" }}{% endfor %}]
        {% endif %}
        arguments:
            parameters: [{name: task-info, value: '{{ task.id }}'}, {name: runtime, value: {{ task.run_time }}}]
      {% endfor %}
      - name: {{ loop_task.id }}
        template: loop
        {% if loop_task.parents %}
        dependencies: [{% for dep in loop_task.parents %}{{ dep }}{{ ", " if not loop.last else ""}}{% endfor %}]
        {% endif %}
        arguments: 
            parameters: [{name: iteration_counter, value: '20'}]
  - name: loop
    inputs:
      parameters:
      - name: iteration_counter
    steps:
    - - name: task
        template: task
        arguments:
            parameters: [{name: task-info, value: '{{ loop_task.id }}'}, {name: runtime, value: {{ loop_task.run_time }}}]
    - - name: condition
        template: condition
        arguments:
          parameters: [{name: iteration_counter, value: "{{ '{{ inputs.parameters.iteration_counter }}' }}"}]
    - - name: repeat
        template: loop
        arguments:
          parameters: [{name: iteration_counter, value: "{{ '{{ steps.condition.outputs.result }}' }}"}]
        when: "{{ '{{ steps.condition.outputs.result }}' }} != done"
  - name: condition
    inputs:
      parameters:
      - name: iteration_counter
    nodeSelector:
      cws: worker
    script:
      image: python:alpine3.6
      command: [python]
      source: |
        current_iteration = {{ '{{ inputs.parameters.iteration_counter }}' }}
        next_iteration = current_iteration - 1
        if next_iteration == 0:
                print('done')
        else:
                print(next_iteration)
  - name: task
    inputs:
      parameters:
      - name: task-info
      - name: runtime
    nodeSelector:
      cws: worker
    script:
      image: alpine:3.7
      command: [sh]
      source: |
        START=$(date -Iseconds -u)
        sleep {{ '{{ inputs.parameters.runtime }}' }}
        echo "{{ '{{ inputs.parameters.task-info }}' }},$START,$(date -Iseconds -u),{{ '{{ inputs.parameters.runtime}}'}}"
      resources:
        requests:
          cpu: 1
'''


def uid(s):
    h = hashlib.md5(s.encode())
    return 'task-' + h.hexdigest()


class Task:
    def __init__(self, task_instance):
        self.id = uid(task_instance.name)
        self.name = task_instance.name
        self.parents = [uid(parent) for parent in task_instance.parents]
        self.run_time = task_instance.run_time

def generate_argo_loop_workflow(workflow, workflow_name, scheduler_name, loop_name):
    # task names are unique
    tasks = [Task(t) for t in workflow.tasks.values() if t.name != loop_name]
    template = Template(LOOP_TEMPLATE)
    data = {
        'workflow': workflow_name,
        'scheduler': scheduler_name,
        'tasks': tasks,
        'loop_task': Task(workflow.tasks[loop_name])
    }
    render = '\n'.join([l for l in template.render(
        **data).split('\n') if not l.strip() == ''])
    # print(render)
    return render

def generate_argo_workflow(workflow, workflow_name, scheduler_name, loop_name=None):
    if loop_name is not None:
        return generate_argo_loop_workflow(workflow, workflow_name, scheduler_name, loop_name)
    # task names are unique
    tasks = [Task(t) for t in workflow.tasks.values()]
    template = Template(TEMPLATE)
    data = {
        'workflow': workflow_name,
        'scheduler': scheduler_name,
        'tasks': tasks,
    }
    render = '\n'.join([l for l in template.render(
        **data).split('\n') if not l.strip() == ''])
    # print(render)
    return render
