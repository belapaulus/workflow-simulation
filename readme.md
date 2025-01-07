# workflow-simulation

Use WfInstance traces to explore different online scheduling stratetgies.

# Components

## Simulation

The simulation consists of a queue of events. Events can be added and the next
event can be called. An event has a time, a callback function (i.e. what is
supposed to happen at that time) and some user provided data that will be
passed to the callback function. Calling an event means that the simulation
performs some basic checks, sets its own time to the event time, and calls the
event's callback function with the user provided data.

## Runtime

The runtime component takes on the role of a workflow engine. It has an
interface to the scheduler and primarily manages the worklfow object. The
runtime component provides a task instance class. The task instance objects are
passed between the runtime, the scheduler and the cluster.

When a runtime object is created it registers the operate function as a
callback function with the scheduler that shall be called when a task has
finished. When the start method is called the workflow state is set to running
and the operate function is called for the first time.

The operate function first checks if all tasks are done, if that is the case,
the workflow state is set to 'done'. Afterwards it determines which tasks
are ready to run and adds them to a batch in the scheduler.

## Scheduler

The scheduler component has its own representation of the workflow created
from a simple dictionary that contains for each node as a key its parents as
values. The scheduler class workflow representation keeps track of the
critical path, descendants and following loop nodes ahead for each task in the
workflow.

The scheduler provides an add_to_batch function that either adds tasks to an
existing batch or creates a new batch, adds the tasks to that batch and
registers a timeout event in the simulation, that will close the batch and
try to schedule the tasks.

Scheduling a batch consists of two parts: first, the tasks are ordered
according to the priorization strategy, second the tasks are according to their
priorities assigned to nodes.

the task finish callback provided by the runtime is passed on to the cluster.

## Cluster

the cluster object maintains a set of machines and their memory. Further it
marks tasks as running and registers their finish event with the simulaiton or 
marks them as done and calls the runtime task finish callback
