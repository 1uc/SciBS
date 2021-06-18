# SciBS: Scientific Batch Systems
A feeble attempt at abstracting away the differences between the three import
batch systems used in scientific computing: SLURM, LSF and no batch system.

## Brief Overview
Some commands need to be run on a cluster, through a batch system, e.g. `./foo
--bar` turns into `bsub ./foo --bar`, usually with some additional commands.

Sometimes, one wants to run such a command from within Python using
`subprocess`. SciBS provides the basic infrastructure to wrap and run such
command either locally or on a cluster (using a batch system).

### Resources
These jobs need to specify their resources, e.g. 2 cores for 3 hours and 5GB
RAM per core, using MPI. In `scibs` this could be

    r = scibs.MPIResource(
        n_tasks=2,
        wall_clock=datetime.timedelta(hours=2)
        mem_per_task=5e9
    )

Other resource types exist, please consult the code.

### Jobs
A job consists of a 'basic command', previously `./foo --bar`, a working
directory and the resources required to run the job.

    job = scibs.Job(cmd=["./foo", "--bar"], resources=r, cwd="$HOME")

### Batch System
Jobs can be submitted to a batch system (derived from `SciBS`), e.g.

    with scibs.LSF(...) as lsf:
      for job in jobs:
          lsf.submit(job)

This takes care of wrapping the basic command `./foo --bar`. First for MPI

    mpirun -np 2 ./foo --bar

and then so that it can be submitted to the batch system

    bsub -W 2:00 -R rusage[mem=5000] -n 2 'mpirun -np 2 ./foo --bar'

These transformation can be configured by means of Policies, i.e. one policy for
wrapping the basic command, and another policy for submitting the job to the
batch system.

### No Batch System
Naturally, one doesn't always run on a cluster. No problem, for these cases
there's a `SciBS` which just submits stuff on the local computer:

    with scibs.LocalBS(...) as lsb:
      for job in jobs:
          lbs.submit(job)

which will attempt to run as many jobs simultaneously as the host can deal
with.

## Configuration
This "library" should be treated as code. Code to make stuff happen in your
environment. It is reasonably easy to implement the specific cases one needs,
implementing them all is hard. Therefore, please treat the code as code and
extend it such that it covers you needs. It's open-source in the hope that it
may help others cope with our scientific batch systems.

## Licence
The software was initially developed at ETH Zurich and is distributed under the
MIT license.

Please note that the project is in a very immature state.
