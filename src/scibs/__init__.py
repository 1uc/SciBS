# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

from .job import Job

from .dependencies import Singleton, AfterOK, AfterAny
from .dependency_policies import SLURMDependencyPolicy

from .resources import Resource
from .resources import MPIResource, OMPResource, CUResource, CU
from .resources import JustCoresResource, JustGPUsResource

from .utilities import hhmm, hhmmss

from .submission_policies import SubmissionPolicy, StdOutSubmissionPolicy
from .submission_policies import SubprocessSubmissionPolicy, DebugSubmissionPolicy
from .submission_policies import MultiSubmissionPolicy, SLURMSubmissionPolicy
from .wrap_policies import WrapPolicy, DefaultWrapPolicy, EulerWrapPolicy
from .wrap_policies import SBatchWrapPolicy
from .resource_policies import DefaultResourcePolicy, GPUResourcePolicy

from .schedules import Schedule, GreedySchedule
from .schedules import LocalResources, LocalGPUResources

from .scibs import SciBS
from .lsf import LSF, EulerLSF
from .local_bs import LocalBS
from .sequential_local_bs import SequentialLocalBS
from .slurm import SLURM, BB5, SBatchBB5
