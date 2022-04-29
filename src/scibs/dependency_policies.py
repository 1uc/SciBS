# SPDX-License-Identifier: MIT
# Copyright (c) 2022 Luc Grosheintz-Laval

import scibs


class SLURMDependencyPolicy:
    def __call__(self, dependency):
        if isinstance(dependency, scibs.Singleton):
            deps_str = "singleton"

        elif isinstance(dependency, scibs.AfterOK):
            deps_str = f"afterok:{dependency.job_id}"

        elif isinstance(dependency, scibs.AfterAny):
            deps_str = f"afterany:{dependency.job_id}"

        else:
            raise NotImplementedError("Missing case.")

        return f"--dependency={deps_str}"
