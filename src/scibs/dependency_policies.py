# SPDX-License-Identifier: MIT
# Copyright (c) 2022 Luc Grosheintz-Laval

class SLURMDependencyPolicy:
    def __call__(self, dependency):
        condition = dependency["condition"]

        if condition == "singleton":
            deps_str = "singleton"

        else:
            job_id = dependency["job_id"]
            deps_str = f"{condition}:{job_id}"

        return f"--dependency={deps_str}"
