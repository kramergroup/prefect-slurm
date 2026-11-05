from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from prefect_slurm.api.jobs import JobDefinition
from prefect_slurm.slurm import CLIBasedSlurmBackend

SCRIPT = "#!/bin/bash\n#SBATCH --mail-type=FAIL\n\nsource ~/.bashrc\nprefect flow-run execute"


def test_exports_go_after_sbatch_directives():
    script = CLIBasedSlurmBackend._export_environment(
        SCRIPT, {"PREFECT__FLOW_RUN_ID": "abc", "PREFECT_API_URL": "https://x/api"}
    )
    assert script.split("\n") == [
        "#!/bin/bash",
        "#SBATCH --mail-type=FAIL",
        "",
        "export PREFECT__FLOW_RUN_ID=abc",
        "export PREFECT_API_URL=https://x/api",
        "source ~/.bashrc",
        "prefect flow-run execute",
    ]


def test_export_values_are_shell_quoted_and_none_skipped():
    script = CLIBasedSlurmBackend._export_environment(
        "#!/bin/bash\necho", {"A": "x y,z=1", "B": "it's", "C": None}
    )
    assert "export A='x y,z=1'" in script
    assert "export B='it'\"'\"'s'" in script
    assert "C" not in script


def test_empty_environment_leaves_script_unchanged():
    assert CLIBasedSlurmBackend._export_environment(SCRIPT, {}) == SCRIPT


@pytest.mark.asyncio
async def test_submit_does_not_pass_export_to_sbatch():
    backend = CLIBasedSlurmBackend("host", "user", "pw")
    backend._run_remote_command = AsyncMock(
        return_value=SimpleNamespace(stdout="42\n", stderr="")
    )
    job = JobDefinition(
        partition="small",
        time_limit="01:00:00",
        current_working_directory="/scratch",
        environment={"PREFECT__FLOW_RUN_ID": "abc"},
    )

    assert await backend.submit(job, StringIO(SCRIPT)) == 42

    kwargs = backend._run_remote_command.call_args.kwargs
    assert "--export" not in kwargs["cmd"]
    assert "--partition=small" in kwargs["cmd"]
    assert "export PREFECT__FLOW_RUN_ID=abc" in kwargs["in_stream"].read()
