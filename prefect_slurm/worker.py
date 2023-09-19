"""
This module provides a prefect worker implementation to interact with
SLURM workload managers used in HPC environments.

TODO:
1) The worker currently uses the API-based backend. This should be made
   a choice at work pool generation.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import anyio.abc
from prefect.blocks.system import Secret
from prefect.client.schemas import FlowRun
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.utilities.filesystem import relative_path_to_current_platform
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, HttpUrl, validator

from prefect_slurm.api.jobs import JobDefinition
from prefect_slurm.slurm import APIBasedSlurmBackend, SlurmJobStatus


class SlurmJobConfiguration(BaseJobConfiguration):

    """
    SlurmJobConfiguration defines the SLURM configuration for a particular job.

    Currently only the most important options for a job are covered. This includes:

    1) which partition to run on
    2) walltime, number of nodes and number of cpus
    3) working directory
    4) credentials to access SLURM
    5) a conda environment to initiate for the run (that should probably be outsourced)
    """

    stream_output: bool = Field(default=True)
    working_dir: Optional[Path] = Field(default=None)

    num_nodes: int = Field(default=1)
    num_processes_per_node: int = Field(default=72)
    max_walltime: str = Field(
        default="24:00:00", regex="^[0-9]{1,9}:[0-5][0-9]:[0-5][0-9]"
    )

    slurm_queue: str = Field(
        template="small",
        title="Slurm Queue",
        description="The Slurm queue jobs are submitted to.",
    )

    slurm_user: str = Field(
        template="username",
        title="Username",
        description="The username used to authenticate with the slurm API.",
    )

    slurm_token: Secret = Field(
        template="api-token",
        title="API Token",
        description="The bearer token to authenticate with the Slurm API.",
    )

    slurm_url: HttpUrl = Field(
        template="http://slurm-api-host",
        title="API URL",
        description="URL of the Slurm API endpoint.",
    )

    conda_env: Optional[str] = Field(default=None)

    update_interval_sec: int = Field(
        default=30,
        title="Update Interval",
        description="Interval in seconds to poll the API for job updates",
    )

    @validator("working_dir")
    def validate_command(cls, v):
        """
        Make sure that the working directory is formatted for the current platform.
        """
        if v:
            return relative_path_to_current_platform(v)
        return v

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment: Optional["DeploymentResponse"] = None,
        flow: Optional["Flow"] = None,
    ):
        """
        Prepare the flow run by setting some important environment variables and
        adjusting the execution environment.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow)

        # self.env = {**os.environ, **self.env}

        # self.command = (
        #     f"{sys.executable} -m prefect.engine"
        #     if self.command == self._base_flow_run_command()
        #     else self.command


class SlurmVariables(BaseVariables):

    """
    SlurmVariables defines a set of variables to configure SlurmJobConfigurations at
    runtime. These variables can be defined at submission time of a new job and will
    be used to template a job definition.
    """

    stream_output: bool = Field(
        default=True,
        description=(
            "If enabled, workers will stream output from flow run processes to "
            "local standard output."
        ),
    )

    working_dir: Optional[Path] = Field(
        default=None,
        title="Working Directory",
        description=(
            "If provided, workers will execute flow run processes within the "
            "specified path as the working directory. Otherwise, a temporary "
            "directory will be created."
        ),
    )

    num_nodes: int = Field(
        default=1,
        title="Number of requested compute nodes",
        description="The total number of requested compute nodes for the job. "
        "Needs to be compatible with the queue policy.",
    )

    num_processes_per_node: int = Field(
        default=72,
        title="Number of processes per node",
        description="Number of MPI processes per node. Should usually be set to the "
        "number of CPUs per node for pure MPI jobs or a integer devisable "
        "of the number of CPus per node for hybrid MPI-OpenMP jobs. Be "
        "mindful of the need to set OpenMP environment variables accordingly.",
    )

    max_walltime: str = Field(
        default="24:00:00",
        title="Maximum Walltime",
        description="Maximum walltime for job execution. Format: [hh:mm:ss].",
    )

    update_interval_sec: int = Field(
        default=30,
        title="Update Interval",
        description="Interval in seconds to poll the API for job updates",
    )

    conda_env: Optional[str] = Field(
        default=None,
        title="Conda Environment",
        description="Name of the Conda environment to run in. "
        "The environment must be pre-installed on the "
        "infrastructure environment",
    )

    slurm_user: str = Field(
        template="username",
        title="Username",
        description="The username used to authenticate with the slurm API.",
    )

    slurm_token: Secret = Field(
        template="api-token",
        title="API Token",
        description="The bearer token to authenticate with the Slurm API.",
    )

    slurm_url: HttpUrl = Field(
        template="http://slurm-api-host",
        title="API URL",
        description="URL of the Slurm API endpoint.",
    )


class SlurmWorkerResult(BaseWorkerResult):
    """Contains information about the final state of a completed slurm job"""


class SlurmWorker(BaseWorker):

    """
    A prefect worker to initialise a SLURM job as infrastructure to run
    flows. The worker will submit a batch job to execute a prefect engine, monitor
    the job status and cancel the SLURM job if the work flow is canceled.

    The worker currently only supports API-based communication with SLURM.
    """

    type = "slurm"
    job_configuration = SlurmJobConfiguration
    job_configuration_variables = SlurmVariables

    _description = "Execute flow runs as jobs on a SLURM cluster."

    _display_name = "Slurm Worker"

    async def run(
        self,
        flow_run: FlowRun,
        configuration: SlurmJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ):
        """
        Run a workflow on slurm by submitting a new batch job to the queue
        to execute the prefect engine. Then monitor the job and report the
        result.
        """

        command = configuration.command
        if not command:
            command = f"{sys.executable} -m prefect.engine"

        flow_run_logger = self.get_flow_run_logger(flow_run)

        backend = APIBasedSlurmBackend(
            endpoint=configuration.slurm_url,
            username=configuration.slurm_user,
            token=configuration.slurm_token,
        )

        env = configuration._base_environment()
        env.update(configuration.env)

        script = self._submit_script(configuration)
        job_def = JobDefinition(
            partition=configuration.slurm_queue,
            time_limit=configuration.max_walltime,
            tasks=configuration.num_processes_per_node * configuration.num_nodes,
            nodes=configuration.num_nodes,
            current_working_directory=configuration.working_dir,
            environment=configuration.env,
        )

        print(script)
        slurm_job_id = await backend.submit(job_def, script)

        flow_run_logger.info(f"Slurm job {slurm_job_id} submitted.")

        if task_status:
            # Use a unique ID to mark the run as started. This ID is later used to
            # tear down infrastructure if the flow run is cancelled.
            task_status.started(slurm_job_id)

        # Monitor the execution
        job_status = await self._watch_job(slurm_job_id, configuration)
        exit_code = 0 if job_status == SlurmJobStatus.COMPLETED else -1

        flow_run_logger.info(
            f"Slurm job {slurm_job_id} finished with status {job_status}."
        )

        return SlurmWorkerResult(status_code=exit_code, identifier=str(slurm_job_id))

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: SlurmJobConfiguration,
        grace_seconds: int = 30,
    ):
        """
        Cancel the slurm job to kill the infrastructure. The slurm job is
        identified by the infrastructure_pid parameter.
        """

        backend = APIBasedSlurmBackend(
            endpoint=configuration.slurm_url,
            username=configuration.slurm_user,
            token=configuration.slurm_token,
        )

        await backend.kill(infrastructure_pid, grace_seconds=grace_seconds)
        return

    @staticmethod
    def _base_flow_run_command() -> str:
        """
        Generate a command for a flow run job.
        """
        return "python -m prefect.engine"

    async def _watch_job(
        self, job_id: int, configuration: SlurmJobConfiguration
    ) -> SlurmJobStatus:
        """
        Watch a running slurm job by periodically polling the
        API for the job status.
        """

        backend = APIBasedSlurmBackend(
            endpoint=configuration.slurm_url,
            username=configuration.slurm_user,
            token=configuration.slurm_token,
        )

        status = None

        while status in [
            None,
            SlurmJobStatus.PENDING,
            SlurmJobStatus.PREEMPTED,
            SlurmJobStatus.RUNNING,
        ]:
            status = await backend.status(job_id)
            await asyncio.sleep(configuration.update_interval_sec)

        return status

    def _submit_script(self, configuration: SlurmJobConfiguration) -> str:
        """
        Generate the submit script for the slurm job
        """
        script = ["#!/bin/bash"]
        script += ["source /beegfs/home/k/kramerd/.bashrc"]
        # script += self.pre_run
        command = configuration.command or self._base_flow_run_command()

        if configuration.conda_env:
            script += [f"conda run -n {configuration.conda_env} {command}"]
        else:
            script += [command]
        # script += self.post_run

        return "\n".join(script)
