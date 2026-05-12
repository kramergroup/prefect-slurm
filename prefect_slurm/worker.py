"""
This module provides a prefect worker implementation to interact with
SLURM workload managers used in HPC environments.

TODO:
1) The worker currently uses the API-based backend. This should be made
   a choice at work pool generation.
"""

import asyncio
import sys
from io import StringIO
from pathlib import Path
from typing import Optional

import anyio.abc
from jinja2 import Template
from prefect.blocks.core import Block
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
from pydantic import Field, HttpUrl, SecretStr, field_validator

from prefect_slurm.api.jobs import JobDefinition
from prefect_slurm.slurm import (
    APIBasedSlurmBackend,
    CLIBasedSlurmBackend,
    SlurmBackend,
    SlurmJobStatus,
)


class SlurmAPIConnection(Block):
    """
    SlurmAPIConnection defines a SLURM API endpoint.
    """

    username: str = Field(
        title="Username", description="The name of  the user interacting with SLURM"
    )

    auth_token: SecretStr = Field(
        title="Token secret name",
        description="The name of the secret holding the API authenication token",
    )

    endpoint: HttpUrl = Field(
        title="SLURM API Endpoint URL", description="The URL of the SLURM API endpoint"
    )

    insecure: bool = Field(
        default=False,
        title="Insecure connection",
        description="Allow insecure connection with SLURM API",
    )

    _logo_url = "https://upload.wikimedia.org/wikipedia/commons/3/3a/Slurm_logo.svg"
    _description = "Configures an API endpoint for a SLURM cluster"


class SlurmSSHConnection(Block):
    """
    SlurmSSHConnection defined a ssh connection for the slurm cluster.
    """

    host: str = Field(
        title="SSH hostname",
        description="DNS name or IP of the login node",
    )

    username: str = Field(
        title="Username", description="The name of  the user interacting with SLURM"
    )

    password: SecretStr = Field(
        title="Password", description="The password of the user interacting with SLURM"
    )

    port: int = Field(
        default=22, title="Port", description="The SSH port to contact the cluster on"
    )

    _logo_url = "https://upload.wikimedia.org/wikipedia/commons/3/3a/Slurm_logo.svg"
    _description = "Configures a ssh connection for a SLURM cluster"


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

    working_dir: Optional[Path] = Field(
        default=None,
        title="Working directory",
        description="Working directory for the job",
    )

    num_nodes: int = Field(default=1)
    num_processes_per_node: int = Field(default=72)
    max_walltime: str = Field(
        default="24:00:00", pattern="^[0-9]{1,9}:[0-5][0-9]:[0-5][0-9]"
    )

    queue: str = Field(
        default="small",
        title="Slurm Queue",
        description="The Slurm queue jobs are submitted to.",
    )

    connection_name: Optional[str] = Field(
        default=None,
        title="Slurm connection",
        description="""
            The connection block name to access the SLURM manager. This can either
            be a API endpoint or a SSH connection. When omitted, the worker derives
            the name from hpc_system and the authenticated user identity.
        """,
    )

    hpc_system: Optional[str] = Field(
        default=None,
        title="HPC system name",
        description=(
            "Short identifier for the HPC system (e.g. 'issy'). Set once as a "
            "literal in the work pool's base-job-template.json — not in variables. "
            "Used to derive the connection block name as "
            "'slurm-{hpc_system}-{submitter}' when connection_name is not set."
        ),
    )

    update_interval_sec: int = Field(
        default=30,
        title="Update Interval",
        description="Interval in seconds to poll the API for job updates",
    )

    script_template: Optional[str] = Field(
        default=None,
        title="Submit script template",
        description=""""
            Allows to provide a template for generating submit scripts. If provided,
            a custom submit script is generated. This allows to tweak the
            execution environment.
        """,
    )

    image: Optional[str] = Field(
        default=None,
        title="Docker image",
        description="The name of a docker image for packaging the code",
    )

    pre_command: Optional[str] = Field(
        default=None,
        title="Pre-run command",
        description="""
            A shell command to execute before the flow is executed. This is
            provided as an environment variable to the script_template and
            can be used to initiate environments etc.
        """,
        examples=["conda activate prefect", "conda create -n mytempenv"],
    )

    post_command: Optional[str] = Field(
        default=None,
        title="Post-run command",
        description="""
            A shell command to execute after the flow is executed. This is
            provided as an environment variable to the script_template and
            can be used to clean-up environments etc.
        """,
        examples=["conda remove mytempenv"],
    )

    @field_validator("working_dir")
    @classmethod
    def validate_working_directory(cls, v):
        """
        Make sure that the working directory is formatted for the current platform.
        """
        if v:
            return relative_path_to_current_platform(v)
        return v

    @field_validator("script_template", mode="before")
    @classmethod
    def ensure_script_template_is_string(cls, v):
        """
        Ensures that the script template is a single string with newline
        characters if needed.
        """

        if type(v) == list:
            return "\n".join(v)

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

    queue: str = Field(
        default="small",
        title="Slurm Queue",
        description="The Slurm queue jobs are submitted to.",
    )

    connection_name: Optional[str] = Field(
        default=None,
        title="Slurm connection",
        description="""
            The name of a connection block to access the SLURM manager. This can either
            be a API endpoint or a SSH connection. When omitted, the worker derives
            the name from hpc_system and the authenticated user identity.
        """,
    )

    script_template: Optional[str] = Field(
        default=None,
        title="Submit script template",
        description="""
            Allows to provide a template for generating submit scripts. If provided,
            a custom submit script is generated. This allows to tweak the
            execution environment.
        """,
    )

    image: Optional[str] = Field(
        title="Docker image",
        description="The name of a docker image for packaging the code",
    )

    pre_command: Optional[str] = Field(
        default=None,
        title="Pre-run command",
        description="""
            A shell command to execute before the flow is executed. This is
            provided as an environment variable to the script_template and
            can be used to initiate environments etc.
        """,
        examples=["conda activate prefect", "conda create -n mytempenv"],
    )

    post_command: Optional[str] = Field(
        default=None,
        title="Post-run command",
        description="""
            A shell command to execute after the flow is executed. This is
            provided as an environment variable to the script_template and
            can be used to clean-up environments etc.
        """,
        examples=["conda remove mytempenv"],
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

        connection_name = self._resolve_connection_name(configuration, flow_run)
        backend = await self._create_backend_from_name(connection_name)

        env = configuration._base_environment()
        env.update(configuration.env)

        script = self._submit_script(configuration)
        job_def = JobDefinition(
            partition=configuration.queue,
            time_limit=configuration.max_walltime,
            tasks=configuration.num_processes_per_node * configuration.num_nodes,
            nodes=configuration.num_nodes,
            current_working_directory=configuration.working_dir,
            environment=configuration.env,
            name=f"prefect-{flow_run.id}",
        )

        slurm_job_id = await backend.submit(job_def, StringIO(script))

        flow_run_logger.info(
            f"Submitted job '{job_def.name}' for run '{configuration.name}' "
            f"using connection {connection_name}; "
            f"slurm job id {slurm_job_id}."
        )

        if task_status:
            # Use a unique ID to mark the run as started. This ID is later used to
            # tear down infrastructure if the flow run is cancelled.
            task_status.started(f"{slurm_job_id}@{connection_name}")

        # Monitor the execution
        job_status = await self._watch_job(
            slurm_job_id, connection_name, configuration.update_interval_sec
        )
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

        job_id, conn_name = self._parse_infrastructure_pid(
            infrastructure_pid, fallback_connection=configuration.connection_name
        )
        backend = await self._create_backend_from_name(conn_name)
        await backend.kill(job_id, grace_seconds=grace_seconds)
        return

    @staticmethod
    def _base_flow_run_command() -> str:
        """
        Generate a command for a flow run job.
        """

        return "python -m prefect.engine"

    def _resolve_connection_name(
        self,
        configuration: SlurmJobConfiguration,
        flow_run: FlowRun,
    ) -> str:
        """Return the connection block name to use for this flow run.

        Explicit connection_name always wins. Otherwise derives the name
        from hpc_system and the authenticated user identity supplied by
        the reverse proxy via flow_run.created_by.display_value.
        """
        if configuration.connection_name:
            return configuration.connection_name
        cb = flow_run.created_by
        if configuration.hpc_system and cb and cb.type == "USER" and cb.display_value:
            return f"slurm-{configuration.hpc_system}-{cb.display_value}"
        raise AttributeError(
            "Cannot determine SLURM connection: set 'connection_name' explicitly, "
            "or set 'hpc_system' and ensure the reverse proxy injects user identity "
            "via X-Remote-User (or configured PREFECT_SERVER_USER_HEADER). "
            f"[hpc_system={configuration.hpc_system!r}, "
            f"created_by={cb!r}, "
            f"type={cb.type if cb else None}, "
            f"display_value={cb.display_value if cb else None}]"
        )

    @staticmethod
    def _parse_infrastructure_pid(
        infrastructure_pid: str,
        fallback_connection: Optional[str] = None,
    ):
        """Parse infrastructure_pid into (job_id, connection_name).

        New format: "{job_id}@{connection_name}" — written by run().
        Old format: "{job_id}" — written before this feature existed;
                    fallback_connection is used (from configuration.connection_name).
        """
        if "@" in infrastructure_pid:
            job_id, conn_name = infrastructure_pid.split("@", 1)
            return job_id, conn_name
        if fallback_connection:
            return infrastructure_pid, fallback_connection
        raise AttributeError(
            "Cannot determine SLURM connection from infrastructure_pid "
            f"'{infrastructure_pid}': no '@' separator and no fallback connection set."
        )

    async def _create_backend_from_name(self, connection_name: str) -> SlurmBackend:
        """Load the connection block identified by connection_name and return
        the appropriate backend (API or SSH)."""
        try:
            connection_block = await SlurmAPIConnection.load(connection_name)
            return APIBasedSlurmBackend(
                endpoint=connection_block.endpoint,
                username=connection_block.username,
                token=connection_block.auth_token,
                insecure=connection_block.insecure,
            )
        except ValueError:
            try:
                connection_block = await SlurmSSHConnection.load(connection_name)
                return CLIBasedSlurmBackend(
                    host=connection_block.host,
                    username=connection_block.username,
                    password=connection_block.password,
                )
            except ValueError:
                raise AttributeError(
                    f"No valid connection block found for '{connection_name}'. "
                    "Create a SlurmAPIConnection or SlurmSSHConnection block "
                    "with that name."
                )

    async def _watch_job(
        self,
        job_id: int,
        connection_name: str,
        update_interval_sec: int = 30,
    ) -> SlurmJobStatus:
        """
        Watch a running slurm job by periodically polling the
        API for the job status.
        """

        backend = await self._create_backend_from_name(connection_name)

        status = None

        while status in [
            None,
            SlurmJobStatus.PENDING,
            SlurmJobStatus.PREEMPTED,
            SlurmJobStatus.RUNNING,
            SlurmJobStatus.CONFIGURING,
        ]:
            status = await backend.status(job_id)
            await asyncio.sleep(update_interval_sec)

        return status

    def _submit_script(self, configuration: SlurmJobConfiguration) -> str:
        """
        Generate the submit script for the slurm job
        """

        try:

            return self._submit_script_from_template(configuration)

        except ValueError:

            script = ["#!/bin/bash"]

            command = configuration.command or self._base_flow_run_command()
            script += [command]

            return "\n".join(script)

    def _submit_script_from_template(self, configuration: SlurmJobConfiguration) -> str:
        """
        Creates the submit script based on a provided template.
        """

        if configuration.script_template is None:
            raise ValueError("Script template required")

        template = Template(configuration.script_template)

        return template.render(
            working_dir=configuration.working_dir,
            num_nodes=configuration.num_nodes,
            num_processes_per_node=configuration.num_processes_per_node,
            max_walltime=configuration.max_walltime,
            queue=configuration.queue,
            image=configuration.image,
            pre_command=configuration.pre_command,
            post_command=configuration.post_command,
        )


if __name__ == "__main__":

    from prefect.cli.worker import start

    start(worker_name="debug", work_pool_name="hsuper-test", worker_type="slurm")
