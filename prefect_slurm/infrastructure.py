"""
This module provides an infrastrucutre block for prefect agents to interact
with the SLURM workload manager commonly used in HPC environments.
"""

import os
import time
from io import StringIO
from typing import List, Optional, Tuple

import anyio.abc
from prefect.blocks.core import SecretStr
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field
from sshfs import SSHFileSystem
from typing_extensions import Literal

from prefect_slurm.api.jobs import JobDefinition
from prefect_slurm.slurm import CLIBasedSlurmBackend, SlurmBackend


class SlurmJobResult(InfrastructureResult):
    """Contains information about the final state of a completed Slurm Job"""


class SlurmJob(Infrastructure):
    """
    Runs a command in a SLURM job.

    Requires access to a SLURM scheduler.
    """

    type: Literal["slurm-job"] = Field(
        default="slurm-job", description="The type of infrastructure."
    )

    host: str = Field(
        default=None,
        description=("The hostname of the login node for the cluster running SLURM"),
    )

    username: str = Field(
        default=None, description=("The username of your account on the cluster")
    )

    pre_run: Optional[List[str]] = Field(
        default=[],
        description=("Commands to run before executing the flow with the slurm job"),
    )

    post_run: Optional[List[str]] = Field(
        default=[],
        description=("Commands to run after executing the flow with the slurm job"),
    )

    working_directory: Optional[str] = Field(
        default=None,
        description="Base directory for slurm runs. If specified, a subdirectory "
        "(if needed) will be created for each flow run.",
    )

    retain_working_directory: Optional[bool] = Field(
        default=False,
        description="If set, the temporary working directory will not be deleted "
        "after the slurm job has finished.",
    )

    password: SecretStr = Field(
        default=None, description=("The password to authenticate username")
    )

    slurm_kwargs: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "A dictionary with slurm batch arguments as key-value pairs. "
            "E.g, the parameter --nodes=1"
        ),
    )

    stream_output: bool = Field(
        default=True,
        description="If set, output will be streamed from the job to "
        "local standard output.",
    )

    conda_env: str = Field(
        default=None,
        description="Conda environment name to activet on HPC system "
        "(must be pre-installed)",
    )

    _backend_instance: SlurmBackend = None

    @property
    def _backend(self) -> SlurmBackend:
        """
        Obtain the backend that manages communication with the
        hpc environment running slurm

        This currently returns a cli-based backend only.
        """

        if not self._backend_instance:
            self.logger.debug(
                f"Instantiating Slurm CLI-based backend on "
                f"{self.host} for user {self.username}"
            )
            self._backend_instance = CLIBasedSlurmBackend(
                self.host, self.username, self.password
            )

        return self._backend_instance

    @sync_compatible
    async def run(
        self,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SlurmJobResult:
        """
        Run a flow within the slurm-based hpc environment.

        The infrastuture run:
            1) creates a subdirectory named according to the flow run id
            2) submits a new slurm job that will run the prefect engine passing the
               flow run id as env variable PREFECT__FLOW_RUN_ID
            3) monitors the status of the slurm job
            4) cleans up the hpc environment after the job has finished
        """

        if not self.command:
            raise ValueError("Slurm job cannot be run with empty command.")

        # Prepare working directory
        flow_run_id = self._get_environment_variables()["PREFECT__FLOW_RUN_ID"]
        wdir = os.path.join(
            self.working_directory if self.working_directory else ".", flow_run_id
        )

        # Manage garbage collection of fs to avoid long running sessions
        fs = self._filesystem()
        try:
            await run_sync_in_worker_thread(fs.mkdir, wdir)
        finally:
            del fs

        self.logger.debug(
            f"Slurm Job: created flow run dir [{wdir}] on host [{self.host}]"
        )
        self.slurm_kwargs["chdir"] = wdir

        # Configure output files
        self.slurm_kwargs["output"] = "output.log"
        self.slurm_kwargs["error"] = "error.log"

        # Submit slurm job
        jobid = await self._backend.submit(
            JobDefinition.from_kwargs(self.slurm_kwargs),
            StringIO(self._submit_script()),
        )
        pid = self._get_infrastructure_pid(jobid)

        if task_status is not None:
            task_status.started(pid)

        self.logger.info(f"Slurm Job: Job {jobid} submitted and registered as {pid}.")

        # Monitor the job until completion
        status_code = await self._watch_job(self._backend, jobid)

        # Manage garbage collection of fs to avoid long running sessions
        fs = self._filesystem()
        try:
            # Capture output
            if self.stream_output:
                try:
                    with fs.open(
                        os.path.join(wdir, self.slurm_kwargs["output"]), "r"
                    ) as stream:
                        print(stream.read())
                    with fs.open(
                        os.path.join(wdir, self.slurm_kwargs["error"]), "r"
                    ) as stream:
                        print(stream.read())
                except Exception:
                    self.logger.error("Could not retrieve logs from slurm job")

            # Cleanup after run
            if not self.retain_working_directory:
                try:
                    fs.rmdir(wdir, recursive=True)
                except Exception:
                    self.logger.error(
                        f"Slurm Job: could not delete working directory for "
                        f"flow run [{flow_run_id}] on host [{self.host}]"
                    )
        finally:
            del fs

        return SlurmJobResult(identifier=pid, status_code=status_code)

    def preview(self):
        """
        Returns a string representation of the infrastructure block
        """
        return "Not implemented"

    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        """
        Kill a flow run.

        This will submit a cancel request to the slurm manager

        :infrastructure_pid: identifier for the infrastructure
        :grace_seconds: timeout to complete the slurm job termination request
        """
        _, jobid = self._parse_infrastructure_pid(infrastructure_pid)
        await self._backend.kill(jobid)

    def _submit_script(self) -> str:
        """
        Generate the submit script for the slurm job
        """
        script = ["#!/bin/bash"]
        script += [
            f"export {k}={v}" for k, v in self._get_environment_variables(False).items()
        ]
        script += self.pre_run
        if self.conda_env:
            script += [f"conda run -n {self.conda_env} " + " ".join(self.command)]
        else:
            script += [" ".join(self.command)]
        script += self.post_run

        return "\n".join(script)

    def _get_infrastructure_pid(self, jobid: str) -> str:
        """
        Generates a Slurm infrastructure PID.

        The PID is in the format: "<cluster name>:<jobid>".
        """
        pid = f"{self.host}:{jobid}"
        return pid

    def _parse_infrastructure_pid(
        self,
        infrastructure_pid,
    ) -> Tuple[str, int]:
        """
        Parses the infrastructure pid formated as "<cluster name>:<jobid>" and
        returns the cluster name and jobid
        """
        hostname, pid = infrastructure_pid.split(":")
        return hostname, int(pid)

    async def _watch_job(
        self, backend: SlurmBackend, jobid: str, polling_seconds: int = 30
    ) -> int:
        """
        Monitor a running slurm job.

        The slurm work load manager is polled periodocally for the job status.
        The routine returns zero if the job has terminated with COMPLETED state or is
        not found in the slurm queue anymore. It returns -1 for FAILED jobs
        or UNDEFINED jobs.

        Note that detecting failed slurm runs is fragile, because a failed job can
        be removed from the slurm queue within the polling interval. In this case, the
        routine returns zero indicating a normal termination.

        :backend: the backend used to communicate with slurm
        :jobid: the id of the slurm job
        :polling_seconds: polling interval for quering the slurm workload manager
        """
        completed = False
        submitted = False

        startWatching = time.time()

        while not completed:

            status = await backend.status(jobid)

            # Job never seen on the slurm queue
            if (status == status.UNDEFINED) and not submitted:
                self.logger.error(f"Slurm Job: Job {jobid!r} not known to slurm.")

                if (time.time() - startWatching) < polling_seconds:
                    # Just started watching, give the slurm agent some time
                    # to process the submission
                    continue
                else:
                    completed = True
                    return -1

            # This point is only reached if the jobid is known
            # to slurm in an interation
            submitted = True

            # Job removed from slurm queue - assume it finished ok
            if (status == status.UNDEFINED) or (status == status.COMPLETED):
                self.logger.info(f"Slurm Job: Job {jobid!r} finished/cleared.")
                completed = True
                return 0

            if status == status.FAILED:
                self.logger.warn(f"Slurm Job: Job {jobid!r} failed.")
                completed = True
                return -1

            await anyio.sleep(polling_seconds)

        # we should never reach this point!
        return -1

    def _get_environment_variables(self, include_os_environ: bool = True):
        """
        Obtain environment variables to pass to the slurm job

        :include_os_environ: include current OS environment variables if True
        """
        os_environ = os.environ if include_os_environ else {}
        # The base environment must override the current environment or
        # the Prefect settings context may not be respected
        env = {**os_environ, **self._base_environment(), **self.env}

        # Drop null values allowing users to "unset" variables
        return {key: value for key, value in env.items() if value is not None}

    def _filesystem(self) -> SSHFileSystem:
        """
        Return a connection to the slurm login node filesystem via ssh
        """
        return SSHFileSystem(
            host=self.host,
            username=self.username,
            password=self.password.get_secret_value(),
        )
