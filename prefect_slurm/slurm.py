"""
This module provides a common interface to interact with SLURM workload managers.

The module defines a common interface for a SLURM backed service and two
implementations:

    1) A CLI-based implementation that utilises ssh sessions to interact usually
       with the login node of an HPC cluster.
    2) An API-based implementation that accesses a SLURM API endpoint.

If available, the API endpoint is usually the better choice for stability. CLI-based
sessions can be unrealiabe. But not all HPC environments expose the REST API provided
by SLURM, and some functions are not yet supported by the API either. In these cases,
the CLI-based client is the way to go.
"""

import abc
from enum import Enum
from io import TextIOBase

import asyncssh
from httpx import URL

from prefect_slurm.api.jobs import APIEndpoint, JobDefinition


class SlurmJobStatus(Enum):

    """ "
    Models the different states of a SLURM Job.
    """

    COMPLETED = 0
    RUNNING = 1
    FAILED = 2
    PREEMPTED = 3
    PENDING = 4
    UNDEFINED = 5
    UNKNOWN = 6
    CONFIGURING = 7
    CANCELLED = 8


class SlurmBackend:

    """
    Backend to interact with the SLURM scheduler. This is an abstract base class.
    Specialised implementations should either use CLI or API-based controll of
    the scheduler.
    """

    @abc.abstractmethod
    async def submit(
        self,
        job_definition: JobDefinition,
        run_script: TextIOBase = None,
        grace_seconds: int = 30,
    ) -> int:
        """Submit a new SLURM Job to process a flow run"""

    @abc.abstractmethod
    async def status(self, jobid: int, grace_seconds: int = 30) -> SlurmJobStatus:
        """Obtain the status of a SLURM job"""

    @abc.abstractmethod
    async def kill(self, jobid: int, grace_seconds: int = 30):
        """Cancel the job with jobid"""


class APIBasedSlurmBackend(SlurmBackend):

    """
    API-based backend to control slurm scheduler


    Parameters
    ----------

    endpoint (URL)  The URL for the SLURM API endpoint
    username (str)  The username to authenticate with the API
    token    (str)  The token to authenticate the user at the API
    insecure (bool) Allow insecure connections to API endpoints
    """

    endpoint: URL
    username: str
    token: str
    insecure: bool

    def __init__(
        self, endpoint: URL, username: str, token: str, insecure: bool = False
    ):
        self.endpoint = endpoint
        self.username = username
        self.token = token
        self.insecure = insecure

    async def submit(
        self,
        job_defintion: JobDefinition,
        run_script: TextIOBase = None,
        grace_seconds: int = 30,
    ) -> int:
        """Submit a new SLURM Job to process a flow run"""

        api = APIEndpoint(self.endpoint, self.username, self.token)

        response = await api.submit(job_defintion, run_script)

        return response.job_id

    async def status(self, jobid: int, grace_seconds: int = 30) -> SlurmJobStatus:
        """Obtain the status of a SLURM job"""

        api = APIEndpoint(self.endpoint, self.username, self.token)

        response = await api.status(jobid, timeout=grace_seconds)

        if response.has_errors():
            return SlurmJobStatus.UNKNOWN

        return SlurmJobStatus[response.jobs[0].job_state]

    async def kill(self, jobid: int, grace_seconds: int = 30):
        """Cancel the job with jobid"""

        return


class CLIBasedSlurmBackend(SlurmBackend):

    """
    CLI-based backend to control a slurm scheduler

    Parameters
    ----------

    host (str)      The hostname (usually the login-node) on which the slurm
                    commands sbatch, squeue, and scancel are available
    username (str)  The username to authenticate with the hpc system via ssh
    password (str)  The password to authenticate the user via ssh
    """

    host: str
    username: str
    password: str

    def __init__(self, host: str, username: str, password: str):
        self.host = host
        self.username = username
        self.password = password

    async def submit(
        self,
        job_definition: JobDefinition,
        run_script: TextIOBase = None,
        grace_seconds: int = 30,
    ) -> int:
        """
        Submit a new slurm job using the cli command 'sbatch'

        :slurm_kwargs: dictionary of parameters passed to sbatch
        :run_script: IO stream passed to stdin during job submission as the job script
        :grace_seconds: timeout for executing sbatch on the hpc system
        """

        result = await self._run_remote_command(
            cmd=self._submit_command(job_definition.to_kwargs()),
            in_stream=run_script,
            grace_seconds=grace_seconds,
        )
        try:
            return int(result.stdout.strip())
        except ValueError:
            # If job submission fails, the returned value is not a number. Pass the
            # root cause back up as RuntimeError.
            raise RuntimeError(result.stderr.strip())

    async def kill(self, jobid: int, grace_seconds: int = 30):
        """
        Cancel a slurm job using the 'scancel' cli command

        :jobid: the jobid that references the job in slurm
        :grace_seconds: timeout for executing sbatch on the hpc system
        """

        await self._run_remote_command(
            cmd=self._kill_command(jobid),
            grace_seconds=grace_seconds,
        )

    async def status(self, jobid: int, grace_seconds: int = 30) -> SlurmJobStatus:
        """
        Obtain the status of a slurm job using the 'squeue' cli command

        :jobid: the jobid that references the job in slurm
        :grace_seconds: timeout for executing squeue on the hpc system
        """

        result = await self._run_remote_command(
            cmd=self._status_command(jobid),
            grace_seconds=grace_seconds,
        )

        # squeue exits non-zero when the job is not found, which happens for
        # both completed and failed jobs after they age out of the queue.
        if result.exit_status != 0:
            return await self._sacct_status(jobid, grace_seconds)

        try:
            status, exit_code = [v.strip() for v in result.stdout.split()[0:2]]

            if status == "PENDING":
                return SlurmJobStatus.PENDING
            if status == "COMPLETED":
                return SlurmJobStatus.COMPLETED
            if status == "PREEMPTED":
                return SlurmJobStatus.PREEMPTED
            if status == "FAILED":
                return SlurmJobStatus.FAILED
            if status == "RUNNING":
                return SlurmJobStatus.RUNNING
            if status == "CONFIGURING":
                return SlurmJobStatus.CONFIGURING

            return SlurmJobStatus.UNKNOWN
        except Exception:
            return SlurmJobStatus.UNDEFINED

    async def _run_remote_command(
        self,
        cmd: str,
        in_stream: TextIOBase = None,
        grace_seconds: int = 30,
        safe=False,
    ) -> asyncssh.SSHCompletedProcess:
        """
        Run a shell command on the remote hpc system using ssh

        :cmd: the command to be executed
        :in_stream: IO stream passed as stdin the the process on the hpc system
        :grace_seconds: timeout for executing squeue on the hpc system
        """
        result = None
        async with self._get_connection() as c:
            result = await c.run(cmd, stdin=in_stream, timeout=grace_seconds)

        return result

    def _submit_command(self, slurm_kwargs: dict[str, str]) -> str:
        """
        Generates the sbatch command to submit a job to slurm

        :slurm_kwargs: dictionary of parameters passed to sbatch
        """

        # Create the arguments from slurm_kwargs
        args = [
            f"--{k}" if v is None else f"--{k}={v}" for k, v in slurm_kwargs.items()
        ]
        cmd = " ".join(["sbatch", "--parsable"] + args)

        return cmd

    def _kill_command(self, jobid: int) -> str:
        """
        Generates the kill command to terminate a slurm job

        :jobid: the jobid that references the job in slurm
        """

        return f"scancel {jobid}"

    def _status_command(self, jobid) -> str:
        """
        Generate the squeue command to monitor job status

        :jobid: the jobid that references the job in slurm
        """

        return f"squeue --job={jobid} --Format=State,exit_code --noheader"

    def _get_connection(self) -> asyncssh.SSHClientConnection:
        """
        Return a connection to the slurm login node
        """

        return asyncssh.connect(
            host=self.host,
            options=asyncssh.SSHClientConnectionOptions(
                username=self.username,
                password=self.password.get_secret_value(),
                known_hosts=None,
            ),
        )

    async def _sacct_status(
        self, jobid: int, grace_seconds: int = 30
    ) -> SlurmJobStatus:
        """
        Query the final state of a completed job via sacct.

        Used as a fallback when squeue no longer lists the job. sacct retains
        history for jobs that have already left the queue.
        """

        result = await self._run_remote_command(
            cmd=f"sacct -j {jobid} -X --format=State --noheader --parsable2",
            grace_seconds=grace_seconds,
        )

        if result.exit_status != 0 or not result.stdout.strip():
            return SlurmJobStatus.UNDEFINED

        # --parsable2 gives one field per line without a trailing delimiter.
        # Take the first line (the job allocation itself, not any step records).
        # sacct may return "CANCELLED by <uid>" so we check with startswith.
        state = result.stdout.strip().split("\n")[0].strip().upper()

        if state.startswith("CANCELLED"):
            return SlurmJobStatus.CANCELLED
        if state in ("TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"):
            return SlurmJobStatus.FAILED
        try:
            return SlurmJobStatus[state]
        except KeyError:
            return SlurmJobStatus.UNKNOWN
