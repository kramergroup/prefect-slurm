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
import asyncio
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
    """

    endpoint: URL
    username: str
    token: str

    def __init__(self, endpoint: URL, username: str, token: str):
        self.endpoint = endpoint
        self.username = username
        self.token = token

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

        return int(result.stdout.strip())

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

        # Status command exits with non-zero exit code if jobid is not found.
        # This includes finished jobs that have been removed from the queue!!!!
        if result.exit_status != 0:
            return SlurmJobStatus.UNDEFINED

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

        return f"scancel ${jobid}"

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

    class APIBasedSlurmBackend(SlurmBackend):
        """
        API-based backend to control a slurm scheduler

        Parameters
        ----------

        endpoint (str)    The URL of the SLURM API endpoint
        username (str)    The username to authenticate with the SLURM API
        token (str)       The API token (scontrol token lifespan=6000 | cut -d= -f2)
        """

        api: APIEndpoint

        def __init__(self, endpoint: str, username: str, token: str):
            self.api = APIEndpoint(endpoint=endpoint, username=username, token=token)

        async def submit(
            self,
            slurm_kwargs: dict[str, str],
            run_script: TextIOBase = None,
            grace_seconds: int = 30,
        ) -> int:
            """
            Submit a new SLURM Job to process a flow run
            """

            job = JobDefinition.from_kwargs(slurm_kwargs)
            task_submit = self.api.submit(job, run_script)
            res = await asyncio.wait_for(task_submit, timeout=grace_seconds)

            if res.errors and len(res.errors) > 0:
                raise RuntimeError(res.errors)

            return res.job_id

        async def status(self, jobid: int, grace_seconds: int = 30) -> SlurmJobStatus:
            """
            Query the status of a SLURM job by jobid
            """

            task_status = self.api.status(jobid)
            res = await asyncio.wait_for(task_status, timeout=grace_seconds)

            if len(res.errors) > 0 or len(res.jobs) != 1:
                return SlurmJobStatus.UNKNOWN

            return SlurmJobStatus[res.jobs[0].job_state]

        async def kill(self, jobid: int, grace_seconds: int = 30):
            """
            Cancel the job with jobid
            """

            task_cancel = self.api.cancel(jobid)
            await asyncio.wait_for(task_cancel, timeout=grace_seconds)
