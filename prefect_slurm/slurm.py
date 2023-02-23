import time, os
import abc

from io import StringIO
from typing import Optional, Tuple, List

import anyio.abc

from sshfs import SSHFileSystem
import asyncssh

from pydantic import Field
from typing_extensions import Literal

from prefect.blocks.core import SecretStr
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible

from io import TextIOBase
from enum import Enum

import yaml
import subprocess

class SlurmJobStatus(Enum):
  
    """"
    Models the different states of a SLURM Job.
    """
  
    COMPLETED=0
    RUNNING=1
    FAILED=2
    PREEMPTED=3
    PENDING=4
    UNDEFINED=5
    UNKNOWN=6


class SlurmBackend:
  
    """
    Backend to interact with the SLURM scheduler. This is an abstract base class. Specialised implementations
    should either use CLI or API-based controll of the scheduler.
    """

    @abc.abstractmethod
    async def submit(self,slurm_kwargs:dict[str,str],run_script:TextIOBase=None, grace_seconds:int=30) -> int:
        """Submit a new SLURM Job to process a flow run"""

    @abc.abstractmethod
    async def status(self,jobid:int, grace_seconds:int=30) -> SlurmJobStatus:  
        """Obtain the status of a SLURM job"""

    @abc.abstractmethod
    async def kill(self,jobid:int, grace_seconds:int=30):
        """Cancel the job with jobid"""

class CLIBasedSlurmBackend(SlurmBackend):

    """
    CLI-based backend to control a slurm scheduler

    Parameters
    ----------

    host (str)      The hostname (usually the login-node) on which the slurm commands sbatch, squeue, and scancel are available
    username (str)  The username to authenticate with the hpc system via ssh
    password (str)  The password to authenticate the user via ssh
    """
    
    host:str
    username:str
    password:str

    def __init__(self,host:str, username:str, password:str):
        self.host=host
        self.username=username
        self.password=password

    async def submit(self,slurm_kwargs:dict[str,str], run_script:TextIOBase=None, grace_seconds:int=30) -> int:
        
        result = await self._run_remote_command(
            cmd=self._submit_command(slurm_kwargs),
            in_stream=run_script,
            grace_seconds=grace_seconds,
        )

        return int(result.stdout.strip())

    async def kill(self,jobid:int, grace_seconds:int=30):

        await self._run_remote_command(
              cmd=self._kill_command(jobid),
              grace_seconds=grace_seconds,
        )

    async def status(self,jobid:int, grace_seconds:int=30) -> SlurmJobStatus:

        result = await self._run_remote_command(
            cmd=self._status_command(jobid),
            grace_seconds=grace_seconds,
        )

        # Status command exits with non-zero exit code if jobid is not found.
        # This includes finished jobs that have been removed from the queue!!!!
        if result.exit_status != 0: return SlurmJobStatus.UNDEFINED

        try:
          status, exit_code = [v.strip() for v in result.stdout.split()[0:2]]

          if status == "PENDING": return SlurmJobStatus.PENDING
          if status == "COMPLETED": return SlurmJobStatus.COMPLETED
          if status == "PREEMPTED": return SlurmJobStatus.PREEMPTED
          if status == "FAILED": return SlurmJobStatus.FAILED
          if status == "RUNNING": return SlurmJobStatus.RUNNING

          return SlurmJobStatus.UNKNOWN
        except:
          return SlurmJobStatus.UNDEFINED


    async def _run_remote_command(
      self, cmd: str, in_stream: TextIOBase=None, grace_seconds: int = 30, safe=False
    ) -> asyncssh.SSHCompletedProcess:

      result = None
      async with self._get_connection() as c:
          result = await c.run(
              cmd, stdin=in_stream, timeout=grace_seconds
          )

      return result

    def _submit_command(self,slurm_kwargs:dict[str,str]) -> str:
        """
        Generates the sbatch command to submit a job to slurm
        """

        ## Create the arguments from slurm_kwargs
        args = [
            f"--{k}" if v == None else f"--{k}={v}" for k, v in slurm_kwargs.items()
        ]
        cmd = " ".join(["sbatch", "--parsable"] + args)

        return cmd


    def _kill_command(self, jobid: int) -> str:
        """
        Generates the kill command to terminate a slurm job
        """

        return f"scancel ${jobid}"
    

    def _status_command(self, jobid) -> str:
        """
        Generate the squeue command to monitor job status
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
              password=self.password.get_secret_value()
            )
        )


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
        default=[], description=("Commands to run before executing the flow with the slurm job")
    )

    post_run: Optional[List[str]] = Field(
        default=[], description=("Commands to run after executing the flow with the slurm job")
    )

    working_directory: Optional[str] = Field(
        default=None, description="Base directory for slurm runs. If specified, a subdirectory (if needed) will be created for each flow run."
    )

    retain_working_directory: Optional[bool] = Field(
      default=False, description="If set, the temporary working directory will not be deleted after the slurm job has finished."
    )

    password: SecretStr = Field(
        default=None, description=("The password to authenticate username")
    )

    slurm_kwargs: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "A dictionary with slurm batch arguments as key-value pairs. E.g, the parameter --nodes=1"
        ),
    )
    
    stream_output: bool = Field(
        default=True,
        description="If set, output will be streamed from the job to local standard output.",
    )

    conda_env: str = Field(
       default=None,
       description="Conda environment name to activet on HPC system (must be pre-installed)"
    )

    _backend_instance : SlurmBackend = None

    @property
    def _backend(self) -> SlurmBackend:
      if not self._backend_instance: 
        self.logger.debug(f"Instantiating Slurm CLI-based backend on {self.host} for user {self.username}")
        self._backend_instance = CLIBasedSlurmBackend(self.host,self.username,self.password)

      return self._backend_instance

    @sync_compatible
    async def run(
        self,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SlurmJobResult:

        if not self.command:
            raise ValueError("Slurm job cannot be run with empty command.")
        
        # Prepare working directory
        flow_run_id = self._get_environment_variables()['PREFECT__FLOW_RUN_ID']
        wdir = os.path.join(self.working_directory if self.working_directory else ".", flow_run_id)
        
        fs = self._filesystem()            
        await run_sync_in_worker_thread(fs.mkdir,wdir)
        self.logger.debug(f"Slurm Job: created flow run dir [{wdir}] on host [{self.host}]")
        self.slurm_kwargs["chdir"] = wdir

        # Configure output files
        self.slurm_kwargs['output'] = "output.log"
        self.slurm_kwargs['error'] = "error.log"
          
        # Submit slurm job
        jobid = await self._backend.submit(self.slurm_kwargs, StringIO(self._submit_script()))
        pid = self._get_infrastructure_pid(jobid)

        if task_status is not None:
            task_status.started(pid)

        self.logger.info(f"Slurm Job: Job {jobid} submitted and registered as {pid}.")

        # Monitor the job until completion
        status_code = await self._watch_job(self._backend, jobid)

        # Capture output
        if self.stream_output:
            try: 
              with fs.open(os.path.join(wdir,self.slurm_kwargs['output']),'r') as stream:
                  print(stream.read())
              with fs.open(os.path.join(wdir,self.slurm_kwargs['error']),'r') as stream:
                  print(stream.read())
            except:
              self.logger.error("Could not retrieve logs from slurm job")

        # Cleanup after run
        if not self.retain_working_directory:
            try:
              fs.rmdir(wdir)
            except:
              self.logger.error(f"Slurm Job: could not delete working directory for flow run [{flow_run_id}] on host [{self.host}]")

        return SlurmJobResult(identifier=pid, status_code=status_code)

    def preview(self):
        return "Not implemented"


    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        _, jobid = self._parse_infrastructure_pid(infrastructure_pid)
        await self._backend.kill(jobid)


    def _submit_script(self) -> str:
        """
        Generate the submit script for the slurm job
        """
        script = ["#!/bin/bash"]
        script += [f"export {k}={v}" for k, v in self._get_environment_variables(False).items()]
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


    async def _watch_job(self, backend:SlurmBackend, jobid: str, polling_seconds: int=30) -> int:

        completed = False
        submitted = False

        startWatching = time.time()

        while not completed:

            status = await backend.status(jobid)

            # Job never seen on the slurm queue
            if (status == status.UNDEFINED) and not submitted:
                self.logger.error(f"Slurm Job: Job {jobid!r} not known to slurm.")
                
                if (time.time() - startWatching) < polling_seconds:
                  # Just started watching, give the slurm agent some time to process the submission
                  continue
                else:
                  completed = True
                  return -1

            # This point is only reached if the jobid is known to slurm in an interation
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