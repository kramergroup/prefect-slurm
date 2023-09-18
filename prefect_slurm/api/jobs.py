"""
SLURM API for managing jobs.

This module implements some functionality of the SLURM REST API to manage jobs.
Only a very limited feature set is currently supported:

1) Submitting a new job
2) Obtaining the status of a job
3) Cancelling a job
"""

import json
from pathlib import Path
from typing import Annotated, Any, List, Optional
from urllib.parse import urljoin

import httpx
import regex
from pydantic import BaseModel, Field, validator

DEFAULT_SCRIPT = """#!/bin/bash
echo "hello script"
srun hostname
echo $SLURM_JOB_ID
sleep 100
"""

TimeString = Annotated[str, Field(regex="[0-9]{1,3}:[0-6][0-9]:[0-6][0-9]")]


class JobDefinition(BaseModel):
    """
    Define a job for slurm
    """

    partition: str
    time_limit: TimeString = Field(default="01:00:00", description="Maximum Walltime")
    tasks: int = Field(default=72, description="Number of MPI tasks")
    name: str = Field(default="my-job", description="Name of the SLURM job")
    nodes: str = Field(default=1, description="Number of nodes for the SLURM job")
    current_working_directory: Path = Field(description="Working directory")
    environment: dict[str, str] = Field(
        default={
            "PATH": "/bin:/usr/bin/:/usr/local/bin/",
            "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
        },
        description="Environment variables",
    )

    @validator("current_working_directory")
    def validate_current_working_directory(cls, v):
        """
        ensure that the provided working directory is a valid path.
        """
        return Path(v)

    @staticmethod
    def from_kwargs(slurm_kwargs: dict[str, str]):
        """
        Create a job definition from a list of slurm keyword arguments (as would be used
        with sbatch). This constructor only respects known and mapped keywords.
        """

        lookup = {
            "p": "partition",
            "t": "time_limit",
            "time": "time_limit",
            "n": "tasks",
            "ntasks": "tasks",
            "N": "nodes",
            "chdir": "current_working_directory",
        }

        for k in slurm_kwargs.copy():
            if k in lookup:
                slurm_kwargs[lookup[k]] = slurm_kwargs.pop(k)

        # Decompose the export parameter to populate an environment dict
        # This implementation is not fool-proof because of the handling of "," and "="
        # characters. It would be better to deal with escaped versions (e.g., \,).
        export = slurm_kwargs.pop("export", default=None)
        env = {}
        if export:
            for s in export.split(","):
                k = s.split("=")
                env[k[0]] = k[1]

        slurm_kwargs["environment"] = env

        return JobDefinition(**slurm_kwargs)

    def to_kwargs(self):
        """
        Convert the JobDefinition to the equivalent cli command line
        arguments for sbatch.
        """

        res = {
            "p": self.partition,
            "t": self.time_limit,
            "n": self.tasks,
            "N": self.nodes,
            "chdir": self.current_working_directory,
        }
        if self.environment:
            res["export"] = ",".join([k + "=" + v for k, v in self.environment.keys()])


class ResponseError(BaseModel):

    """
    Describes an error that was returned from the API in response to a request
    """

    error_code: Optional[int]
    error_number: Optional[int]
    description: Optional[str]
    error: str
    source: Optional[str]

    @validator("error_number", always=True)
    def check_error_code_number(cls, error_number, values):
        """
        ensure that either an error number or an error code is provided.
        """
        if not values.get("error_code") and not error_number:
            raise ValueError("Either error_number or error_code is required")
        return error_number


class BasicResonse(BaseModel):
    """
    Basic model for API responses.
    """

    errors: List[ResponseError]

    def has_errors(self) -> bool:
        """
        Returns true if the response contains errors
        """
        return self.errors is not None and len(self.errors) > 0


class SubmitRequest(BaseModel):
    """
    Request to submit a new job to the API
    """

    job: JobDefinition
    script: str


class SubmitResponse(BasicResonse):
    """
    Response to a job submission request
    """

    job_id: Optional[int]

    @validator("job_id", always=True)
    def check_job_id(cls, job_id, values):
        """
        Job ID only required if response has no errors.
        """
        if len(values.get("errors")) == 0 and not job_id:
            raise ValueError("Job id is required unless errors are returned")
        return job_id


class JobStatus(BaseModel):
    """
    State of a slurm job. The job_state field is populated with an all
    uppercase string representing the state of the job
    """

    job_state: str


class StatusResponse(BasicResonse):
    """
    Response of the API to a job status request
    """

    jobs: List[JobStatus]

    @validator("jobs", always=True)
    def check_jobs_or_errors(cls, jobs, values):
        """
        Ensure that either errors or a list of job status information is returned
        """
        if (not values.get("errors") or len(values.get("errors")) == 0) and len(
            jobs
        ) == 0:
            raise ValueError("Either jobs or errors should be returned")
        return jobs


class CancelResponse(BasicResonse):
    """
    Result of a cancel request
    """


class APIEndpoint:
    """
    Implementation the SLURM REST API endpoint
    """

    endpoint: str
    username: str
    token: str

    def __init__(self, endpoint: str, username: str, token: str) -> None:
        self.endpoint = endpoint
        self.username = username
        self.token = token

    async def submit(
        self, job: JobDefinition, script: str = DEFAULT_SCRIPT, timeout: int = 30
    ) -> SubmitResponse:
        """
        Submit a new job to SLURM.
        """

        payload = SubmitRequest(job=job, script=script)

        async with httpx.AsyncClient() as c:

            resp = await c.post(
                url=urljoin(self.endpoint, "job/submit"),
                content=payload.json(exclude_none=True),
                headers={
                    "X-SLURM-USER-NAME": self.username,
                    "X-SLURM-USER-TOKEN": self.token,
                    "Content-Type": "application/json",
                },
                timeout=timeout,
            )

            # This is ugly. SLURM returns a json string with "Connection Closed" added,
            # making the response non-json. We filter for valid json and
            # only process this
            data = _extract_valid_json(resp.text)
            result = SubmitResponse(**data)

            # Handle errors
            for e in result.errors:
                if e.error_code == 5005:
                    raise PermissionError(
                        "Slurm rejected request. Possibly due to an invalid token."
                    )

            if len(result.errors) > 0:
                raise RuntimeError(e)

        return result

    async def status(self, jobid: int, timeout: int = 30) -> StatusResponse:
        """
        Check status of a slurm job.
        """

        async with httpx.AsyncClient() as c:

            resp = await c.get(
                url=urljoin(self.endpoint, "job/%d" % jobid),
                headers={
                    "X-SLURM-USER-NAME": self.username,
                    "X-SLURM-USER-TOKEN": self.token,
                    "Content-Type": "application/json",
                },
                timeout=timeout,
            )

            # This is ugly. SLURM returns a json string with "Connection Closed" added,
            # making the response non-json. We filter for valid json and
            # only process this
            data = _extract_valid_json(resp.text)
            result = StatusResponse(**data)

            if len(result.jobs) == 0:
                raise AttributeError("Jobid %d not known" % jobid)

            return result

    async def cancel(self, job_id: int, timeout=30) -> CancelResponse:
        """
        Cancel a slurm job
        """

        async with httpx.AsyncClient() as c:

            resp = await c.delete(
                url=urljoin(self.endpoint, "job/%d" % job_id),
                headers={
                    "X-SLURM-USER-NAME": self.username,
                    "X-SLURM-USER-TOKEN": self.token,
                    "Content-Type": "application/json",
                },
                timeout=timeout,
            )

            data = _extract_valid_json(resp.text)
            result = CancelResponse(**data)

            # Handle errors
            for e in result.errors:
                if e.error_code == 5005:
                    raise PermissionError(
                        "Slurm rejected request. Possibly due to an invalid token."
                    )
                if e.error_number == 2017:
                    raise ValueError("Invalid job id specified")

            if len(result.errors) > 0:
                raise RuntimeError(result.errors)

            return result


# ----------------------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------------------


def _extract_valid_json(value: str) -> Any:
    """
    Extracts a json string from a larger string. This function simply does an
    outer bracket match on curly brackets before attempting to decode the json.
    """
    r = regex.compile("\{(?:[^}{]+|(?R))*+\}")  # noqa: W605
    m = r.match(value)
    if not m:
        raise RuntimeError("Slurm API did not return valid JSON.")
    return json.loads(m.group(0))
