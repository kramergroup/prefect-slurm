from flow import test_flow
from prefect.deployments.runner import DockerImage
from prefect.types.entrypoint import EntrypointType

PULL_STEPS = [
    {
        "prefect.deployments.steps.set_working_directory": {
            "directory": "{{ $PREFECT_SLURM_WORKING_DIR }}"
        }
    }
]

if __name__ == "__main__":

    test_flow.deploy(
        work_pool_name="hsuper-dev",
        name="test-s3-flow",
        push=True,
        entrypoint_type=EntrypointType.MODULE_PATH,
        image=DockerImage(
            name="materialsfoundry.io/flows/test-s3-flow",
            dockerfile="Dockerfile",
            tag="dev",
            platform="linux/amd64",
        ),
        job_variables={"queue": "dev", "max_walltime": "00:05:00"},
        pull=PULL_STEPS,
    )
