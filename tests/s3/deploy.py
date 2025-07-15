from flow import test_flow
from prefect.deployments.runner import DockerImage
from prefect.types.entrypoint import EntrypointType

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
    )
