from prefect.deployments.runner import DockerImage
from prefect.types.entrypoint import EntrypointType
from test_flow import test_flow

if __name__ == "__main__":

    test_flow.deploy(
        work_pool_name="hsuper-dev",
        name="test-flow",
        push=True,
        entrypoint_type=EntrypointType.MODULE_PATH,
        image=DockerImage(
            name="materialsfoundry.io/flows/test-flow",
            dockerfile="Dockerfile",
            tag="dev",
            platform="linux/amd64",
        ),
        job_variables={"queue": "dev", "max_walltime": "00:05:00"},
    )
