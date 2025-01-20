import socket

from prefect import flow
from prefect.deployments.runner import DockerImage


@flow
def test_flow() -> str:
    return f"Hello, world from {socket.gethostname()}!"


# if __name__ == "__main__":
#     test_flow.from_source(
#         source=str(Path(__file__).parent), entrypoint="test_flow.py:test_flow"
#     ).deploy(
#         work_pool_name="hsuper-slurm-dev",
#         name="test-flow",
#     )

if __name__ == "__main__":
    test_flow.deploy(
        work_pool_name="hsuper-slurm-dev",
        name="test-flow",
        push=True,
        image=DockerImage(
            name="materialsfoundry.io/flows/test-flow",
            dockerfile="Dockerfile",
            tag="dev",
            platform="linux/amd64",
        ),
        job_variables={"queue": "dev", "max_walltime": "00:05:00"},
    )
