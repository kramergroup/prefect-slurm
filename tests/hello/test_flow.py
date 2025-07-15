import socket
from time import sleep

from prefect import flow, task


@task(name="Print Hello")
def print_hello():
    print(f"Hello, world from {socket.gethostname()}!")
    sleep(5)


@task(name="Print Directory")
def print_dir():
    print(f"Hello, world from {socket.gethostname()}!")
    sleep(5)


@flow(log_prints=True)
def test_flow():
    print_hello()
    print_dir()


if __name__ == "__main__":

    from socks import monkey_patch_websockets

    print("Running Prefect CLI from entrypoint.py")
    monkey_patch_websockets()

    test_flow.serve(
        name="test-flow-dev", tags=["test"], description="Test flow", parameters={}
    )
    # test_flow.from_source(
    #     source=str(Path(__file__).parent), entrypoint="test_flow.py:test_flow"
    # ).deploy(
    #     work_pool_name="hsuper-slurm-dev",
    #     name="test-flow",
    # )
