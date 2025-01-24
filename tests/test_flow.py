import os
import socket

from prefect import flow


@flow(log_prints=True)
def test_flow() -> str:
    print(f"Hello, world from {socket.gethostname()}!")
    print(f"Running in directory {os.getcwd()}: ")

    return f"Hello, world from {socket.gethostname()}!"


# if __name__ == "__main__":
#     test_flow.from_source(
#         source=str(Path(__file__).parent), entrypoint="test_flow.py:test_flow"
#     ).deploy(
#         work_pool_name="hsuper-slurm-dev",
#         name="test-flow",
#     )
