from prefect.deployments.runner import DockerImage
from prefect.types.entrypoint import EntrypointType
from test_flow import test_flow

# Prefect 3.7+ uses a temp workspace for flow execution regardless of shell CWD.
# The pull step below reads PREFECT_SLURM_WORKING_DIR, which the script template
# exports before launching prefect flow-run execute (or srun singularity).
# For Singularity images the value is /run (the bind-mount point inside the container).
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
        pull=PULL_STEPS,
    )
