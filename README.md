# Integrating prefect with SLURM workload manager

The prefect-slurm infrastructure allows it to run prefect workflows on HPC
infrastructure mananged by [SLURM](https://slurm.schedmd.com).

## Getting started

Create an infrastructure block

```python
import prefect_slurm

from prefect_slurm import SlurmJob

def create_slurm_job() -> SlurmJob:

    infra = SlurmJob(
      host="<DNS name of the HPC login node>",
      username="<USERNAME>",
      slurm_kwargs={
        "jobname": "prefect",
        "partition": "small",
        "nodes": "1",
        "ntasks-per-node": "72"
      },
    )

    return infra

if __name__ == "__main__":
  create_slurm_job().save("hpc", overwrite=True)
```

Configure a deployment to run on slurm

```bash
prefect deployment build my_flow.py:hpc_job -ib slurmjob/hpc --name hpc/job
```

### Installation

Install `prefect-shell` with `pip`:

```bash
pip install -U 'prefect-slurm @ git+https://github.com/kramergroup/prefect-slurm'
```

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

This infrastructure block is designed to work with Prefect 2. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Feedback

If you encounter any bugs while using `prefect-slurm`, feel free to open an issue in the [prefect-slurm](https://github.com/kramergroup/prefect-slurm) repository.

Feel free to star or watch [`prefect-slurm`](https://github.com/kramergroup/prefect-slurm) for updates too!
 
### Contributing
 
If you'd like to help contribute to fix an issue or add a feature to `prefect-slurm`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).
 
Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/kramergroup/prefect-slurm/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
