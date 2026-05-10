# Integrating prefect with SLURM workload manager

The prefect-slurm infrastructure allows it to run prefect workflows on HPC
infrastructure mananged by [SLURM](https://slurm.schedmd.com).

## Getting started

This integration requires a dedicated work-pool type that needs to be registered with
the prefect-api:

```bash
prefect work-pool create --type slurm --base-job-template ./base-job-template.json slurm-pool --overwrite
```

Once the work pool exists, a worker is needed to pull flow runs and submit them to the slurm infrastructure. This
worker needs access to the prefect API as well as the slurm cluster (either via ssh or the slurm API). This can 
be achieved by running the provided docker container on premises.

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

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
