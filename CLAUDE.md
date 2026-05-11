# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
pip install -e ".[dev]"
pip install -r requirements-dev.txt
pre-commit install
```

## Commands

```bash
# Run tests
pytest

# Run a single test file
pytest tests/hello/test_flow.py

# Lint and format (pre-commit runs all of these, plus autoflake8)
black prefect_slurm/
isort prefect_slurm/
flake8 prefect_slurm/

# Type check
mypy prefect_slurm/

# Check docstring coverage (must stay ≥ 95%)
interrogate prefect_slurm/

# Coverage
coverage run -m pytest && coverage report

# Register the work pool type with Prefect (required before starting a worker)
prefect work-pool create --type slurm --base-job-template ./base-job-template.json slurm-pool --overwrite
```

## Architecture

This is a Prefect 3 worker plugin that submits Prefect flow runs as SLURM batch jobs on HPC clusters.

### Plugin registration

The `prefect.collections` entry point in `pyproject.toml` auto-registers the worker type with Prefect's collection registry. This is what makes `--type slurm` work in the `prefect work-pool create` command without any manual registration.

### Execution flow

1. `SlurmWorker.run()` (`prefect_slurm/worker.py`) is called by Prefect for each flow run.
2. It resolves a `SlurmJobConfiguration` (derived from `BaseJobConfiguration`) against the work pool's `base-job-template.json` and any per-deployment overrides.
3. It calls `_create_backend()` to load a connection block (`SlurmAPIConnection` or `SlurmSSHConnection`) by name from Prefect's block registry. The connection type is determined dynamically by attempting to load each block type.
4. The worker generates a submit script via `_submit_script()` — either from a Jinja2 `script_template` or a minimal default — and calls `backend.submit()`.
5. `_watch_job()` polls for job status every `update_interval_sec` seconds until the job leaves the active states.

### Key classes

| Class | File | Purpose |
|---|---|---|
| `SlurmWorker` | `worker.py` | Prefect worker; entry point for all flow run execution |
| `SlurmJobConfiguration` | `worker.py` | Per-job config (queue, nodes, walltime, script template, pre/post commands) |
| `SlurmVariables` | `worker.py` | Runtime-overridable variables that template `SlurmJobConfiguration` |
| `SlurmAPIConnection` | `worker.py` | Prefect Block holding SLURM REST API credentials |
| `SlurmSSHConnection` | `worker.py` | Prefect Block holding SSH login-node credentials |
| `APIBasedSlurmBackend` | `slurm.py` | Backend that calls the SLURM REST API via `httpx` |
| `CLIBasedSlurmBackend` | `slurm.py` | Backend that runs `sbatch`/`squeue`/`scancel` over SSH via `asyncssh` |
| `JobDefinition` | `api/jobs.py` | Pydantic model for a SLURM job; converts to/from sbatch kwargs |
| `APIEndpoint` | `api/jobs.py` | Low-level async HTTP client for the SLURM REST API |

### Backend selection

`_create_backend()` tries `SlurmAPIConnection.load(name)` first; if that raises `ValueError`, it falls back to `SlurmSSHConnection.load(name)`. There is no explicit config flag — the type is inferred from whichever block exists under `connection_name`.

### Submit script templating

`script_template` in `SlurmJobConfiguration` is a Jinja2 template. It receives variables: `working_dir`, `num_nodes`, `num_processes_per_node`, `max_walltime`, `queue`, `image`, `pre_command`, `post_command`. The template can be supplied as a list of strings (joined with `\n`) or a single string. See `base-job-template.json` for a reference template that sets up an SSH SOCKS proxy, handles Singularity container execution, and wraps pre/post commands.

### Working directory in Prefect 3.7+

In Prefect 3.7, `prefect flow-run execute` always creates a `tempfile.TemporaryDirectory` as its workspace root, ignoring the shell's CWD entirely. The flow's actual working directory is controlled exclusively by the deployment's **pull steps**: the workspace resolver runs pull steps starting from the temp dir, tracks any `os.chdir()` calls, and uses the final CWD as the flow subprocess's `cwd`.

The script template exports `PREFECT_SLURM_WORKING_DIR` before calling `prefect flow-run execute` (or `srun singularity ...`). Each deployment targeting this worker pool must include a pull step that reads this variable:

```yaml
# in prefect.yaml
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: "{{ $PREFECT_SLURM_WORKING_DIR }}"
```

The `{{ $VAR }}` syntax is Prefect's env-var placeholder — `steps/core.py` calls `apply_values(inputs, os.environ)` which strips the `$` prefix and looks up `os.environ["PREFECT_SLURM_WORKING_DIR"]`.

- **No image** (bare Python): `PREFECT_SLURM_WORKING_DIR=$(realpath .)` — the absolute path of the `working_dir/FLOW_RUN_ID` directory created by the script.
- **Singularity image**: `PREFECT_SLURM_WORKING_DIR=/run` — the container's bind-mount point.

### SLURM REST API quirk

The SLURM REST API sometimes appends `"Connection Closed"` to otherwise valid JSON responses. `_extract_valid_json()` in `api/jobs.py` uses a recursive regex to extract only the outermost JSON object, working around this.

### CLI backend terminal state

When `squeue` exits non-zero (job gone from queue), `CLIBasedSlurmBackend.status()` falls back to `sacct` via `_sacct_status()` to retrieve the actual final state. `SlurmJobStatus.UNDEFINED` is only returned if `sacct` also fails or returns no data.

### Tests

`tests/hello/` and `tests/s3/` are integration test flows meant to be deployed to an actual cluster — they are not unit tests. There are no automated unit tests in this repo yet.

### Versioning

`setuptools_scm` derives the version from git tags. Tags must follow the format `prefect-slurm-X.Y.Z`. The `root` in `pyproject.toml` is set to `../../..`, indicating this package lives inside a monorepo and tags are on the repo root.
