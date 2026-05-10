# Multi-User HPC Credentials Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow each group member to submit SLURM jobs under their own HPC credentials by propagating reverse-proxy user identity through the Prefect server into the worker's block lookup.

**Architecture:** The reverse proxy injects an `X-Remote-User` (or configurable) header from the SSL client cert CN; a patched Prefect server reads that header into `flow_run.created_by.display_value`; the worker derives the connection block name as `slurm-{hpc_system}-{display_value}`. A bash build script automates downloading, patching, and pushing the server image.

**Tech Stack:** Python 3.9+, Pydantic v1 (Prefect 2.x uses v1 model API), pytest, pytest-asyncio, unittest.mock, bash, Docker, PyPI tarball download via `pip download`.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `prefect_slurm/worker.py` | New `hpc_system` field; optional `connection_name`; new resolution/parsing helpers; refactored `_create_backend_from_name` |
| Create | `tests/test_worker_multi_user.py` | Unit tests for the new worker logic |
| Modify | `base-job-template.json` | Add `hpc_system` placeholder; null out hardcoded `connection_name` |
| Modify | `base-job-template-socks.json` | Same changes as above |
| Create | `scripts/patch-prefect-server.sh` | Download → patch → build → push |
| Create | `scripts/prefect-server-x-remote-user.patch` | Canonical unified diff for `dependencies.py` |

---

## Task 1: Test infrastructure — helpers and fixtures

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_worker_multi_user.py`

- [ ] **Step 1: Create empty `tests/__init__.py`**

```bash
touch tests/__init__.py
```

- [ ] **Step 2: Create the test file with helpers only (no test functions yet)**

Create `tests/test_worker_multi_user.py`:

```python
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from prefect.client.schemas import FlowRun
from prefect.server.schemas.core import CreatedBy

from prefect_slurm.slurm import APIBasedSlurmBackend, CLIBasedSlurmBackend
from prefect_slurm.worker import SlurmAPIConnection, SlurmJobConfiguration, SlurmSSHConnection, SlurmWorker


def make_flow_run(display_value=None):
    """Return a minimal FlowRun with optional created_by identity."""
    created_by = (
        CreatedBy(type="USER", display_value=display_value)
        if display_value is not None
        else None
    )
    return FlowRun.construct(
        id=uuid.uuid4(),
        flow_id=uuid.uuid4(),
        name="test-run",
        created_by=created_by,
        tags=[],
        labels={},
        parameters={},
        state=None,
    )


def make_config(**kwargs):
    """Return a SlurmJobConfiguration with only the given fields set."""
    return SlurmJobConfiguration.construct(**kwargs)


@pytest.fixture
def worker():
    """Return a SlurmWorker instance without full Prefect initialisation."""
    return SlurmWorker.__new__(SlurmWorker)


def mock_api_block(username="jsmith"):
    block = MagicMock(spec=SlurmAPIConnection)
    block.endpoint = "http://slurm.example.com:6820"
    block.username = username
    block.auth_token = SecretStr("tok")
    block.insecure = False
    return block


def mock_ssh_block(username="jsmith"):
    block = MagicMock(spec=SlurmSSHConnection)
    block.host = "login.hpc.example.com"
    block.username = username
    block.password = SecretStr("s3cr3t")
    return block
```

- [ ] **Step 3: Verify imports resolve**

```bash
cd /Users/deniskramer/temp/prefect/prefect-slurm && python -c "from tests.test_worker_multi_user import make_flow_run, make_config; print('ok')"
```

Expected output: `ok`

- [ ] **Step 4: Commit**

```bash
git add tests/__init__.py tests/test_worker_multi_user.py
git commit -m "test: add test infrastructure for multi-user credential tests"
```

---

## Task 2: Make `connection_name` optional; add `hpc_system`

**Files:**
- Modify: `prefect_slurm/worker.py:128-134` (the `connection_name` field in `SlurmJobConfiguration`)
- Modify: `tests/test_worker_multi_user.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_worker_multi_user.py`:

```python
# ---------------------------------------------------------------------------
# Task 2: SlurmJobConfiguration field schema
# ---------------------------------------------------------------------------

def test_connection_name_is_optional():
    """connection_name must accept None without raising a validation error."""
    cfg = SlurmJobConfiguration(name="t", command="echo hi", connection_name=None)
    assert cfg.connection_name is None


def test_hpc_system_field_exists():
    """hpc_system field must exist and default to None."""
    cfg = SlurmJobConfiguration(name="t", command="echo hi", connection_name=None)
    assert cfg.hpc_system is None


def test_hpc_system_can_be_set():
    """hpc_system accepts a string value."""
    cfg = SlurmJobConfiguration(name="t", command="echo hi", hpc_system="issy")
    assert cfg.hpc_system == "issy"
```

- [ ] **Step 2: Run tests — expect failure**

```bash
pytest tests/test_worker_multi_user.py::test_connection_name_is_optional tests/test_worker_multi_user.py::test_hpc_system_field_exists tests/test_worker_multi_user.py::test_hpc_system_can_be_set -v
```

Expected: all three FAIL (connection_name required; hpc_system not defined).

- [ ] **Step 3: Modify `SlurmJobConfiguration` in `worker.py`**

In `prefect_slurm/worker.py`, find the `connection_name` field (around line 128) and change it from required to optional:

```python
# BEFORE:
    connection_name: str = Field(
        title="Slurm connection",
        description="""
            The connection block name to access the SLURM manager. This can either
            be a API endpoint or a SSH connection.
        """,
    )

# AFTER:
    connection_name: Optional[str] = Field(
        default=None,
        title="Slurm connection",
        description="""
            The connection block name to access the SLURM manager. This can either
            be a API endpoint or a SSH connection. When omitted, the worker derives
            the name from hpc_system and the authenticated user identity.
        """,
    )
```

Immediately after the `connection_name` field, add:

```python
    hpc_system: Optional[str] = Field(
        default=None,
        title="HPC system name",
        description=(
            "Short identifier for the HPC system (e.g. 'issy'). Set once as a "
            "literal in the work pool's base-job-template.json — not in variables. "
            "Used to derive the connection block name as "
            "'slurm-{hpc_system}-{submitter}' when connection_name is not set."
        ),
    )
```

- [ ] **Step 4: Run tests — expect pass**

```bash
pytest tests/test_worker_multi_user.py::test_connection_name_is_optional tests/test_worker_multi_user.py::test_hpc_system_field_exists tests/test_worker_multi_user.py::test_hpc_system_can_be_set -v
```

Expected: all three PASS.

- [ ] **Step 5: Commit**

```bash
git add prefect_slurm/worker.py tests/test_worker_multi_user.py
git commit -m "feat: make connection_name optional; add hpc_system to SlurmJobConfiguration"
```

---

## Task 3: Add `_resolve_connection_name()` helper

**Files:**
- Modify: `prefect_slurm/worker.py` (inside `SlurmWorker`)
- Modify: `tests/test_worker_multi_user.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_worker_multi_user.py`:

```python
# ---------------------------------------------------------------------------
# Task 3: _resolve_connection_name
# ---------------------------------------------------------------------------

def test_resolve_uses_explicit_connection_name(worker):
    cfg = make_config(connection_name="my-explicit-block", hpc_system="issy")
    flow_run = make_flow_run(display_value="jsmith")
    assert worker._resolve_connection_name(cfg, flow_run) == "my-explicit-block"


def test_resolve_derives_name_from_hpc_system_and_identity(worker):
    cfg = make_config(connection_name=None, hpc_system="issy")
    flow_run = make_flow_run(display_value="jsmith")
    assert worker._resolve_connection_name(cfg, flow_run) == "slurm-issy-jsmith"


def test_resolve_raises_when_no_identity(worker):
    cfg = make_config(connection_name=None, hpc_system="issy")
    flow_run = make_flow_run(display_value=None)
    with pytest.raises(AttributeError, match="Cannot determine SLURM connection"):
        worker._resolve_connection_name(cfg, flow_run)


def test_resolve_raises_when_no_hpc_system_and_no_connection_name(worker):
    cfg = make_config(connection_name=None, hpc_system=None)
    flow_run = make_flow_run(display_value="jsmith")
    with pytest.raises(AttributeError, match="Cannot determine SLURM connection"):
        worker._resolve_connection_name(cfg, flow_run)


def test_resolve_raises_when_created_by_is_none(worker):
    cfg = make_config(connection_name=None, hpc_system="issy")
    flow_run = make_flow_run()  # no created_by at all
    with pytest.raises(AttributeError, match="Cannot determine SLURM connection"):
        worker._resolve_connection_name(cfg, flow_run)
```

- [ ] **Step 2: Run tests — expect failure**

```bash
pytest tests/test_worker_multi_user.py -k "resolve" -v
```

Expected: all FAIL with `AttributeError: 'SlurmWorker' object has no attribute '_resolve_connection_name'`.

- [ ] **Step 3: Add `_resolve_connection_name` to `SlurmWorker`**

Inside `SlurmWorker` in `worker.py`, add this method before `_create_backend`:

```python
    def _resolve_connection_name(
        self,
        configuration: SlurmJobConfiguration,
        flow_run: FlowRun,
    ) -> str:
        """Return the connection block name to use for this flow run.

        Explicit connection_name always wins. Otherwise derives the name
        from hpc_system and the authenticated user identity supplied by
        the reverse proxy via flow_run.created_by.display_value.
        """
        if configuration.connection_name:
            return configuration.connection_name
        if (
            configuration.hpc_system
            and flow_run.created_by
            and flow_run.created_by.display_value
        ):
            return f"slurm-{configuration.hpc_system}-{flow_run.created_by.display_value}"
        raise AttributeError(
            "Cannot determine SLURM connection: set 'connection_name' explicitly, "
            "or set 'hpc_system' and ensure the reverse proxy injects user identity "
            "via X-Remote-User (or configured PREFECT_SERVER_USER_HEADER)."
        )
```

- [ ] **Step 4: Run tests — expect pass**

```bash
pytest tests/test_worker_multi_user.py -k "resolve" -v
```

Expected: all 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add prefect_slurm/worker.py tests/test_worker_multi_user.py
git commit -m "feat: add _resolve_connection_name to SlurmWorker"
```

---

## Task 4: Add `_parse_infrastructure_pid()` helper

**Files:**
- Modify: `prefect_slurm/worker.py` (inside `SlurmWorker`)
- Modify: `tests/test_worker_multi_user.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_worker_multi_user.py`:

```python
# ---------------------------------------------------------------------------
# Task 4: _parse_infrastructure_pid
# ---------------------------------------------------------------------------

def test_parse_pid_new_format(worker):
    job_id, conn = worker._parse_infrastructure_pid("12345@slurm-issy-jsmith")
    assert job_id == "12345"
    assert conn == "slurm-issy-jsmith"


def test_parse_pid_old_format_uses_fallback(worker):
    job_id, conn = worker._parse_infrastructure_pid("12345", fallback_connection="slurm-old")
    assert job_id == "12345"
    assert conn == "slurm-old"


def test_parse_pid_old_format_no_fallback_raises(worker):
    with pytest.raises(AttributeError, match="Cannot determine SLURM connection"):
        worker._parse_infrastructure_pid("12345")


def test_parse_pid_connection_name_with_at_symbol(worker):
    """Connection names won't contain @, but rsplit handles edge cases."""
    job_id, conn = worker._parse_infrastructure_pid("99@slurm-hawk-user@domain")
    assert job_id == "99"
    assert conn == "slurm-hawk-user@domain"
```

- [ ] **Step 2: Run tests — expect failure**

```bash
pytest tests/test_worker_multi_user.py -k "parse_pid" -v
```

Expected: all FAIL.

- [ ] **Step 3: Add `_parse_infrastructure_pid` to `SlurmWorker`**

Add this static method to `SlurmWorker`, directly after `_resolve_connection_name`:

```python
    @staticmethod
    def _parse_infrastructure_pid(
        infrastructure_pid: str,
        fallback_connection: Optional[str] = None,
    ):
        """Parse infrastructure_pid into (job_id, connection_name).

        New format: "{job_id}@{connection_name}" — written by run().
        Old format: "{job_id}" — written before this feature existed;
                    fallback_connection is used (from configuration.connection_name).
        """
        if "@" in infrastructure_pid:
            job_id, conn_name = infrastructure_pid.rsplit("@", 1)
            return job_id, conn_name
        if fallback_connection:
            return infrastructure_pid, fallback_connection
        raise AttributeError(
            "Cannot determine SLURM connection from infrastructure_pid "
            f"'{infrastructure_pid}': no '@' separator and no fallback connection set."
        )
```

- [ ] **Step 4: Run tests — expect pass**

```bash
pytest tests/test_worker_multi_user.py -k "parse_pid" -v
```

Expected: all 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add prefect_slurm/worker.py tests/test_worker_multi_user.py
git commit -m "feat: add _parse_infrastructure_pid to SlurmWorker"
```

---

## Task 5: Refactor `_create_backend` → `_create_backend_from_name`

**Files:**
- Modify: `prefect_slurm/worker.py`
- Modify: `tests/test_worker_multi_user.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_worker_multi_user.py`:

```python
# ---------------------------------------------------------------------------
# Task 5: _create_backend_from_name
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_backend_from_name_returns_api_backend(worker):
    with patch.object(SlurmAPIConnection, "load", new=AsyncMock(return_value=mock_api_block())):
        backend = await worker._create_backend_from_name("slurm-issy-jsmith")
    assert isinstance(backend, APIBasedSlurmBackend)


@pytest.mark.asyncio
async def test_create_backend_from_name_falls_back_to_ssh(worker):
    with patch.object(SlurmAPIConnection, "load", new=AsyncMock(side_effect=ValueError)), \
         patch.object(SlurmSSHConnection, "load", new=AsyncMock(return_value=mock_ssh_block())):
        backend = await worker._create_backend_from_name("slurm-issy-jsmith")
    assert isinstance(backend, CLIBasedSlurmBackend)


@pytest.mark.asyncio
async def test_create_backend_from_name_raises_when_neither_block_found(worker):
    with patch.object(SlurmAPIConnection, "load", new=AsyncMock(side_effect=ValueError)), \
         patch.object(SlurmSSHConnection, "load", new=AsyncMock(side_effect=ValueError)):
        with pytest.raises(AttributeError, match="No valid connection"):
            await worker._create_backend_from_name("slurm-issy-jsmith")
```

- [ ] **Step 2: Run tests — expect failure**

```bash
pytest tests/test_worker_multi_user.py -k "create_backend_from_name" -v
```

Expected: all FAIL with `AttributeError: 'SlurmWorker' object has no attribute '_create_backend_from_name'`.

- [ ] **Step 3: Rename and refactor `_create_backend` in `worker.py`**

Replace the existing `_create_backend` method entirely:

```python
    async def _create_backend_from_name(self, connection_name: str) -> SlurmBackend:
        """Load the connection block identified by connection_name and return
        the appropriate backend (API or SSH)."""
        try:
            connection_block = await SlurmAPIConnection.load(connection_name)
            return APIBasedSlurmBackend(
                endpoint=connection_block.endpoint,
                username=connection_block.username,
                token=connection_block.auth_token,
                insecure=connection_block.insecure,
            )
        except ValueError:
            try:
                connection_block = await SlurmSSHConnection.load(connection_name)
                return CLIBasedSlurmBackend(
                    host=connection_block.host,
                    username=connection_block.username,
                    password=connection_block.password,
                )
            except ValueError:
                raise AttributeError(
                    f"No valid connection block found for '{connection_name}'. "
                    "Create a SlurmAPIConnection or SlurmSSHConnection block with that name."
                )
```

- [ ] **Step 4: Run tests — expect pass**

```bash
pytest tests/test_worker_multi_user.py -k "create_backend_from_name" -v
```

Expected: all 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add prefect_slurm/worker.py tests/test_worker_multi_user.py
git commit -m "refactor: rename _create_backend to _create_backend_from_name (takes explicit name)"
```

---

## Task 6: Wire new helpers into `run()`, `kill_infrastructure()`, `_watch_job()`

**Files:**
- Modify: `prefect_slurm/worker.py`

This task updates the three call sites. There are no new unit tests here — the integration is validated by running the full test suite. The existing helpers are already tested.

- [ ] **Step 1: Update `_watch_job` signature**

Change the signature and body of `_watch_job` from:

```python
    async def _watch_job(
        self, job_id: int, configuration: SlurmJobConfiguration
    ) -> SlurmJobStatus:
        backend = await self._create_backend(configuration)
```

to:

```python
    async def _watch_job(
        self, job_id: int, connection_name: str
    ) -> SlurmJobStatus:
        backend = await self._create_backend_from_name(connection_name)
```

The rest of `_watch_job` is unchanged.

- [ ] **Step 2: Update `run()`**

Replace the two lines in `run()` that call `_create_backend` and `_watch_job`:

```python
# BEFORE (two separate calls):
        backend = await self._create_backend(configuration)
        ...
        job_status = await self._watch_job(slurm_job_id, configuration)

# AFTER:
        connection_name = self._resolve_connection_name(configuration, flow_run)
        backend = await self._create_backend_from_name(connection_name)
```

Also update the `task_status.started(...)` call and `_watch_job` call in `run()`:

```python
# BEFORE:
        if task_status:
            task_status.started(slurm_job_id)
        job_status = await self._watch_job(slurm_job_id, configuration)

# AFTER:
        if task_status:
            task_status.started(f"{slurm_job_id}@{connection_name}")
        job_status = await self._watch_job(slurm_job_id, connection_name)
```

- [ ] **Step 3: Update `kill_infrastructure()`**

Replace the body of `kill_infrastructure()`:

```python
# BEFORE:
        backend = await self._create_backend(configuration)
        await backend.kill(infrastructure_pid, grace_seconds=grace_seconds)

# AFTER:
        job_id, conn_name = self._parse_infrastructure_pid(
            infrastructure_pid, fallback_connection=configuration.connection_name
        )
        backend = await self._create_backend_from_name(conn_name)
        await backend.kill(job_id, grace_seconds=grace_seconds)
```

- [ ] **Step 4: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: all tests PASS. No references to `_create_backend` should remain in worker.py:

```bash
grep "_create_backend[^_]" prefect_slurm/worker.py
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add prefect_slurm/worker.py
git commit -m "feat: wire multi-user credential resolution into run/kill/watch"
```

---

## Task 7: Update `base-job-template.json` and `base-job-template-socks.json`

**Files:**
- Modify: `base-job-template.json`
- Modify: `base-job-template-socks.json`

- [ ] **Step 1: Update `base-job-template.json`**

In `base-job-template.json`, inside `"job_configuration"`:

- Replace `"connection_name": "hsuper-kramerd"` with `"connection_name": null`
- Add `"hpc_system": "CHANGE_ME"` on the line before `"connection_name"`

Result for that section:

```json
  "job_configuration": {
    "hpc_system": "CHANGE_ME",
    "connection_name": null,
    "image": "{{ image }}",
    ...
```

- [ ] **Step 2: Update `base-job-template-socks.json`**

Apply the same change to `base-job-template-socks.json`:

- Replace `"connection_name": "hsuper-kramerd"` with `"connection_name": null`
- Add `"hpc_system": "CHANGE_ME"` before `"connection_name"`

- [ ] **Step 3: Verify JSON is valid**

```bash
python -c "import json; json.load(open('base-job-template.json')); json.load(open('base-job-template-socks.json')); print('ok')"
```

Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add base-job-template.json base-job-template-socks.json
git commit -m "feat: add hpc_system placeholder to base job templates; null out hardcoded connection_name"
```

---

## Task 8: Write the Prefect server patch file

**Files:**
- Create: `scripts/prefect-server-x-remote-user.patch`

This task generates a unified diff against a known version of Prefect's
`dependencies.py`. The patch adds two env-var-configurable module constants and
extends `get_created_by` to read a reverse-proxy-injected user header.

> **Important:** `Request` is already imported in `prefect/server/api/dependencies.py`
> via `from prefect._vendor.starlette.requests import Request` — no new import needed.

- [ ] **Step 1: Create the `scripts/` directory**

```bash
mkdir -p scripts
```

- [ ] **Step 2: Download Prefect 2.20.25 source to a temp directory**

```bash
mkdir -p /tmp/prefect-patch-work
pip download prefect==2.20.25 --no-deps --no-binary prefect -d /tmp/prefect-patch-work
tar -xzf /tmp/prefect-patch-work/prefect-2.20.25.tar.gz -C /tmp/prefect-patch-work
```

- [ ] **Step 3: Copy the original file**

```bash
cp /tmp/prefect-patch-work/prefect-2.20.25/src/prefect/server/api/dependencies.py \
   /tmp/prefect-patch-work/dependencies.py.orig
```

- [ ] **Step 4: Write the patched version**

```bash
cp /tmp/prefect-patch-work/dependencies.py.orig \
   /tmp/prefect-patch-work/dependencies.py.patched
```

Open `/tmp/prefect-patch-work/dependencies.py.patched` and apply these two changes:

**Change A — add imports after `import logging`:**

```python
# BEFORE:
import logging
from base64 import b64decode

# AFTER:
import logging
import os
import re
from base64 import b64decode
```

**Change B — add module constants after `PREFECT_API_DEFAULT_LIMIT` import (before `def provide_request_api_version`):**

```python
# BEFORE:
from prefect.settings import PREFECT_API_DEFAULT_LIMIT


def provide_request_api_version

# AFTER:
from prefect.settings import PREFECT_API_DEFAULT_LIMIT

# Configurable user identity header injected by reverse proxy (nginx / Traefik)
_USER_HEADER = os.getenv("PREFECT_SERVER_USER_HEADER", "x-remote-user").lower().replace("_", "-")
_USER_HEADER_REGEX = os.getenv("PREFECT_SERVER_USER_HEADER_REGEX", "")


def provide_request_api_version
```

**Change C — extend `get_created_by` to add `request: Request` parameter and read the header:**

```python
# BEFORE:
def get_created_by(
    prefect_automation_id: Optional[UUID] = Header(None, include_in_schema=False),
    prefect_automation_name: Optional[str] = Header(None, include_in_schema=False),
) -> Optional[schemas.core.CreatedBy]:
    """A dependency that returns the provenance information to use when creating objects
    during this API call."""
    if prefect_automation_id and prefect_automation_name:
        try:
            display_value = b64decode(prefect_automation_name.encode()).decode()
        except Exception:
            display_value = None

        if display_value:
            return schemas.core.CreatedBy(
                id=prefect_automation_id,
                type="AUTOMATION",
                display_value=display_value,
            )

    return None

# AFTER:
def get_created_by(
    request: Request,
    prefect_automation_id: Optional[UUID] = Header(None, include_in_schema=False),
    prefect_automation_name: Optional[str] = Header(None, include_in_schema=False),
) -> Optional[schemas.core.CreatedBy]:
    """A dependency that returns the provenance information to use when creating objects
    during this API call."""
    if prefect_automation_id and prefect_automation_name:
        try:
            display_value = b64decode(prefect_automation_name.encode()).decode()
        except Exception:
            display_value = None

        if display_value:
            return schemas.core.CreatedBy(
                id=prefect_automation_id,
                type="AUTOMATION",
                display_value=display_value,
            )

    raw = request.headers.get(_USER_HEADER)
    if raw:
        if _USER_HEADER_REGEX:
            m = re.search(_USER_HEADER_REGEX, raw)
            username = m.group(1) if m else None
        else:
            username = raw.strip()
        if username:
            return schemas.core.CreatedBy(type="USER", display_value=username)

    return None
```

- [ ] **Step 5: Generate the patch file**

```bash
diff -u \
    /tmp/prefect-patch-work/dependencies.py.orig \
    /tmp/prefect-patch-work/dependencies.py.patched \
    > scripts/prefect-server-x-remote-user.patch
```

Verify the patch has three hunks:

```bash
grep "^@@" scripts/prefect-server-x-remote-user.patch
```

Expected: three `@@` lines.

- [ ] **Step 6: Verify the patch applies cleanly**

```bash
patch --dry-run -p0 \
    /tmp/prefect-patch-work/dependencies.py.orig \
    scripts/prefect-server-x-remote-user.patch
```

Expected: `patching file ... (dry run)` with no errors.

- [ ] **Step 7: Commit**

```bash
git add scripts/prefect-server-x-remote-user.patch
git commit -m "feat: add Prefect server patch for reverse-proxy user identity header"
```

---

## Task 9: Write `scripts/patch-prefect-server.sh`

**Files:**
- Create: `scripts/patch-prefect-server.sh`

- [ ] **Step 1: Create the script**

Create `scripts/patch-prefect-server.sh`:

```bash
#!/usr/bin/env bash
# patch-prefect-server.sh — Download a specific Prefect version, apply the
# X-Remote-User identity patch, build a Docker image, and optionally push it.
#
# Usage:
#   ./scripts/patch-prefect-server.sh \
#       --version 2.20.25 \
#       --registry registry.example.org/iscc \
#       --image-name prefect-server \
#       [--push]
#
# The patch file scripts/prefect-server-x-remote-user.patch must exist.
# It is version-specific: if the target Prefect version differs from the one
# the patch was generated against, 'patch' will report a failure and the
# script will exit non-zero.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_FILE="${SCRIPT_DIR}/prefect-server-x-remote-user.patch"

VERSION=""
REGISTRY=""
IMAGE_NAME="prefect-server"
PUSH=false

usage() {
    echo "Usage: $0 --version VERSION --registry REGISTRY [--image-name NAME] [--push]"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)    VERSION="$2";    shift 2 ;;
        --registry)   REGISTRY="$2";  shift 2 ;;
        --image-name) IMAGE_NAME="$2"; shift 2 ;;
        --push)       PUSH=true;       shift   ;;
        *) usage ;;
    esac
done

[[ -z "$VERSION"  ]] && { echo "ERROR: --version is required";  usage; }
[[ -z "$REGISTRY" ]] && { echo "ERROR: --registry is required"; usage; }
[[ -f "$PATCH_FILE" ]] || { echo "ERROR: patch file not found: $PATCH_FILE"; exit 1; }

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "==> Downloading prefect==${VERSION} source..."
pip download "prefect==${VERSION}" --no-deps --no-binary prefect -d "$WORKDIR" -q
TARBALL="$(ls "$WORKDIR"/prefect-*.tar.gz | head -1)"
[[ -z "$TARBALL" ]] && { echo "ERROR: source tarball not found in $WORKDIR"; exit 1; }
tar -xzf "$TARBALL" -C "$WORKDIR"
SRC_DIR="$(ls -d "$WORKDIR"/prefect-*/)"

DEPS_FILE="${SRC_DIR}src/prefect/server/api/dependencies.py"
[[ -f "$DEPS_FILE" ]] || { echo "ERROR: dependencies.py not found at $DEPS_FILE"; exit 1; }

echo "==> Applying patch..."
patch --forward -p1 -d "$SRC_DIR" < "$PATCH_FILE"

FULL_TAG="${REGISTRY}/${IMAGE_NAME}:${VERSION}-patched"
LATEST_TAG="${REGISTRY}/${IMAGE_NAME}:latest-patched"

echo "==> Writing Dockerfile..."
cat > "$WORKDIR/Dockerfile" <<DOCKERFILE
FROM prefecthq/prefect:${VERSION}

# Replace dependencies.py with the patched version
COPY dependencies.py /tmp/patched_dependencies.py
RUN SITE=\$(python -c "import prefect, os; print(os.path.dirname(prefect.__file__))") && \\
    cp /tmp/patched_dependencies.py "\${SITE}/server/api/dependencies.py"

# Runtime-configurable — override in your container environment
ENV PREFECT_SERVER_USER_HEADER=x-remote-user
ENV PREFECT_SERVER_USER_HEADER_REGEX=
DOCKERFILE

cp "$DEPS_FILE" "$WORKDIR/dependencies.py"

echo "==> Building image ${FULL_TAG}..."
docker build \
    --tag "$FULL_TAG" \
    --tag "$LATEST_TAG" \
    "$WORKDIR"

echo "==> Built: ${FULL_TAG}"
echo "==> Tagged: ${LATEST_TAG}"

if [[ "$PUSH" == true ]]; then
    echo "==> Pushing ${FULL_TAG}..."
    docker push "$FULL_TAG"
    echo "==> Pushing ${LATEST_TAG}..."
    docker push "$LATEST_TAG"
    echo "==> Push complete."
fi
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/patch-prefect-server.sh
```

- [ ] **Step 3: Smoke-test argument parsing (no Docker needed)**

```bash
bash -n scripts/patch-prefect-server.sh && echo "syntax ok"
./scripts/patch-prefect-server.sh 2>&1 | head -3
```

Expected first line: `Usage: ./scripts/patch-prefect-server.sh --version VERSION --registry REGISTRY [--image-name NAME] [--push]`

- [ ] **Step 4: Full smoke-test (requires Docker and network)**

```bash
./scripts/patch-prefect-server.sh \
    --version 2.20.25 \
    --registry localhost:5000 \
    --image-name prefect-server
```

Expected: download → patch → build → two tags reported. No push.

- [ ] **Step 5: Commit**

```bash
git add scripts/patch-prefect-server.sh
git commit -m "feat: add patch-prefect-server.sh build script"
```

---

## Task 10: Final checks and run full test suite

- [ ] **Step 1: Run the complete test suite**

```bash
pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 2: Verify no stale references to old `_create_backend`**

```bash
grep -n "_create_backend[^_]" prefect_slurm/worker.py
```

Expected: no output.

- [ ] **Step 3: Verify `hpc_system` is absent from `SlurmVariables`**

```bash
grep -n "hpc_system" prefect_slurm/worker.py
```

Expected: only lines inside `SlurmJobConfiguration`, not inside `SlurmVariables`.

- [ ] **Step 4: Check JSON templates are valid and contain the new field**

```bash
python -c "
import json
for f in ['base-job-template.json', 'base-job-template-socks.json']:
    d = json.load(open(f))
    assert 'hpc_system' in d['job_configuration'], f'{f} missing hpc_system'
    assert d['job_configuration']['connection_name'] is None, f'{f} connection_name not null'
    print(f'{f}: ok')
"
```

Expected:
```
base-job-template.json: ok
base-job-template-socks.json: ok
```

- [ ] **Step 5: Final commit if any cleanup needed, otherwise confirm done**

```bash
git log --oneline -8
```

Expected commits (in order):
- `feat: add patch-prefect-server.sh build script`
- `feat: add Prefect server patch for reverse-proxy user identity header`
- `feat: add hpc_system placeholder to base job templates; null out hardcoded connection_name`
- `feat: wire multi-user credential resolution into run/kill/watch`
- `refactor: rename _create_backend to _create_backend_from_name (takes explicit name)`
- `feat: add _parse_infrastructure_pid to SlurmWorker`
- `feat: add _resolve_connection_name to SlurmWorker`
- `feat: make connection_name optional; add hpc_system to SlurmJobConfiguration`

---

## Self-Review Notes

**Spec coverage:**
- ✅ `hpc_system` in `SlurmJobConfiguration` only (not `SlurmVariables`) — Task 2
- ✅ `connection_name` optional with backward compat — Tasks 2, 6
- ✅ Block name derived as `slurm-{hpc_system}-{display_value}` — Task 3
- ✅ `infrastructure_pid` encodes resolved name; `kill_infrastructure` parses it — Tasks 4, 6
- ✅ `_create_backend` refactored — Task 5
- ✅ `base-job-template.json` + socks variant updated — Task 7
- ✅ `.patch` file generated against real source — Task 8
- ✅ Build script with `--version`, `--registry`, `--image-name`, `--push` — Task 9
- ✅ Reverse proxy config documented in spec (not code) — design doc

**Type consistency check:**
- `_resolve_connection_name(configuration, flow_run) -> str` — used in Task 6 `run()`
- `_parse_infrastructure_pid(pid, fallback_connection=None) -> tuple` — used in Task 6 `kill_infrastructure()`
- `_create_backend_from_name(connection_name: str) -> SlurmBackend` — used in Tasks 5, 6
- `_watch_job(job_id, connection_name: str)` — updated signature used in Task 6
