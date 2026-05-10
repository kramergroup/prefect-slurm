# Multi-User HPC Credentials Design

**Date:** 2026-05-10  
**Status:** Approved

## Problem

The prefect-slurm plugin currently stores SLURM credentials in a single Prefect Block
referenced by name from `SlurmJobConfiguration.connection_name`. All flow runs on a work
pool share that one block — and therefore one set of HPC credentials. Groups where each
member has their own HPC account cannot use the plugin without a shared service account.

## Goals

- Each user submits jobs under their own HPC credentials.
- Transparent at submission time: no per-run user action required.
- No custom code or tooling installed on users' machines.
- Works with a self-hosted Prefect server behind a reverse proxy (nginx or Traefik) that
  performs SSL client certificate authentication.
- One work pool per HPC system; the pool itself encodes which system it targets.

## Non-Goals

- Supporting Prefect Cloud (cloud has its own identity layer).
- Handling HPC systems that use API-based SLURM auth (SSH-only scope for now; API blocks
  work unchanged when `connection_name` is set explicitly).

---

## Architecture

### Identity flow

```
User browser / CLI
  └─ HTTPS with SSL client certificate (CN = jsmith)
      └─ Reverse proxy (nginx or Traefik)
          └─ Injects configurable header, e.g. X-Remote-User: jsmith
              └─ Patched Prefect server
                  └─ flow_run.created_by.display_value = "jsmith"
                      └─ SlurmWorker
                          └─ derives block name: "slurm-issy-jsmith"
                              └─ SlurmSSHConnection block (jsmith's creds)
                                  └─ CLIBasedSlurmBackend → SLURM job submitted as jsmith
```

### Three components change

| Component | Change |
|---|---|
| Prefect server | `get_created_by` reads a configurable HTTP header and populates `created_by` |
| prefect-slurm plugin | `hpc_system` added to `SlurmJobConfiguration`; `connection_name` becomes optional |
| Build tooling | `scripts/patch-prefect-server.sh` automates patching, building and pushing the server image |

---

## Component 1: Prefect server patch

### File changed

`src/prefect/server/api/dependencies.py` — function `get_created_by`.

### Patch behaviour

Two environment variables control the patch:

| Variable | Default | Description |
|---|---|---|
| `PREFECT_SERVER_USER_HEADER` | `x-remote-user` | HTTP header the proxy injects (case-insensitive, hyphens normalised to underscores for FastAPI) |
| `PREFECT_SERVER_USER_HEADER_REGEX` | *(empty)* | Optional regex with one capture group. When set, the username is extracted from the header value via `re.search(...).group(1)`. When empty, the raw header value is used as-is. |

Automation-triggered runs are unaffected: the existing `prefect-automation-id` /
`prefect-automation-name` logic runs first and short-circuits.

### Patch (unified diff)

```diff
--- a/src/prefect/server/api/dependencies.py
+++ b/src/prefect/server/api/dependencies.py
@@ -1,4 +1,5 @@
+import os, re
 from base64 import b64decode
 from typing import Optional
 from uuid import UUID
@@ -10,8 +11,14 @@ from prefect.server import schemas
+_USER_HEADER     = os.getenv("PREFECT_SERVER_USER_HEADER", "x-remote-user") \
+                      .lower().replace("-", "_")
+_USER_HEADER_REGEX = os.getenv("PREFECT_SERVER_USER_HEADER_REGEX", "")
+
 def get_created_by(
     prefect_automation_id: Optional[UUID] = Header(None, include_in_schema=False),
     prefect_automation_name: Optional[str] = Header(None, include_in_schema=False),
+    **extra_headers,   # not used; actual header read from Request below
 ) -> Optional[schemas.core.CreatedBy]:
```

> **Implementation note:** FastAPI's `Header` dependency cannot accept a runtime-variable
> name. The function signature above is illustrative. The actual implementation injects a
> `Request` parameter and reads `request.headers.get(_USER_HEADER)` directly.

### Reverse proxy configuration

**nginx** (extract CN from full subject DN via `map`, then forward):

```nginx
map $ssl_client_s_dn $remote_user_cn {
    default "";
    ~/CN=(?<CN>[^/,]+) $CN;
}

location / {
    # proxy_set_header overwrites any client-supplied value, preventing spoofing
    proxy_set_header X-Remote-User $remote_user_cn;
}
```

**Traefik — option A (mTLS Header Plugin, recommended):**

```yaml
middlewares:
  inject-user:
    plugin:
      mtlsHeader:
        headers:
          X-Remote-User: '[[.Cert.Subject.CommonName]]'
```

Set `PREFECT_SERVER_USER_HEADER=x-remote-user`, no regex needed.

**Traefik — option B (built-in `passTLSClientCert`, no plugin):**

```yaml
middlewares:
  pass-cert:
    passTLSClientCert:
      info:
        subject:
          commonName: true
```

Set:
```
PREFECT_SERVER_USER_HEADER=x-forwarded-tls-client-cert-info
PREFECT_SERVER_USER_HEADER_REGEX=CN=([^,]+)
```

---

## Component 2: prefect-slurm plugin changes

### `SlurmJobConfiguration` — new field

```python
hpc_system: Optional[str] = Field(
    default=None,
    title="HPC system name",
    description=(
        "Short identifier for the HPC system (e.g. 'issy'). Set once in the "
        "work pool's base-job-template.json, not per-job. When set, the worker "
        "derives the connection block name as 'slurm-{hpc_system}-{submitter}' "
        "from the authenticated user identity."
    ),
)
```

`hpc_system` is added to `SlurmJobConfiguration` **only** — not to `SlurmVariables`.
This means it is a pool-level constant that users cannot override per-submission.

`connection_name` changes from required to `Optional[str] = Field(default=None, ...)`.

### Connection name resolution and `infrastructure_pid`

`kill_infrastructure()` in Prefect's worker contract does **not** receive `flow_run`, so
the connection name must be resolved once in `run()` and carried forward independently of
the configuration object (which is reconstructed from stored job variables on cancel).

**Resolution happens in `run()` before job submission:**

```python
async def run(self, flow_run, configuration, task_status=None):
    name = configuration.connection_name
    if not name:
        if (configuration.hpc_system
                and flow_run.created_by
                and flow_run.created_by.display_value):
            name = f"slurm-{configuration.hpc_system}-{flow_run.created_by.display_value}"
        else:
            raise AttributeError(
                "Cannot determine SLURM connection: set 'connection_name' explicitly, "
                "or set 'hpc_system' and ensure the reverse proxy injects user identity."
            )

    backend = await self._create_backend_from_name(name)
    ...
    slurm_job_id = await backend.submit(...)
    task_status.started(f"{slurm_job_id}@{name}")   # carries resolved name
```

**`infrastructure_pid` format:** `"{slurm_job_id}@{connection_name}"` — e.g.
`12345@slurm-issy-jsmith`.

**`kill_infrastructure()` parses it:**

```python
async def kill_infrastructure(self, infrastructure_pid, configuration, grace_seconds=30):
    if "@" in infrastructure_pid:
        job_id, conn_name = infrastructure_pid.rsplit("@", 1)
    else:
        job_id, conn_name = infrastructure_pid, configuration.connection_name

    backend = await self._create_backend_from_name(conn_name)
    await backend.kill(job_id, grace_seconds=grace_seconds)
```

The `else` branch preserves backward compatibility with existing running jobs that were
submitted before this change (their `infrastructure_pid` is just a bare integer string).

`_create_backend()` is refactored into `_create_backend_from_name(name: str)` that takes
a pre-resolved block name. The resolution logic above replaces what was previously inside
`_create_backend()`.

### `base-job-template.json` — new field

```json
"job_configuration": {
    "hpc_system": "CHANGE_ME",
    "connection_name": null,
    ...
}
```

`connection_name` is removed from the literal section (was `"hsuper-kramerd"`). Admins
set `hpc_system` to the short cluster identifier when creating the pool.

### Block naming convention

Admins create one block per user per HPC system:

```
slurm-{hpc_system}-{ssl_cert_cn}
```

Examples: `slurm-issy-jsmith`, `slurm-hawk-mweber`. All use the existing
`SlurmSSHConnection` block type. No new block types are needed.

### Backward compatibility

Setting `connection_name` explicitly in the job template continues to work exactly as
before. The `hpc_system` path is only taken when `connection_name` is absent.

---

## Component 3: patch + build script

### `scripts/patch-prefect-server.sh`

Self-contained bash script. Usage:

```bash
./scripts/patch-prefect-server.sh \
    --version 2.20.25 \
    --registry registry.example.org/iscc \
    --image-name prefect-server \
    [--push]
```

Steps:

1. Download the Prefect release tarball for `--version` from PyPI.
2. Extract and apply `scripts/prefect-server-x-remote-user.patch` with `patch -p1`.
3. Write a temporary `Dockerfile` that installs the patched source over the stock
   `prefecthq/prefect:{version}` base image.
4. Build and tag as `{registry}/{image-name}:{version}-patched` and `latest-patched`.
5. Push if `--push` is passed.

### `scripts/prefect-server-x-remote-user.patch`

The canonical unified diff of the `dependencies.py` change, version-controlled in this
repo. The build script is a thin wrapper around it — the patch file is the source of
truth for the server change.

### Generated `Dockerfile` (inline, not committed)

```dockerfile
FROM prefecthq/prefect:{VERSION}

# Install patched Prefect over the stock install
COPY dependencies.py /tmp/patched_dependencies.py
RUN SITE=$(python -c "import prefect; import os; print(os.path.dirname(prefect.__file__))") && \
    cp /tmp/patched_dependencies.py $SITE/server/api/dependencies.py

# Runtime-configurable via environment variables
ENV PREFECT_SERVER_USER_HEADER=x-remote-user
ENV PREFECT_SERVER_USER_HEADER_REGEX=
```

---

## One-time admin setup checklist

1. Create `SlurmSSHConnection` blocks for each user: `slurm-{hpc_system}-{cert_cn}`.
2. Create the work pool with the customised `base-job-template.json` (`hpc_system` set).
3. Configure the reverse proxy to inject the user header (see nginx/Traefik config above).
4. Build and deploy the patched Prefect server image using `patch-prefect-server.sh`.
5. Set `PREFECT_SERVER_USER_HEADER` (and optionally `PREFECT_SERVER_USER_HEADER_REGEX`)
   in the server container's environment.

## One-time per-user setup checklist

1. Ensure SSL client certificate CN matches the HPC account username.
2. Ask admin to create the corresponding `SlurmSSHConnection` block.
3. Submit flows as normal — no per-submission action required.
