import uuid
from unittest.mock import AsyncMock, MagicMock, patch  # noqa: F401

import pytest
from prefect.client.schemas import FlowRun
from prefect.server.schemas.core import CreatedBy
from pydantic import SecretStr

from prefect_slurm.slurm import APIBasedSlurmBackend, CLIBasedSlurmBackend  # noqa: F401
from prefect_slurm.worker import (
    SlurmAPIConnection,
    SlurmJobConfiguration,
    SlurmSSHConnection,
    SlurmWorker,
)


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


def test_resolve_raises_when_created_by_is_schedule_type(worker):
    """Scheduler-created runs (type=SCHEDULE) must not be used as user identity."""
    cfg = make_config(connection_name=None, hpc_system="issy")
    flow_run = FlowRun.construct(
        id=uuid.uuid4(),
        flow_id=uuid.uuid4(),
        name="test-run",
        created_by=CreatedBy(type="SCHEDULE", display_value="CronSchedule"),
        tags=[],
        labels={},
        parameters={},
        state=None,
    )
    with pytest.raises(AttributeError, match="Cannot determine SLURM connection"):
        worker._resolve_connection_name(cfg, flow_run)


# ---------------------------------------------------------------------------
# Task 4: _parse_infrastructure_pid
# ---------------------------------------------------------------------------


def test_parse_pid_new_format(worker):
    job_id, conn = worker._parse_infrastructure_pid("12345@slurm-issy-jsmith")
    assert job_id == "12345"
    assert conn == "slurm-issy-jsmith"


def test_parse_pid_old_format_uses_fallback(worker):
    job_id, conn = worker._parse_infrastructure_pid(
        "12345", fallback_connection="slurm-old"
    )
    assert job_id == "12345"
    assert conn == "slurm-old"


def test_parse_pid_old_format_no_fallback_raises(worker):
    with pytest.raises(AttributeError, match="Cannot determine SLURM connection"):
        worker._parse_infrastructure_pid("12345")


def test_parse_pid_connection_name_with_at_symbol(worker):
    """split on first @ keeps any @ in the connection name intact."""
    job_id, conn = worker._parse_infrastructure_pid("99@slurm-hawk-user@domain")
    assert job_id == "99"
    assert conn == "slurm-hawk-user@domain"


# ---------------------------------------------------------------------------
# Task 5: _create_backend_from_name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_backend_from_name_returns_api_backend(worker):
    with patch.object(
        SlurmAPIConnection, "load", new=AsyncMock(return_value=mock_api_block())
    ):
        backend = await worker._create_backend_from_name("slurm-issy-jsmith")
    assert isinstance(backend, APIBasedSlurmBackend)


@pytest.mark.asyncio
async def test_create_backend_from_name_falls_back_to_ssh(worker):
    with patch.object(
        SlurmAPIConnection, "load", new=AsyncMock(side_effect=ValueError)
    ), patch.object(
        SlurmSSHConnection, "load", new=AsyncMock(return_value=mock_ssh_block())
    ):
        backend = await worker._create_backend_from_name("slurm-issy-jsmith")
    assert isinstance(backend, CLIBasedSlurmBackend)


@pytest.mark.asyncio
async def test_create_backend_from_name_raises_when_neither_block_found(worker):
    with patch.object(
        SlurmAPIConnection, "load", new=AsyncMock(side_effect=ValueError)
    ), patch.object(SlurmSSHConnection, "load", new=AsyncMock(side_effect=ValueError)):
        with pytest.raises(AttributeError, match="No valid connection"):
            await worker._create_backend_from_name("slurm-issy-jsmith")
