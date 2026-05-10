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
