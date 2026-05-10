import uuid
from unittest.mock import MagicMock

import pytest
from prefect.client.schemas import FlowRun
from prefect.server.schemas.core import CreatedBy
from pydantic import SecretStr

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
