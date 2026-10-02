"""SMS gateway client package (sending SMS via the gateway REST API, step 5)."""

from .client import (
    GatewayClient,
    GatewayError,
    MockGatewayClient,
    env_mock_enabled,
    make_gateway_client,
)
from .trim import trim_reply, utf16_units

__all__ = [
    "GatewayClient",
    "GatewayError",
    "MockGatewayClient",
    "env_mock_enabled",
    "make_gateway_client",
    "trim_reply",
    "utf16_units",
]
