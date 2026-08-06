"""
env_registry.py — EnvRegistry: reads a single CM instance from environment variables.

Environment variables:
  CM_HOST          — required
  CM_PORT          — default 7183
  CM_USERNAME      — default "admin"
  CM_PASSWORD      — required
  CM_ENVIRONMENT   — default "default"
  CM_USE_TLS       — default "false"
  CM_VERIFY_SSL    — default "true"
  CM_API_VERSION   — default "v51"
  CM_TIMEOUT_SECONDS             — default 30
  CM_DOWNSTREAM_TIMEOUT_SECONDS  — default 30
  CM_DISABLE_ON_SPNEGO           — default "true"
  CM_KERBEROS                    — default "false" (use SPNEGO for downstream)
  # Outbound proxy: set ALL_PROXY / HTTPS_PROXY env vars (httpx trust_env).
"""
from __future__ import annotations

import os

import structlog

from cdp_mcp.config import ClouderaManagerSettings
from cdp_mcp.registry.base import BaseRegistry

log = structlog.get_logger(__name__)


def _bool(val: str) -> bool:
    return val.strip().lower() in ("true", "1", "yes")


class EnvRegistry(BaseRegistry):
    """
    Read-only registry that builds a single ClouderaManagerSettings from env vars.
    Useful for quick single-CM testing without a YAML file.
    """

    def __init__(self) -> None:
        self._instance: list[ClouderaManagerSettings] = []

    def start(self) -> None:
        log.info("env_registry.start")
        self.load()

    def stop(self) -> None:
        log.info("env_registry.stop")

    def load(self) -> list[ClouderaManagerSettings]:
        host = os.environ.get("CM_HOST", "")
        if not host:
            log.warning("env_registry.CM_HOST_not_set")
            self._instance = []
            return []

        settings = ClouderaManagerSettings(
            host=host,
            port=int(os.environ.get("CM_PORT", "7183")),
            username=os.environ.get("CM_USERNAME", "admin"),
            password=os.environ.get("CM_PASSWORD", ""),
            environment_name=os.environ.get("CM_ENVIRONMENT", "default"),
            use_tls=_bool(os.environ.get("CM_USE_TLS", "false")),
            verify_ssl=_bool(os.environ.get("CM_VERIFY_SSL", "true")),
            api_version=os.environ.get("CM_API_VERSION", "v51"),
            timeout_seconds=int(os.environ.get("CM_TIMEOUT_SECONDS", "30")),
            downstream_timeout_seconds=int(
                os.environ.get("CM_DOWNSTREAM_TIMEOUT_SECONDS", "30")
            ),
            disable_on_spnego=_bool(os.environ.get("CM_DISABLE_ON_SPNEGO", "true")),
            # SPNEGO for downstream service endpoints only (NameNode/YARN/Spark/
            # Oozie). cm_client.py is unaffected — it always uses Basic auth.
            kerberos=_bool(os.environ.get("CM_KERBEROS", "false")),
        )
        self._instance = [settings]
        log.info("env_registry.loaded", host=host)
        return self._instance

    def get_all(self) -> list[ClouderaManagerSettings]:
        return list(self._instance)

    def list_raw(self, include_inactive: bool = False) -> list[dict]:
        if not self._instance:
            return []
        s = self._instance[0]
        return [
            {
                "host": s.host,
                "port": s.port,
                "username": s.username,
                "environment_name": s.environment_name,
                "use_tls": s.use_tls,
                "verify_ssl": s.verify_ssl,
                "api_version": s.api_version,
                "active": True,
                "backend": "env",
            }
        ]

    def get_stats(self) -> dict:
        return {
            "backend": "env",
            "total": len(self._instance),
            "active": len(self._instance),
            "inactive": 0,
            "by_environment": (
                {self._instance[0].environment_name: 1} if self._instance else {}
            ),
        }

    def register(self, **kwargs) -> str:
        raise NotImplementedError(
            "EnvRegistry is read-only. Use FileRegistry or IcebergRegistry for mutations."
        )

    def deactivate(self, host: str) -> None:
        raise NotImplementedError(
            "EnvRegistry is read-only. Use FileRegistry or IcebergRegistry for mutations."
        )

    def update_field(self, host: str, field: str, value: str) -> None:
        raise NotImplementedError(
            "EnvRegistry is read-only. Use FileRegistry or IcebergRegistry for mutations."
        )
