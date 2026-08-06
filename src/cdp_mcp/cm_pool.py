"""
cm_pool.py — Multi-CM connection pool with auto-discovery of service endpoints.
(Based on dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

from dataclasses import dataclass, field

import structlog

from cdp_mcp.cm_client import ClouderaManagerClient, CMClientError
from cdp_mcp.config import ClouderaManagerSettings, ServerSettings

log = structlog.get_logger(__name__)


# ── ServiceEndpoints ──────────────────────────────────────────────────────────

@dataclass
class ServiceEndpoints:
    """Discovered application service endpoints for a single CDP cluster."""
    yarn_rm_url: str | None = None
    spark_hs_url: str | None = None
    hdfs_nn_url: str | None = None
    oozie_url: str | None = None
    downstream_timeout_seconds: int = 30
    disable_on_spnego: bool = True
    # When True, the downstream clients attach SPNEGO auth (from the CM
    # instance's kerberos flag) for Kerberized clusters. cm_client.py is
    # unaffected (Basic auth).
    kerberos: bool = False
    spnego_required: set[str] = field(default_factory=set)


# ── CMPool ────────────────────────────────────────────────────────────────────

class CMPool:
    """
    Manages a pool of ClouderaManagerClient instances, one per CM settings entry.

    On startup it:
    1. Connects all clients.
    2. Calls list_clusters() on each CM to build a cluster → client mapping.
    3. Discovers YARN / Spark / HDFS / Oozie endpoints for each cluster.
    """

    def __init__(
        self,
        instances: list[ClouderaManagerSettings],
        server_cfg: ServerSettings,
    ) -> None:
        self._settings = instances
        self._server_cfg = server_cfg
        # environment_name → ClouderaManagerClient
        self._clients: dict[str, ClouderaManagerClient] = {}
        # cluster_name (lower) → ClouderaManagerClient
        self._cluster_map: dict[str, ClouderaManagerClient] = {}
        # cluster_name (lower) → ServiceEndpoints
        self._endpoints: dict[str, ServiceEndpoints] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        log.info("cm_pool.start", num_instances=len(self._settings))
        for cfg in self._settings:
            client = ClouderaManagerClient(cfg, self._server_cfg)
            await client.connect()
            self._clients[cfg.environment_name] = client
            log.info(
                "cm_pool.client_connected",
                env=cfg.environment_name,
                host=cfg.effective_host,
            )

        await self._build_cluster_map()

    async def stop(self) -> None:
        log.info("cm_pool.stop", num_clients=len(self._clients))
        for client in self._clients.values():
            try:
                await client.close()
            except Exception as exc:
                log.warning("cm_pool.close_error", error=str(exc))
        self._clients.clear()
        self._cluster_map.clear()
        self._endpoints.clear()

    # ── Cluster map / discovery ───────────────────────────────────────────────

    async def _build_cluster_map(self) -> None:
        self._cluster_map.clear()
        for env_name, client in self._clients.items():
            try:
                clusters = await client.list_clusters()
                for cluster in clusters:
                    cluster_name: str = cluster.get("name", "")
                    if cluster_name:
                        key = cluster_name.lower()
                        self._cluster_map[key] = client
                        log.info(
                            "cm_pool.cluster_mapped",
                            cluster=cluster_name,
                            env=env_name,
                        )
                        await self._discover_service_endpoints(cluster_name, client)
            except CMClientError as exc:
                log.error(
                    "cm_pool.list_clusters_failed",
                    env=env_name,
                    error=str(exc),
                )

    async def _discover_service_endpoints(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
    ) -> None:
        """
        Discover YARN / Spark / HDFS / Oozie endpoints by inspecting roles
        returned by CM. Sets self._endpoints[cluster_name.lower()].

        If CM does not expose a service, skip silently.
        If any unexpected error occurs, log a warning and continue.
        """
        key = cluster_name.lower()
        eps = ServiceEndpoints(
            downstream_timeout_seconds=client.cfg.downstream_timeout_seconds,
            disable_on_spnego=client.cfg.disable_on_spnego,
            kerberos=client.cfg.kerberos,
        )

        # Check for endpoints_override first
        override = client.cfg.endpoints_override or {}
        if override.get("yarn_rm"):
            eps.yarn_rm_url = override["yarn_rm"]
        if override.get("spark_hs"):
            eps.spark_hs_url = override["spark_hs"]
        if override.get("hdfs_nn"):
            eps.hdfs_nn_url = override["hdfs_nn"]
        if override.get("oozie"):
            eps.oozie_url = override["oozie"]

        try:
            services = await client.list_services(cluster_name)
        except CMClientError as exc:
            log.warning(
                "cm_pool.discover_list_services_failed",
                cluster=cluster_name,
                error=str(exc),
            )
            self._endpoints[key] = eps
            return

        service_map: dict[str, str] = {
            svc.get("type", "").upper(): svc.get("name", "")
            for svc in services
        }
        log.debug(
            "cm_pool.discovered_service_types",
            cluster=cluster_name,
            types=list(service_map.keys()),
        )

        # ── YARN ──────────────────────────────────────────────────────────────
        if not eps.yarn_rm_url:
            await self._discover_yarn(cluster_name, client, service_map, eps)

        # ── Spark History Server ───────────────────────────────────────────────
        if not eps.spark_hs_url:
            await self._discover_spark(cluster_name, client, service_map, eps)

        # ── HDFS NameNode ─────────────────────────────────────────────────────
        if not eps.hdfs_nn_url:
            await self._discover_hdfs(cluster_name, client, service_map, eps)

        # ── Oozie ─────────────────────────────────────────────────────────────
        if not eps.oozie_url:
            await self._discover_oozie(cluster_name, client, service_map, eps)

        self._endpoints[key] = eps
        log.info(
            "cm_pool.endpoints_discovered",
            cluster=cluster_name,
            yarn=eps.yarn_rm_url,
            spark=eps.spark_hs_url,
            hdfs=eps.hdfs_nn_url,
            oozie=eps.oozie_url,
        )

    async def _discover_yarn(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
        service_map: dict[str, str],
        eps: ServiceEndpoints,
    ) -> None:
        service_name = service_map.get("YARN")
        if not service_name:
            log.debug("cm_pool.yarn_not_found", cluster=cluster_name)
            return
        try:
            roles_data = await client._get(
                f"/clusters/{cluster_name}/services/{service_name}/roles"
            )
            for role in roles_data.get("items", []):
                if role.get("type") == "RESOURCEMANAGER":
                    hostname = role.get("hostRef", {}).get("hostname", "")
                    if hostname:
                        role_name = role.get("name", "")
                        https_port = await self._get_role_config_value(
                            cluster_name, client, service_name, role_name,
                            "resourcemanager_webserver_https_port",
                        )
                        if https_port:
                            eps.yarn_rm_url = f"https://{hostname}:{https_port}"
                        else:
                            # Try to get the configured port; fall back to 8088
                            port = await self._get_role_port(
                                cluster_name, client, service_name,
                                role_name, "yarn.resourcemanager.webapp.address",
                                default_port=8088,
                            )
                            eps.yarn_rm_url = f"http://{hostname}:{port}"
                        log.debug(
                            "cm_pool.yarn_rm_discovered",
                            cluster=cluster_name,
                            url=eps.yarn_rm_url,
                        )
                        break
        except Exception as exc:
            log.warning(
                "cm_pool.yarn_discovery_error",
                cluster=cluster_name,
                error=str(exc),
            )

    async def _discover_spark(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
        service_map: dict[str, str],
        eps: ServiceEndpoints,
    ) -> None:
        service_name = service_map.get("SPARK_ON_YARN") or service_map.get("SPARK")
        if not service_name:
            log.debug("cm_pool.spark_not_found", cluster=cluster_name)
            return
        try:
            roles_data = await client._get(
                f"/clusters/{cluster_name}/services/{service_name}/roles"
            )
            for role in roles_data.get("items", []):
                if role.get("type") == "SPARK_YARN_HISTORY_SERVER":
                    hostname = role.get("hostRef", {}).get("hostname", "")
                    if hostname:
                        role_name = role.get("name", "")
                        https_port = await self._get_role_config_value(
                            cluster_name, client, service_name, role_name,
                            "ssl_server_port",
                        )
                        if https_port:
                            eps.spark_hs_url = f"https://{hostname}:{https_port}"
                        else:
                            port = await self._get_role_port(
                                cluster_name, client, service_name,
                                role_name,
                                "history.port",
                                default_port=18088,
                            )
                            eps.spark_hs_url = f"http://{hostname}:{port}"
                        log.debug(
                            "cm_pool.spark_hs_discovered",
                            cluster=cluster_name,
                            url=eps.spark_hs_url,
                        )
                        break
        except Exception as exc:
            log.warning(
                "cm_pool.spark_discovery_error",
                cluster=cluster_name,
                error=str(exc),
            )

    async def _discover_hdfs(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
        service_map: dict[str, str],
        eps: ServiceEndpoints,
    ) -> None:
        service_name = service_map.get("HDFS")
        if not service_name:
            log.debug("cm_pool.hdfs_not_found", cluster=cluster_name)
            return
        try:
            roles_data = await client._get(
                f"/clusters/{cluster_name}/services/{service_name}/roles"
            )
            for role in roles_data.get("items", []):
                if role.get("type") == "NAMENODE":
                    # Skip bad/standby namenodes if possible
                    if role.get("healthSummary", "") == "BAD":
                        continue
                    hostname = role.get("hostRef", {}).get("hostname", "")
                    if hostname:
                        role_name = role.get("name", "")
                        https_port = await self._get_role_config_value(
                            cluster_name, client, service_name, role_name,
                            "dfs_https_port",
                        )
                        if https_port:
                            eps.hdfs_nn_url = f"https://{hostname}:{https_port}"
                        else:
                            port = await self._get_role_port(
                                cluster_name, client, service_name,
                                role_name,
                                "dfs.namenode.http-address",
                                default_port=9870,
                            )
                            eps.hdfs_nn_url = f"http://{hostname}:{port}"
                        log.debug(
                            "cm_pool.hdfs_nn_discovered",
                            cluster=cluster_name,
                            url=eps.hdfs_nn_url,
                        )
                        break
        except Exception as exc:
            log.warning(
                "cm_pool.hdfs_discovery_error",
                cluster=cluster_name,
                error=str(exc),
            )

    async def _discover_oozie(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
        service_map: dict[str, str],
        eps: ServiceEndpoints,
    ) -> None:
        service_name = service_map.get("OOZIE")
        if not service_name:
            log.debug("cm_pool.oozie_not_found", cluster=cluster_name)
            return
        try:
            roles_data = await client._get(
                f"/clusters/{cluster_name}/services/{service_name}/roles"
            )
            for role in roles_data.get("items", []):
                if role.get("type") == "OOZIE_SERVER":
                    hostname = role.get("hostRef", {}).get("hostname", "")
                    if hostname:
                        role_name = role.get("name", "")
                        https_port = await self._get_role_config_value(
                            cluster_name, client, service_name, role_name,
                            "oozie_https_port",
                        )
                        if https_port:
                            eps.oozie_url = f"https://{hostname}:{https_port}"
                        else:
                            port = await self._get_role_port(
                                cluster_name, client, service_name,
                                role_name,
                                "oozie_http_port",
                                default_port=11000,
                            )
                            eps.oozie_url = f"http://{hostname}:{port}"
                        log.debug(
                            "cm_pool.oozie_discovered",
                            cluster=cluster_name,
                            url=eps.oozie_url,
                        )
                        break
        except Exception as exc:
            log.warning(
                "cm_pool.oozie_discovery_error",
                cluster=cluster_name,
                error=str(exc),
            )

    async def _get_role_port(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
        service_name: str,
        role_name: str,
        config_key: str,
        default_port: int,
    ) -> int:
        """Attempt to read a specific config key for a role; fall back to default_port."""
        if not role_name:
            return default_port
        try:
            data = await client._get(
                f"/clusters/{cluster_name}/services/{service_name}"
                f"/roles/{role_name}/config",
                params={"view": "full"},
            )
            for item in data.get("items", []):
                if item.get("name") == config_key:
                    raw_val = item.get("value") or item.get("default", "")
                    # value may be "hostname:port" or just a port number
                    if ":" in str(raw_val):
                        raw_val = str(raw_val).split(":")[-1]
                    if raw_val:
                        return int(raw_val)
        except Exception:
            pass
        return default_port

    async def _get_role_config_value(
        self,
        cluster_name: str,
        client: ClouderaManagerClient,
        service_name: str,
        role_name: str,
        config_key: str,
    ) -> str | None:
        """
        Return a role config value if present, else None -- distinct from
        _get_role_port's default-port fallback, so callers can tell "key not
        configured" apart from "key configured with this value".
        """
        if not role_name:
            return None
        try:
            data = await client._get(
                f"/clusters/{cluster_name}/services/{service_name}"
                f"/roles/{role_name}/config",
                params={"view": "full"},
            )
            for item in data.get("items", []):
                if item.get("name") == config_key:
                    raw_val = item.get("value") or item.get("default")
                    if raw_val not in (None, ""):
                        return str(raw_val)
        except Exception:
            pass
        return None

    # ── Public API ────────────────────────────────────────────────────────────

    def get_client_for_cluster(self, cluster_name: str) -> ClouderaManagerClient | None:
        """Return the CM client responsible for cluster_name, or None."""
        return self._cluster_map.get(cluster_name.lower())

    def get_endpoints(self, cluster_name: str) -> ServiceEndpoints:
        """Return discovered service endpoints for cluster_name."""
        return self._endpoints.get(cluster_name.lower(), ServiceEndpoints())

    def mark_spnego_required(self, cluster_name: str, service: str) -> None:
        """Record that a downstream service requires SPNEGO, so future calls
        can short-circuit instead of repeating a doomed request."""
        key = cluster_name.lower()
        if key in self._endpoints:
            self._endpoints[key].spnego_required.add(service)

    # ── Downstream client factories ──────────────────────────────────────────
    # Centralised construction point for the four downstream clients. When the
    # CM instance has kerberos=True, each client is built with an HTTPSPNEGOAuth
    # (from spnego.py, using the default Kerberos credentials cache); otherwise
    # auth=None (today's behaviour). Returns None if the endpoint was not
    # discovered. build_spnego_auth may raise SpnegoConfigError (typed) if the
    # optional httpx-gssapi extra is missing or no TGT is available — server.py
    # surfaces that via its generic exception handler.

    def _spnego_auth(self, cluster_name: str):
        from cdp_mcp.clients.spnego import build_spnego_auth
        eps = self.get_endpoints(cluster_name)
        return build_spnego_auth(eps.kerberos)

    def get_yarn_client(self, cluster_name: str):
        from cdp_mcp.clients.yarn_client import YarnClient
        eps = self.get_endpoints(cluster_name)
        if not eps.yarn_rm_url:
            return None
        return YarnClient(
            eps.yarn_rm_url,
            timeout=eps.downstream_timeout_seconds,
            auth=self._spnego_auth(cluster_name),
        )

    def get_spark_client(self, cluster_name: str):
        from cdp_mcp.clients.spark_client import SparkClient
        eps = self.get_endpoints(cluster_name)
        if not eps.spark_hs_url:
            return None
        return SparkClient(
            eps.spark_hs_url,
            timeout=eps.downstream_timeout_seconds,
            auth=self._spnego_auth(cluster_name),
        )

    def get_hdfs_client(self, cluster_name: str):
        from cdp_mcp.clients.hdfs_client import HdfsClient
        eps = self.get_endpoints(cluster_name)
        if not eps.hdfs_nn_url:
            return None
        return HdfsClient(
            eps.hdfs_nn_url,
            timeout=eps.downstream_timeout_seconds,
            auth=self._spnego_auth(cluster_name),
        )

    def get_oozie_client(self, cluster_name: str):
        from cdp_mcp.clients.oozie_client import OozieClient
        eps = self.get_endpoints(cluster_name)
        if not eps.oozie_url:
            return None
        return OozieClient(
            eps.oozie_url,
            timeout=eps.downstream_timeout_seconds,
            auth=self._spnego_auth(cluster_name),
        )

    def list_environments(self) -> list[str]:
        return list(self._clients.keys())

    def list_known_clusters(self) -> list[str]:
        return list(self._cluster_map.keys())

    def get_client_for_environment(
        self, environment_name: str
    ) -> ClouderaManagerClient | None:
        return self._clients.get(environment_name)

    # ── Refresh ───────────────────────────────────────────────────────────────

    async def refresh_cluster_map(self) -> None:
        """Rebuild the cluster → client mapping and rediscover endpoints."""
        log.info("cm_pool.refresh_cluster_map")
        self._endpoints.clear()
        await self._build_cluster_map()

    async def reload(
        self, new_instances: list[ClouderaManagerSettings]
    ) -> None:
        """
        Replace the pool with a new list of CM instances.
        Closes all existing connections, then reconnects.
        """
        log.info("cm_pool.reload", num_instances=len(new_instances))
        await self.stop()
        self._settings = new_instances
        await self.start()
