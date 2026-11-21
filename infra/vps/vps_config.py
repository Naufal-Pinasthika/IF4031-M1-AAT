"""Non-secret defaults shared by operator-side VPS tooling.

Application configuration still belongs to .env and Kubernetes manifests.
Keep these defaults stable: changing them is a deployment change, not cleanup.
No inventory, credentials, or network connections are loaded on import.
"""
from typing import NamedTuple


NAMESPACE = 'sistem-informasi-kebencanaan-bnpb'


class ServiceEndpoint(NamedTuple):
    name: str
    port: int
    node_port: int
    backend: str


# Preserve the existing manifest order and selectors. The common Aggregator
# entry routes through the gateway, not directly to one Aggregator pod.
SERVICE_ENDPOINTS = (
    ServiceEndpoint('aggregator', 8083, 30083, 'aggregator-gateway'),
    ServiceEndpoint('client-api', 8080, 30080, 'client-api'),
    ServiceEndpoint('auth', 8084, 30084, 'auth'),
    ServiceEndpoint('sensor-pantai', 8080, 30090, 'sensor-pantai'),
    ServiceEndpoint('notifier-consumer', 8086, 30086, 'notifier-consumer'),
)
TUNNEL_PORTS = tuple(sorted(endpoint.node_port for endpoint in SERVICE_ENDPOINTS))
