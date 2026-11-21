#!/usr/bin/env python3
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "sistem-informasi-kebencanaan-bnpb"
SERVICES = ("aggregator", "auth", "bmkg", "client-api", "dashboard-consumer",
            "notifier-consumer", "pvmbg")
SECRET_KEYS = (
    "BMKG_API_KEY", "PVMBG_TOKEN", "AGGREGATOR_API_KEY", "POSTGRES_ADMIN_PASSWORD",
    "POSTGRES_APP_PASSWORD", "AUTH_POSTGRES_PASSWORD", "AUTH_JWT_PRIVATE_KEY_B64",
    "MEDIA_CLIENT_SECRET", "FIELD_CLIENT_SECRET", "OPS_CLIENT_SECRET",
    "PANTAI_AUTH_TOKEN", "COORDINATOR_POSTGRES_PASSWORD",
    # p1
    "P1_AUDIT_TOKEN",
)
CONFIG_KEYS = {
    "bmkg": ("BMKG_EVENT_INTERVAL_MS",),
    "pvmbg": ("PVMBG_EVENT_INTERVAL_MS", "PVMBG_DELAY_MIN_MS", "PVMBG_DELAY_MAX_MS"),
    "aggregator": ("KAFKA_TOPIC", "AGGREGATOR_POLL_INTERVAL_MS", "AGGREGATOR_HTTP_TIMEOUT_MS",
                   "AGGREGATOR_MAX_IN_FLIGHT", "AGGREGATOR_RESPONSE_SLOTS", "AGGREGATOR_SHARED_READ_PROJECTION", "AGGREGATOR_FIELD_RESERVED_SLOTS", "AGGREGATOR_READ_CACHE_TTL_MS", "AGGREGATOR_START_SINCE", "AGGREGATOR_LEASE_TTL_MS",
                   "COORDINATION_TIMEOUT_MS", "COASTAL_POLL_INTERVAL_MS", "COASTAL_HTTP_TIMEOUT_MS",
                   "COASTAL_MAX_STALENESS_MS", "COASTAL_CIRCUIT_FAILURES", "COASTAL_CIRCUIT_OPEN_MS",
                   # p1
                   "P1_STORE_TIMEOUT_MS", "P1_IMPORT_ONLY", "SATELLITE_URL", "PVMBG_ADAPTER_VERSION"),
    "auth": ("AUTH_ACCESS_TOKEN_TTL_SECONDS", "AUTH_REFRESH_TOKEN_TTL_SECONDS", "AUTH_MAX_IN_FLIGHT",
             "AUTH_JWT_ISSUER", "AUTH_JWT_AUDIENCE", "AUTH_JWT_KEY_ID", "MEDIA_CLIENT_ID",
             "FIELD_CLIENT_ID", "OPS_CLIENT_ID"),
    "client-api": ("AUTH_JWT_ISSUER", "AUTH_JWT_AUDIENCE", "CLIENT_API_MAX_IN_FLIGHT",
                   "CLIENT_API_FIELD_RESERVED_SLOTS", "FIELD_CLIENT_ID"),
    "dashboard-consumer": ("KAFKA_TOPIC", "DASHBOARD_CONSUMER_GROUP"),
    "notifier-consumer": ("KAFKA_TOPIC", "NOTIFIER_CONSUMER_GROUP"),
    "sensor-pantai": ("PANTAI_PROFILE", "PANTAI_SEED", "PANTAI_SPEED"),
}


def read_env(path):
    result = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"invalid dotenv assignment at line {number}")
        value = value.strip()
        if value[:1] in ("'", '"'):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"unclosed quote for {key} at line {number}")
            value = value[1:-1]
        result[key] = value
    return result


def resource(kind, name, data):
    return {
        "apiVersion": "v1", "kind": kind,
        "metadata": {"name": name, "namespace": NAMESPACE},
        "stringData" if kind == "Secret" else "data": data,
        **({"type": "Opaque"} if kind == "Secret" else {}),
    }


def runtime_resources(values):
    for key in SECRET_KEYS:
        if not values.get(key) or "replace-with-" in values[key]:
            raise ValueError(f"{key} must have a non-placeholder value; run scripts/setup-env.sh")
    credentials = [values[key] for key in SECRET_KEYS if key != "AUTH_JWT_PRIVATE_KEY_B64"]
    if len(set(credentials)) != len(credentials):
        raise ValueError("source, database, and client credentials must be distinct")
    topic = values["KAFKA_TOPIC"]
    if topic in (".", "..") or not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic):
        raise ValueError("KAFKA_TOPIC must be a valid Kafka topic name")

    configs = {name: {key: values[key] for key in keys} for name, keys in CONFIG_KEYS.items()}
    configs["aggregator"].update({"BMKG_URL": "http://bmkg:8081", "PVMBG_URL": "http://pvmbg:8082",
                                  "COASTAL_URL": "http://sensor-pantai:8080",
                                  "KAFKA_BROKERS": "kafka:9092", "AGGREGATOR_LISTEN_ADDRESS": ":8083"})
    configs["auth"]["AUTH_LISTEN_ADDRESS"] = ":8084"
    configs["client-api"].update({"AGGREGATOR_URL": "http://aggregator:8083",
                                  "AUTH_JWKS_URL": "http://auth:8084/.well-known/jwks.json"})
    for name, prefix, port, filename in (
        ("dashboard-consumer", "DASHBOARD", 8085, "dashboard.db"),
        ("notifier-consumer", "NOTIFIER", 8086, "notifier.db"),
    ):
        configs[name].update({"KAFKA_BROKERS": "kafka:9092", f"{prefix}_LISTEN_ADDRESS": f":{port}",
                             f"{prefix}_STORE_PATH": f"/data/{filename}", "GOMEMLIMIT": "96MiB"})
    configs["postgres"] = {"POSTGRES_DB": "aggregator", "POSTGRES_USER": "postgres",
                           "POSTGRES_APP_USER": values["POSTGRES_APP_USER"],
                           "PGDATA": "/var/lib/postgresql/data/pgdata"}
    configs["auth-postgres"] = {"POSTGRES_DB": "auth", "POSTGRES_USER": "auth_app",
                                "PGDATA": "/var/lib/postgresql/data/pgdata"}
    configs["aggregator-coordinator"] = {"POSTGRES_DB": "coordination", "POSTGRES_USER": "aggregator_coord",
                                          "PGDATA": "/var/lib/postgresql/data/pgdata"}
    configs["kafka-init"] = {"KAFKA_TOPIC": topic}

    secrets = {
        "bmkg": {"BMKG_API_KEY": values["BMKG_API_KEY"]},
        "pvmbg": {"PVMBG_TOKEN": values["PVMBG_TOKEN"]},
        "postgres": {"POSTGRES_PASSWORD": values["POSTGRES_ADMIN_PASSWORD"],
                     "POSTGRES_APP_PASSWORD": values["POSTGRES_APP_PASSWORD"]},
        "auth-postgres": {"POSTGRES_PASSWORD": values["AUTH_POSTGRES_PASSWORD"]},
        "aggregator": {key: values[key] for key in ("BMKG_API_KEY", "PVMBG_TOKEN", "PANTAI_AUTH_TOKEN", "AGGREGATOR_API_KEY", "P1_AUDIT_TOKEN")},
        "auth": {key: values[key] for key in ("AUTH_JWT_PRIVATE_KEY_B64", "MEDIA_CLIENT_SECRET",
                                              "FIELD_CLIENT_SECRET", "OPS_CLIENT_SECRET")},
        "client-api": {"AGGREGATOR_API_KEY": values["AGGREGATOR_API_KEY"]},
        "aggregator-coordinator": {"POSTGRES_PASSWORD": values["COORDINATOR_POSTGRES_PASSWORD"]},
    }
    satellite_token = values.get("SATELLITE_TOKEN", "")
    if "replace-with-" in satellite_token:
        raise ValueError("set SATELLITE_TOKEN to a real feed token or leave it empty to disable the feed")
    secrets["aggregator"]["SATELLITE_TOKEN"] = satellite_token
    # p1: stable Services replace pod IPs and the legacy single database URL.
    for letter in "ABC":
        secrets["aggregator"][f"P1_STORAGE_{letter}_URL"] = (
            f"postgres://{quote(values['POSTGRES_APP_USER'], safe='')}:{quote(values['POSTGRES_APP_PASSWORD'], safe='')}"
            f"@postgres-{letter.lower()}:5432/aggregator?sslmode=disable"
        )
    secrets["aggregator"]["COORDINATION_DATABASE_URL"] = (
        f"postgres://aggregator_coord:{quote(values['COORDINATOR_POSTGRES_PASSWORD'], safe='')}"
        "@aggregator-coordinator:5432/coordination?sslmode=disable"
    )
    secrets["auth"]["AUTH_DATABASE_URL"] = (
        f"postgres://auth_app:{quote(values['AUTH_POSTGRES_PASSWORD'], safe='')}"
        "@auth-postgres:5432/auth?sslmode=disable"
    )
    result = [resource("ConfigMap", f"{name}-config", data) for name, data in configs.items()]
    result += [resource("Secret", f"{name}-credentials", data) for name, data in secrets.items()]
    result.append(resource("Secret", "pantai-token", {"token": values["PANTAI_AUTH_TOKEN"]}))
    result.append(resource("ConfigMap", "postgres-init", {
        "01-create-aggregator-user.sh": (ROOT / "services/database/init/01-create-aggregator-user.sh").read_text(),
    }))
    # p1: bootstrap is configuration, with node identity supplied at runtime.
    result.append(resource("ConfigMap", "p1-storage-scripts", {
        "p1-storage-start.sh": (ROOT / "services/database/p1-storage-start.sh").read_text(),
    }))
    from observability_resources import resources as observability_resources
    result += observability_resources(values.get('_IMAGE_PREFIX','aat'), values.get('_IMAGE_TAG','m2-dev'))
    return result


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # os.open also protects an existing output file before writing credentials.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--image-prefix", default="aat")
    parser.add_argument("--image-tag", default="m2-dev")
    parser.add_argument("--storage-class")
    parser.add_argument("--existing-secrets", type=Path)
    parser.add_argument("--existing-postgres-config", type=Path)
    args = parser.parse_args()
    try:
        values = read_env(ROOT / ".env.example")
        values.update(read_env(args.env_file))
        values.update(_IMAGE_PREFIX=args.image_prefix.rstrip('/'), _IMAGE_TAG=args.image_tag)
        runtime = runtime_resources(values)
        if args.existing_secrets:
            raw = args.existing_secrets.read_text().strip()
            existing = json.loads(raw) if raw else {"items": []}
            desired = {obj["metadata"]["name"]: obj for obj in runtime if obj["kind"] == "Secret"}
            for obj in existing.get("items", []):
                name = obj["metadata"]["name"]
                for key, value in desired[name]["stringData"].items():
                    encoded = obj.get("data", {}).get(key, "")
                    if base64.b64decode(encoded).decode() != value:
                        raise ValueError("database credentials differ from the deployed Secret; "
                                         "keep the original .env or migrate database passwords explicitly")
        if args.existing_postgres_config:
            raw = args.existing_postgres_config.read_text().strip()
            if raw:
                existing = json.loads(raw)
                if existing.get("data", {}).get("POSTGRES_APP_USER") != values["POSTGRES_APP_USER"]:
                    raise ValueError("database username differs from the deployed ConfigMap; migrate it explicitly")
        for stage in ("platform", "jobs", "apps"):
            directory = args.output_dir.resolve() / stage
            overlay = {
                "apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                "namespace": NAMESPACE,
                "resources": [os.path.relpath(ROOT / "k8s/base" / stage, directory)],
            }
            if stage == "platform":
                write_json(directory / "runtime.json", {"apiVersion": "v1", "kind": "List", "items": runtime})
                overlay["resources"].append("runtime.json")
            if stage == "apps":
                overlay["images"] = [{"name": f"aat/{name}", "newName": f"{args.image_prefix.rstrip('/')}/{name}",
                                       "newTag": args.image_tag} for name in SERVICES]
                overlay["patches"] = []
                for name in SERVICES:
                    settings = [obj for obj in runtime if obj["metadata"]["name"] in
                                (f"{name}-config", f"{name}-credentials")]
                    checksum = hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest()
                    overlay["patches"].append({
                        "target": {"kind": "StatefulSet" if "consumer" in name else "Deployment", "name": name},
                        "patch": json.dumps([{"op": "add", "path": "/spec/template/metadata/annotations",
                                              "value": {"bnpb/config-sha256": checksum}}]),
                    })
            if args.storage_class and stage != "jobs":
                overlay.setdefault("patches", []).append({
                    "target": {"kind": "StatefulSet"},
                    "patch": json.dumps([{"op": "add", "path": "/spec/volumeClaimTemplates/0/spec/storageClassName",
                                          "value": args.storage_class}]),
                })
            write_json(directory / "kustomization.yaml", overlay)
    except (ValueError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
