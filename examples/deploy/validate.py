#!/usr/bin/env python3
"""Structural guard for the Factory-owned deployment example (requires PyYAML)."""
from pathlib import Path
import re
import sys

import yaml

HERE = Path(__file__).resolve().parent


def validate(manifest: str, wiring: str) -> list[str]:
    errors = []
    try:
        documents = [d for d in yaml.safe_load_all(manifest) if d]
    except yaml.YAMLError as exc:
        return [f"invalid YAML: {exc}"]
    deployments = [d for d in documents if d.get("kind") == "Deployment"]
    services = [d for d in documents if d.get("kind") == "Service"]
    if len(deployments) != 1 or len(services) != 1:
        return ["expected one Factory Deployment and one public Service"]
    if any(d.get("kind") == "Secret" for d in documents):
        errors.append("committed Secret resource")
    def inspect_secrets(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if re.search(r"password|secret.?key|access.?key|credential|token|dsn", str(key), re.I) and isinstance(value, (str, int, float)) and str(value):
                    errors.append(f"literal secret field {key}")
                inspect_secrets(value)
        elif isinstance(node, list):
            for value in node:
                inspect_secrets(value)
    for document in documents:
        inspect_secrets(document)
    deployment = deployments[0]
    spec = deployment.get("spec", {})
    pod = spec.get("template", {}).get("spec", {})
    if spec.get("replicas") != 2:
        errors.append("Factory replica count must be two")
    if pod.get("affinity"):
        errors.append("Factory must not require affinity")
    if pod.get("terminationGracePeriodSeconds", 0) < 60:
        errors.append("termination grace below 60 seconds")
    containers = pod.get("containers", [])
    if len(containers) != 1:
        errors.append("expected one Factory container")
    for container in containers:
        resources = container.get("resources", {})
        for kind in ("requests", "limits"):
            if not all(resources.get(kind, {}).get(field) for field in ("cpu", "memory")):
                errors.append(f"missing {kind} CPU or memory bound")
        for env in container.get("env", []):
            name = env.get("name", "")
            if re.search(r"PASSWORD|SECRET|TOKEN|KEY|DSN|CREDENTIAL", name, re.I) and "value" in env:
                errors.append(f"literal credential in env {name}")
        if any("hostlink" in str(port.get("name", "")).lower() for port in container.get("ports", [])):
            errors.append("HostLink port exposed by Factory pod")
    for service in services:
        if service.get("spec", {}).get("type") in ("LoadBalancer", "NodePort"):
            errors.append("public Factory Service must remain ClusterIP template")
        if any("hostlink" in str(port.get("name", "")).lower() for port in service.get("spec", {}).get("ports", [])):
            errors.append("public HostLink exposure")
    if not re.search(r"PerConnectionQueueBytes\s*=\s*1\s*<<\s*20", wiring):
        errors.append("missing finite per-connection byte queue")
    if not re.search(r"MaxConnections\s*=\s*maxConnections", wiring):
        errors.append("missing per-replica admission bound")
    if not re.search(r"MaxConcurrentTransfers\s*:\s*[1-9][0-9]*", wiring):
        errors.append("missing S3 transfer bound")
    if not re.search(r"RequireConfirmedEncryption\s*:\s*true", wiring) or not re.search(r"Encryption\s*:\s*s3store\.EncryptionKMS", wiring):
        errors.append("S3 KMS encryption policy is not explicit and confirmed")
    if not re.search(r"Migrations\s*:\s*pgstore\.MigrationValidate", wiring):
        errors.append("replicas must validate migrations, not apply them")
    if not re.search(r"DeploymentPrefix\s*:\s*cfg\.DeploymentPrefix", wiring):
        errors.append("missing shared S3 deployment prefix")
    return errors


if __name__ == "__main__":
    findings = validate((HERE / "cloud-pooled.yaml").read_text(), (HERE / "wiring" / "wiring.go").read_text())
    if findings:
        print("\n".join(findings), file=sys.stderr)
        sys.exit(1)
    print("Factory deployment example: structural checks passed")
