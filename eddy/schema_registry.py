"""Minimal Confluent Schema Registry REST client plus a local compatibility lint.

Uses plain `requests` so the Spark driver can read schemas without the
confluent-kafka package.
"""

from __future__ import annotations

import json

import requests

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class SchemaRegistryError(RuntimeError):
    pass


class SchemaRegistry:
    def __init__(self, url: str, timeout: int = 10):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["Content-Type"] = CONTENT_TYPE

    def _request(self, method: str, path: str, **kwargs):
        resp = self.session.request(method, f"{self.url}{path}", timeout=self.timeout, **kwargs)
        if resp.status_code >= 400:
            raise SchemaRegistryError(f"{method} {path} -> {resp.status_code}: {resp.text}")
        return resp.json()

    def subjects(self) -> list[str]:
        return self._request("GET", "/subjects")

    def set_compatibility(self, subject: str, level: str) -> dict:
        return self._request("PUT", f"/config/{subject}", data=json.dumps({"compatibility": level}))

    def get_compatibility(self, subject: str) -> str:
        return self._request("GET", f"/config/{subject}?defaultToGlobal=true")["compatibilityLevel"]

    def register(self, subject: str, schema: str) -> int:
        """Register a schema (idempotent: re-registering returns the existing id)."""
        body = self._request("POST", f"/subjects/{subject}/versions", data=json.dumps({"schema": schema}))
        return body["id"]

    def is_compatible(self, subject: str, schema: str) -> tuple[bool, list[str]]:
        """Check a candidate schema against the latest registered version."""
        try:
            body = self._request(
                "POST",
                f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
                data=json.dumps({"schema": schema}),
            )
        except SchemaRegistryError as exc:
            if "40401" in str(exc):  # subject not found: anything is compatible
                return True, []
            raise
        return body.get("is_compatible", False), body.get("messages", [])

    def versions(self, subject: str) -> list[int]:
        return self._request("GET", f"/subjects/{subject}/versions")

    def get_version(self, subject: str, version: int | str = "latest") -> dict:
        return self._request("GET", f"/subjects/{subject}/versions/{version}")

    def writer_schemas(self, subject: str) -> dict[int, str]:
        """Map schema id -> schema string for every registered version of a subject."""
        return {
            v["id"]: v["schema"]
            for v in (self.get_version(subject, n) for n in self.versions(subject))
        }


def _fields(schema: dict) -> dict[str, dict]:
    return {f["name"]: f for f in schema.get("fields", [])}


def _normalise_type(t):
    return json.dumps(t, sort_keys=True)


def backward_compatibility_issues(old: str, new: str) -> list[str]:
    """Offline BACKWARD check: can a consumer on `new` read data written with `old`?

    Catches the common contract breaks before a schema ever reaches the
    registry: new fields without defaults and changed field types. The
    registry's own check remains the source of truth at deploy time.
    """
    old_f, new_f = _fields(json.loads(old)), _fields(json.loads(new))
    issues = []
    for name, field in new_f.items():
        if name not in old_f:
            if "default" not in field:
                issues.append(f"new field '{name}' has no default")
        elif _normalise_type(field["type"]) != _normalise_type(old_f[name]["type"]):
            issues.append(
                f"field '{name}' changed type {old_f[name]['type']!r} -> {field['type']!r}"
            )
    return issues
