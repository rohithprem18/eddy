"""Schema contract gate for CI and pre-deploy checks.

    python -m eddy.compat_check            # offline: lint every consecutive version pair
    python -m eddy.compat_check --registry # also ask the live registry about the newest file

Exits non-zero on any incompatibility so a breaking change cannot merge.
"""

from __future__ import annotations

import argparse
import json
import sys

from fastavro import parse_schema

from eddy.config import get_settings
from eddy.schema_registry import SchemaRegistry, backward_compatibility_issues


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", action="store_true", help="also check against the live registry")
    args = parser.parse_args()

    settings = get_settings()
    files = settings.schema_files()
    failed = False

    for path in files:
        try:
            parse_schema(json.loads(path.read_text()))
            print(f"OK    {path.name} is a valid Avro schema")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL  {path.name} is not valid Avro: {exc}")
            failed = True

    for old, new in zip(files, files[1:], strict=False):
        issues = backward_compatibility_issues(old.read_text(), new.read_text())
        if issues:
            failed = True
            for issue in issues:
                print(f"FAIL  {old.name} -> {new.name}: {issue}")
        else:
            print(f"OK    {old.name} -> {new.name} is backward compatible")

    if args.registry:
        ok, messages = SchemaRegistry(settings.schema_registry_url).is_compatible(
            settings.subject, files[-1].read_text()
        )
        print(f"{'OK  ' if ok else 'FAIL'}  registry check for {files[-1].name}: {messages or 'compatible'}")
        failed |= not ok

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
