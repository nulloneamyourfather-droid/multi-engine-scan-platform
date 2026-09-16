"""Scanner adapter registry.

New engines register here by class; the coordinator exposes their metadata
and agents instantiate them by name. This is the extension point for real
engines (ClamAV, YARA, VirusTotal API, custom binaries, ...).
"""
from __future__ import annotations

from typing import Any

from adapters.base import ScannerAdapter
from adapters.mock_scanner_a import MockScannerA
from adapters.mock_scanner_b import MockScannerB

ADAPTER_CLASSES: dict[str, type[ScannerAdapter]] = {
    MockScannerA.name: MockScannerA,
    MockScannerB.name: MockScannerB,
}


def create_adapter(name: str, config: dict[str, Any] | None = None) -> ScannerAdapter:
    if name not in ADAPTER_CLASSES:
        raise KeyError(f"unknown adapter: {name!r}; available={sorted(ADAPTER_CLASSES)}")
    return ADAPTER_CLASSES[name](config or {})


def list_adapters() -> list[str]:
    return sorted(ADAPTER_CLASSES)
