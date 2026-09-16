"""Tests for the two mock scanner adapters and the executor."""
from __future__ import annotations

import pytest

from adapters import create_adapter
from adapters.mock_scanner_a import MockScannerA
from adapters.mock_scanner_b import MockScannerB
from agent.executor import Executor
from models.task import ScanTask, ScanVerdict


@pytest.mark.parametrize(
    "sha,expected",
    [
        ("0" * 64, ScanVerdict.MALICIOUS),  # starts with 0 -> signature hit
        ("f" * 64, ScanVerdict.MALICIOUS),
        ("a" * 64, ScanVerdict.BENIGN),
        ("1" * 64, ScanVerdict.BENIGN),
    ],
)
def test_mock_scanner_a_verdict(sha, expected):
    task = ScanTask(artifact_sha256=sha, engine="mock_engine_a")
    result = MockScannerA().scan(task)
    assert result.verdict == expected
    assert result.engine == "mock_engine_a"


def test_mock_scanner_a_deterministic():
    task = ScanTask(artifact_sha256="0" * 64, engine="mock_engine_a")
    r1 = MockScannerA().scan(task)
    r2 = MockScannerA().scan(task)
    assert r1.verdict == r2.verdict
    assert r1.scan_duration_ms >= 0


def test_mock_scanner_a_fail_rate_raises():
    task = ScanTask(artifact_sha256="0" * 64, engine="mock_engine_a")
    with pytest.raises(RuntimeError):
        MockScannerA({"fail_rate": 1.0}).scan(task)


def test_mock_scanner_b_disagrees_on_subset():
    task = ScanTask(artifact_sha256="a" * 64, engine="mock_engine_b")  # base benign
    # With disagree_rate=1.0 every sample flips: benign -> malicious.
    result = MockScannerB({"disagree_rate": 1.0}).scan(task)
    assert result.verdict == ScanVerdict.MALICIOUS
    assert result.details["disagreed"] is True


def test_registry_lists_and_creates_adapters():
    from adapters import list_adapters

    names = list_adapters()
    assert "mock_engine_a" in names and "mock_engine_b" in names
    with pytest.raises(KeyError):
        create_adapter("nonexistent")


def test_executor_turns_adapter_exception_into_failed_result():
    task = ScanTask(artifact_sha256="0" * 64, engine="mock_engine_a")
    exec_ = Executor({"mock_engine_a": {"fail_rate": 1.0}})
    result = exec_.execute(task)
    assert result.status.value == "failed"
    assert "RuntimeError" in (result.error or "")


def test_executor_success_enforces_contract():
    task = ScanTask(artifact_sha256="a" * 64, engine="mock_engine_a")
    result = Executor().execute(task)
    assert result.status.value == "succeeded"
    assert result.verdict in (ScanVerdict.MALICIOUS, ScanVerdict.BENIGN)
