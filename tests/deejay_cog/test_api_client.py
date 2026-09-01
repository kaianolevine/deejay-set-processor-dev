"""This cog's machine identity, and the fallback that makes rollout safe."""

from __future__ import annotations

import pytest

from deejay_cog.api_client import MACHINE_SECRET_ENV, api_client, machine_secret

SHARED_ENV = "KAIANO_API_CLERK_MACHINE_SECRET"


def test_uses_own_secret_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MACHINE_SECRET_ENV, "sk_deejay")
    monkeypatch.setenv(SHARED_ENV, "sk_shared")
    assert machine_secret() == "sk_deejay"
    assert api_client().machine_secret == "sk_deejay"


def test_falls_back_to_shared_secret_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsetting one Doppler variable must restore the previous behaviour.

    This is the rollback path: if the new Clerk machine is misconfigured,
    deleting the variable reverts identity without a code change or deploy.
    """
    monkeypatch.delenv(MACHINE_SECRET_ENV, raising=False)
    monkeypatch.setenv(SHARED_ENV, "sk_shared")
    assert machine_secret() is None
    assert api_client().machine_secret == "sk_shared"


def test_blank_value_is_treated_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """A variable that exists but is empty must fall back, not authenticate
    with an empty credential and fail at the first request."""
    monkeypatch.setenv(MACHINE_SECRET_ENV, "")
    monkeypatch.setenv(SHARED_ENV, "sk_shared")
    assert machine_secret() is None
    assert api_client().machine_secret == "sk_shared"


def test_base_url_is_passed_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MACHINE_SECRET_ENV, "sk_deejay")
    assert api_client(base_url="https://api.example/").base_url == "https://api.example"
