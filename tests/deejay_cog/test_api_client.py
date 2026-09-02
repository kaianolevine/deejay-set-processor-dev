"""This cog's identity: its own key, with a fallback that makes rollout safe."""

from __future__ import annotations

import pytest

from deejay_cog.api_client import API_KEY_ENV, LEGACY_SECRET_ENV, api_client, api_key


def test_uses_its_own_key_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(API_KEY_ENV, "k_deejay")
    assert api_key() == "k_deejay"
    assert api_client().api_key == "k_deejay"


def test_falls_back_to_the_shared_secret_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsetting one variable restores the previous behaviour.

    This is the rollback path: no code change, no deploy.
    """
    monkeypatch.delenv(API_KEY_ENV, raising=False)
    monkeypatch.delenv("KAIANO_API_KEY", raising=False)
    monkeypatch.setenv(LEGACY_SECRET_ENV, "sk_shared")
    assert api_key() is None
    client = api_client()
    assert client.api_key is None
    assert client.machine_secret == "sk_shared"


def test_blank_key_is_treated_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """An existing but empty variable must fall back, not authenticate with
    an empty credential and fail on the first request."""
    monkeypatch.setenv(API_KEY_ENV, "   ")
    monkeypatch.delenv("KAIANO_API_KEY", raising=False)
    assert api_key() is None


def test_base_url_is_passed_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(API_KEY_ENV, "k_deejay")
    assert api_client(base_url="https://api.example/").base_url == "https://api.example"
