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


# ---------------------------------------------------------------------------
# Self-registration
# ---------------------------------------------------------------------------


def test_registers_once_when_running_under_its_own_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import MagicMock

    import deejay_cog.api_client as mod

    monkeypatch.setattr(mod, "_registered", False)
    monkeypatch.setenv(mod.MACHINE_SECRET_ENV, "sk_deejay")

    posted = MagicMock(return_value={"principal": {"subject": "mch_x", "roles": []}})
    monkeypatch.setattr(
        mod, "KaianoApiClient", lambda **_kw: MagicMock(post=posted, **{})
    )

    mod.api_client()
    mod.api_client()

    assert posted.call_count == 1
    assert posted.call_args[0][0] == "/v1/identity/register"
    assert posted.call_args[0][1] == {"name": "deejay-cog"}


def test_does_not_register_while_on_the_shared_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registering under the shared secret would bind the name to the wrong
    subject, and the real machine would then be refused forever."""
    from unittest.mock import MagicMock

    import deejay_cog.api_client as mod

    monkeypatch.setattr(mod, "_registered", False)
    monkeypatch.delenv(mod.MACHINE_SECRET_ENV, raising=False)
    monkeypatch.setenv("KAIANO_API_CLERK_MACHINE_SECRET", "sk_shared")

    posted = MagicMock()
    monkeypatch.setattr(mod, "KaianoApiClient", lambda **_kw: MagicMock(post=posted))

    mod.api_client()
    assert posted.call_count == 0


def test_registration_failure_does_not_break_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient blip must not take out an ingest run."""
    from unittest.mock import MagicMock

    import deejay_cog.api_client as mod

    monkeypatch.setattr(mod, "_registered", False)
    monkeypatch.setenv(mod.MACHINE_SECRET_ENV, "sk_deejay")

    posted = MagicMock(side_effect=RuntimeError("api down"))
    monkeypatch.setattr(mod, "KaianoApiClient", lambda **_kw: MagicMock(post=posted))

    client = mod.api_client()
    assert client is not None
