"""This cog's API client — and its machine identity.

Every cog reads ``KAIANO_API_CLERK_MACHINE_SECRET``, because that name lives
in shared library code (``mini_app_polis.api.KaianoApiClient``). With one
Doppler config behind the whole fleet, that name therefore carries one value
for everyone: possession of it is the only identity claim any cog makes, so
the API can tell that *a* cog called it and never which one. That is the
per-caller attribution Project Keystone set out to get and did not land.

Changing that shared value in place would re-key every cog at once, and each
would immediately start presenting a subject with no principal row behind it.
So identity moves one cog at a time, under a name only this cog reads.

The fallback is what makes that safe:

    set   DEEJAY_COG_CLERK_MACHINE_SECRET  -> this cog authenticates as itself
    unset DEEJAY_COG_CLERK_MACHINE_SECRET  -> falls back to the shared secret

Both directions are a Doppler edit and a restart, with no code change and no
deploy. If the new machine turns out to be misconfigured, rollback is
deleting one variable.
"""

from __future__ import annotations

import os

from mini_app_polis.api import KaianoApiClient  # type: ignore[import-untyped]

#: Doppler variable holding deejay-cog's own Clerk machine secret. Only this
#: cog reads it. Changing this name is a coordinated Doppler change.
MACHINE_SECRET_ENV = "DEEJAY_COG_CLERK_MACHINE_SECRET"


def machine_secret() -> str | None:
    """This cog's own machine secret, or None to fall back to the shared one.

    Empty string is treated as unset: a Doppler variable that exists but is
    blank should behave like an absent one, not like an empty credential that
    fails at the first request.
    """
    return os.environ.get(MACHINE_SECRET_ENV) or None


def api_client(base_url: str | None = None) -> KaianoApiClient:
    """Build the API client with this cog's identity.

    Use this everywhere instead of ``KaianoApiClient(...)`` or
    ``KaianoApiClient.from_env()``, so the cog presents one identity from
    every call site rather than depending on which constructor was reached.
    """
    return KaianoApiClient(base_url=base_url, machine_secret=machine_secret())
