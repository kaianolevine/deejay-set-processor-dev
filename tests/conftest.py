import sys
import types


def _install_kaiano_stubs() -> None:
    """
    Provide minimal `kaiano` modules for unit tests when the real dependency
    isn't installed in this execution environment.
    """

    kaiano_mod = types.ModuleType("kaiano")
    sys.modules["kaiano"] = kaiano_mod

    # kaiano.config
    config_mod = types.ModuleType("kaiano.config")
    # These are patched in tests that rely on them; placeholders are enough
    # for import-time attribute access.
    for name in [
        "DJ_SETS_FOLDER_ID",
        "OUTPUT_NAME",
        "TEMP_TAB_NAME",
        "SUMMARY_TAB_NAME",
        "CSV_SOURCE_FOLDER_ID",
        "SUMMARY_FOLDER_NAME",
    ]:
        setattr(config_mod, name, "")
    sys.modules["kaiano.config"] = config_mod
    kaiano_mod.config = config_mod

    # kaiano.logger
    logger_mod = types.ModuleType("kaiano.logger")

    class _DummyLogger:
        def info(self, *_args, **_kwargs) -> None:
            return None

        def debug(self, *_args, **_kwargs) -> None:
            return None

        def warning(self, *_args, **_kwargs) -> None:
            return None

        def error(self, *_args, **_kwargs) -> None:
            return None

        def exception(self, *_args, **_kwargs) -> None:
            return None

    def get_logger() -> _DummyLogger:
        return _DummyLogger()

    logger_mod.get_logger = get_logger  # type: ignore[attr-defined]
    sys.modules["kaiano.logger"] = logger_mod
    kaiano_mod.logger = logger_mod

    # kaiano.google
    google_mod = types.ModuleType("kaiano.google")

    class GoogleAPI:  # pragma: no cover
        pass

    google_mod.GoogleAPI = GoogleAPI
    sys.modules["kaiano.google"] = google_mod
    kaiano_mod.google = google_mod

    # kaiano.json
    json_mod = types.ModuleType("kaiano.json")

    def create_collection_snapshot(_folder_name: str) -> dict:
        return {"folders": []}

    def write_json_snapshot(_snapshot: dict, _path: str) -> None:
        return None

    json_mod.create_collection_snapshot = create_collection_snapshot  # type: ignore[attr-defined]
    json_mod.write_json_snapshot = write_json_snapshot  # type: ignore[attr-defined]
    sys.modules["kaiano.json"] = json_mod
    kaiano_mod.json = json_mod

    # kaiano.api (used by pipeline_evaluator and ingest_to_api)
    api_mod = types.ModuleType("kaiano.api")

    class KaianoApiError(Exception):
        pass

    class KaianoApiClient:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def post(self, *_args, **_kwargs) -> dict:
            return {}

        @classmethod
        def from_env(cls) -> "KaianoApiClient":
            return cls()

    api_mod.KaianoApiClient = KaianoApiClient
    api_mod.KaianoApiError = KaianoApiError
    sys.modules["kaiano.api"] = api_mod
    kaiano_mod.api = api_mod

    api_errors_mod = types.ModuleType("kaiano.api.errors")
    api_errors_mod.KaianoApiError = KaianoApiError
    sys.modules["kaiano.api.errors"] = api_errors_mod


try:
    import kaiano  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover
    _install_kaiano_stubs()
