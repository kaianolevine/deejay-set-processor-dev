from kaiano_common_utils import helpers

from deejay_set_processor import placeholder


def test_import_common_utils():
    # Sanity check that the shared utils package is available
    assert hasattr(helpers, "try_lock_folder")


def test_placeholder():
    assert placeholder() is True
