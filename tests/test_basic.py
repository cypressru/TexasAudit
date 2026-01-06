"""Basic tests for Fraudit."""

import pytest


def test_import():
    """Test that the main module can be imported."""
    import fraudit
    assert hasattr(fraudit, "__version__")


def test_version():
    """Test version string format."""
    from fraudit import __version__
    assert isinstance(__version__, str)
    parts = __version__.split(".")
    assert len(parts) >= 2


def test_config_loads():
    """Test that config module loads without error."""
    from fraudit.config import config
    assert config is not None


def test_normalization():
    """Test vendor name normalization."""
    from fraudit.normalization import normalize_vendor_name

    assert normalize_vendor_name("ACME, INC.") == "acme"
    assert normalize_vendor_name("The Widget Company LLC") == "widget company"
    assert normalize_vendor_name("ABC CORP") == "abc"
