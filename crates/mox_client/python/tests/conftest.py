import logging
import sys


def pytest_configure(config):
    # Configure root logger so Rust logs forwarded via pyo3-log are visible
    root = logging.getLogger()
    if not root.handlers:  # avoid duplicate handlers under repeated runs
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        root.addHandler(handler)
    # Default to INFO globally; turn up verbosity for our crate
    root.setLevel(logging.INFO)
    logging.getLogger("mox_client").setLevel(logging.DEBUG)


