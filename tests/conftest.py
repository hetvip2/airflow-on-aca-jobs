"""Test config: put the plugin packages on the import path.

The runtime artifact is the Airflow *plugins* folder, so tests import the same
modules Airflow would (``operators.*`` / ``triggers.*``) by adding that folder
to ``sys.path``. No Azure account or network is required - every ARM call is
mocked.
"""
import os
import sys

_PLUGINS = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "airflow", "plugins"
)
if _PLUGINS not in sys.path:
    sys.path.insert(0, _PLUGINS)
