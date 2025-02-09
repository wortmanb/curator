import sys

from cx_Freeze import Executable, setup
from setuptools import find_packages

# base="Win32GUI" should be used only for Windows GUI app
base = "Win32GUI" if sys.platform == "win32" else None

setup(
    packages=find_packages(),
    executables=[
        Executable("run_curator.py", base=base, target_name="curator"),
        Executable("run_singleton.py", base=base, target_name="curator_cli"),
        Executable("run_es_repo_mgr.py", base=base, target_name="es_repo_mgr"),
    ],
)
