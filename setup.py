#!/usr/bin/env python
import os
import sys
import re

# require python 3.8 or newer
if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)

# require version of setuptools that supports find_namespace_packages
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)

# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), "r", encoding="utf8") as f:
    long_description = f.read()


# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version_dict():
    _version_path = os.path.join(this_directory, "dbt", "adapters", "watsonx_spark", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _build = r"""(\+build[0-9]+)?"""
    _version_pattern = rf"""version\s*=\s*["']{_semver}{_pre}{_build}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()


package_name = "dbt-watsonx-spark"
package_version = "0.0.6"
description = """IBM watsonx.data spark plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="IBM watsonx.data",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "sqlparams>=3.0.0",
        "dbt-common>=1.0.4,<2.0",
        "dbt-adapters>=1.1.1,<2.0",
        "dbt-core>=1.8.0",
        "PyHive[hive_pure_sasl]~=0.7.0",
        "thrift>=0.20.0",
        "requests>=2.32.2"
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8,<3.12",
)
