#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pathlib
from urllib.parse import urlparse

from pip._internal.req import parse_requirements as parse
from setuptools import find_packages, setup


def _format_requirement(req):
    if req.is_editable:
        # parse out egg=... fragment from VCS URL
        parsed = urlparse(req.requirement)
        egg_name = parsed.fragment.partition("egg=")[-1]
        without_fragment = parsed._replace(fragment="").geturl()
        return f"{egg_name} @ {without_fragment}"
    return req.requirement


def parse_requirements(fname):
    """Turn requirements.txt into a list"""
    reqs = parse(fname, session="test")
    return [_format_requirement(ir) for ir in reqs]


CMDCLASS = {}

try:
    from sphinx.setup_command import BuildDoc

    CMDCLASS.update({"build_sphinx": BuildDoc})
except ImportError:
    # sphinx not installed - do not provide build_sphinx cmd
    pass

is_docs = os.getenv("READTHEDOCS") == "True"

REQUIREMENTS = (
    [] if is_docs else parse_requirements("requirements/requirements.txt")
)
TEST_REQUIREMENTS = parse_requirements("requirements/requirements_test.txt")
DOCS_REQUIREMENTS = parse_requirements("requirements/requirements_docs.txt")
SETUP_REQUIREMENTS = parse_requirements("requirements/requirements_setup.txt")
EXTRAS_REQUIRE = {"tests": TEST_REQUIREMENTS, "docs": DOCS_REQUIREMENTS}

setup(
    name="fmu-sumo",
    description="Python package for interacting with Sumo in an FMU setting",
    long_description=pathlib.Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/equinor/fmu-sumo",
    use_scm_version={"write_to": "src/fmu/sumo/version.py"},
    author="Equinor",
    license="Apache 2.0",
    keywords="fmu, sumo",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "ert": [
            "fmu_sumo_jobs = fmu.sumo.hook_implementations.jobs",
            "sumo_upload = fmu.sumo.uploader.scripts.sumo_upload",
        ],
        "console_scripts": [
            "sumo_upload=fmu.sumo.uploader.scripts.sumo_upload:main",
        ],
    },
    cmdclass=CMDCLASS,
    install_requires=REQUIREMENTS,
    setup_requires=SETUP_REQUIREMENTS,
    test_suite="tests",
    extras_require=EXTRAS_REQUIRE,
    python_requires=">=3.8",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=False,
)
