#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from urllib.parse import urlparse
from pip._internal.req import parse_requirements as parse


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

REQUIREMENTS = parse_requirements("requirements/requirements.txt")
TEST_REQUIREMENTS = parse_requirements("requirements/requirements_test.txt")
DOCS_REQUIREMENTS = parse_requirements("requirements/requirements_docs.txt")
SETUP_REQUIREMENTS = parse_requirements("requirements/requirements_setup.txt")
EXTRAS_REQUIRE = {"tests": TEST_REQUIREMENTS, "docs": DOCS_REQUIREMENTS}

setup(
    name="fmu-sumo",
    description="Python package for interacting with Sumo in an FMU setting",
    url="https://github.com/equinor/fmu-sumo",
    use_scm_version={"write_to": "src/fmu/sumo/version.py"},
    author="Equinor",
    license="Apache 2.0",
    keywords="fmu, sumo",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    author_email="Equinor ASA",
    entry_points={
        "ert": [
            "fmu_sumo_jobs = fmu.sumo.hook_implementations.jobs",
            "sumo_upload = fmu.sumo.uploader.scripts.sumo_upload",
        ],
        "console_scripts": ["sumo_upload=fmu.sumo.uploader.scripts.sumo_upload:main"],
    },
    cmdclass=CMDCLASS,
    install_requires=REQUIREMENTS,
    setup_requires=SETUP_REQUIREMENTS,
    test_suite="tests",
    extras_require=EXTRAS_REQUIRE,
    python_requires=">=3.6",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=False,
)
