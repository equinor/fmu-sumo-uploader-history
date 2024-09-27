import importlib
from typing import Dict

import os
import sys
from pathlib import Path

import ert
import importlib_resources

from fmu.sumo.uploader.forward_models import SumoUpload


def _remove_suffix(string: str, suffix: str) -> str:
    if not string.endswith(suffix):
        raise ValueError(f"{string} does not end with {suffix}")
    return string[: -len(suffix)]


def _get_forward_models_from_directory(directory: str) -> Dict[str, str]:
    resource_directory_ref = importlib_resources.files("fmu.sumo.uploader") / directory
    all_files = []
    with importlib_resources.as_file(resource_directory_ref) as resource_directory:
        all_files = [
            resource_directory / file
            for file in resource_directory.glob("*")
            if (resource_directory / file).is_file()
        ]

    # ERT will look for an executable in the same folder as the forward model
    # configuration file is located. If the name of the config is the same as
    # the name of the executable, ERT will be confused. The usual standard in
    # ERT would be to capitalize the config file. On OSX systems, which are
    # case-insensitive, this isn't viable. The config files are therefore
    # appended with "_CONFIG".
    # The forward models will be installed as FORWARD_MODEL_NAME,
    # and the FORWARD_MODEL_NAME_CONFIG will point to an executable named
    # forward_model_name - which we install with entry-points. The user can use the
    # forward model as normal:
    # FORWARD_MODEL FORWARD_MODEL_NAME()
    return {_remove_suffix(path.name, "_CONFIG"): str(path) for path in all_files}


def _get_module_variable_if_exists(module_name, variable_name, default=""):
    try:
        script_module = importlib.import_module(module_name)
    except ImportError:
        return default

    return getattr(script_module, variable_name, default)


# @ert.plugin(
#     name="fmu_sumo_uploader"
# )
# def job_documentation(job_name):
#     sumo_fmu_jobs = set(installable_jobs().data.keys())
#     if job_name not in sumo_fmu_jobs:
#         return None

#     module_name = "jobs.scripts.{}".format(job_name.lower())

#     description = _get_module_variable_if_exists(
#         module_name=module_name, variable_name="description"
#     )
#     examples = _get_module_variable_if_exists(
#         module_name=module_name, variable_name="examples"
#     )
#     category = _get_module_variable_if_exists(
#         module_name=module_name, variable_name="category", default="other"
#     )

#     return {
#         "description": description,
#         "examples": examples,
#         "category": category,
#     }


@ert.plugin(name="fmu_sumo_uploader")
def installable_forward_model_steps():
    return [SumoUpload]

@ert.plugin(name="fmu_sumo_uploader")
def legacy_ertscript_workflow(config):
    config.add_workflow(SumoUpload, "SUMO_UPLOAD")
