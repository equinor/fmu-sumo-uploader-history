import importlib
from typing import Dict

import os
import sys
from pathlib import Path

import ert
import importlib_resources

from fmu.sumo.uploader.forward_models import SumoUpload


@ert.plugin(name="fmu_sumo_uploader")
def installable_forward_model_steps():
    return [SumoUpload]

@ert.plugin(name="fmu_sumo_uploader")
def legacy_ertscript_workflow(config):
    config.add_workflow(SumoUpload, "SUMO_UPLOAD")

@ert.plugin(name="fmu_sumo_uploader")
def installable_jobs():
    return { "SUMO_UPLOAD": "fmu/sumo/uploader/forward_models/config/SUMO_UPLOAD_CONFIG" }

@ert.plugin(name="fmu_sumo_uploader")
def job_documentation(job_name: str):
    if job_name == "SUMO_UPLOAD":
        documentation = SumoUpload.documentation()
        return {
            "description": documentation.description,
            "examples": documentation.examples,
            "category": documentation.category
            }
