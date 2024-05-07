"""Objectify an FMU case (results) as it appears on the disk."""

import os
from pathlib import Path
import glob
import logging
import warnings
import httpx

import yaml
import json
import hashlib
import base64

from fmu.sumo.uploader._sumocase import SumoCase
from fmu.sumo.uploader._fileondisk import FileOnDisk
from fmu.dataio import ExportData
from fmu.dataio._utils import read_parameters_txt
from fmu.sumo.uploader._logger import get_uploader_logger



logger = get_uploader_logger()

# pylint: disable=C0103 # allow non-snake case variable names


class CaseOnDisk(SumoCase):
    """
    Class to hold information about an ERT run on disk.

    The CaseOnDisk object is a representation of files belonging to an FMU case,
    as they are stored on the Scratch disk.

    A Case in this context is a set of metadata describing this particular case,
    and an arbitrary number of files belonging to this case. Each file is in reality
    a file pair, consisting of a data file (could be any file type) and a metadata file
    (yaml formatted, according) to FMU standards.

    Example for initialization:
        >>> from fmu import sumo

        >>> env = 'dev'
        >>> case_metadata_path = 'path/to/case_metadata.yaml'
        >>> search_path = 'path/to/search_path/'

        >>> sumo_connection = sumo.SumoConnection(env=env)
        >>> case = sumo.CaseOnDisk(
                case_metadata_path=case_metadata_path,
                sumo_connection=sumo_connection)

        After initialization, files must be explicitly indexed into the CaseOnDisk object:

        >>> case.add_files(search_path)

        When initialized, the case can be uploaded to Sumo:

        >>> case.upload()

    Args:
        case_metadata_path (str): Path to the case_metadata file for the case
        sumo_connection (fmu.sumo.SumoConnection): SumoConnection object


    """

    def __init__(
        self, case_metadata_path: str, sumo_connection, 
        verbosity=logging.WARNING, sumo_mode="copy"
    ):
        """Initialize CaseOnDisk.

        Args:
            case_metadata_path (str): Path to case_metadata for case
            sumo_connection (fmu.sumo.SumoConnection): Connection to Sumo.
            verbosity (str): Python logging level.
        """

        self.verbosity = verbosity
        logger.setLevel(level=verbosity)

        logger.debug("case metadata path: %s", case_metadata_path)
        self._case_metadata_path = Path(case_metadata_path)
        case_metadata = _load_case_metadata(case_metadata_path)
        super().__init__(case_metadata, sumo_connection, verbosity, sumo_mode)

        self._sumo_logger = sumo_connection.api.getLogger(
            "fmu-sumo-uploader"
        )
        self._sumo_logger.setLevel(logging.INFO)
        # Avoid that logging to sumo-server also is visible in local logging:
        self._sumo_logger.propagate = False
        self._sumo_logger.info(
            "Initializing Sumo upload for case with sumo_parent_id: " + str(self._sumo_parent_id), 
                extra={'objectUuid': self._sumo_parent_id}
        )

    def __str__(self):
        s = f"{self.__class__}, {len(self._files)} files."

        if self._sumo_parent_id is not None:
            s += f"\nInitialized on Sumo. Sumo_ID: {self._sumo_parent_id}"
        else:
            s += "\nNot initialized on Sumo."

        return s

    def __repr__(self):
        return str(self.__str__)

    @property
    def sumo_parent_id(self):
        """Return the sumo parent ID"""
        return self._sumo_parent_id

    @property
    def fmu_case_uuid(self):
        """Return the fmu_case_uuid"""
        return self._fmu_case_uuid

    @property
    def files(self):
        """Return the files"""
        return self._files

    def add_files(self, search_string):
        """Add files to the case, based on search string"""

        logger.info("Searching for files at %s", search_string)
        file_paths = _find_file_paths(search_string)

        for file_path in file_paths:
            try:
                file = FileOnDisk(path=file_path, verbosity=self.verbosity)
                self._files.append(file)
                logger.info("File appended: %s", file_path)

            except Exception as err:
                warnings.warn(f"No metadata, skipping file: {err}")

    def upload_parameters_txt(
        self,
        glob_var_path: str = "./fmuconfig/output/global_variables.yml",
        parameters_path: str = "./parameters.txt",
    ):
        """Upload parameters.txt if it is not present in Sumo for the current realization"""
        logger.info("Uploading parameters.txt")
        print(f"CONFIG_PATH: {glob_var_path}")

        fmu_id = self.fmu_case_uuid
        if not "realization" in self.files[0].metadata["fmu"].keys():
            logger.info("Cannot upload parameters.txt due to no realization")
            return

        realization_id = self.files[0].metadata["fmu"]["realization"]["uuid"]
        query = f"fmu.case.uuid:{fmu_id} AND fmu.realization.uuid:{realization_id} AND data.content:parameters"

        search_res = self.sumo_connection.api.get(
            "/search", {"$query": query}
        ).json()

        if search_res["hits"]["total"]["value"] == 0:
            with open(glob_var_path, "r") as variables_yml:
                global_config = yaml.safe_load(variables_yml)

            parameters = read_parameters_txt(parameters_path)

            exd = ExportData(
                config=global_config, content="parameters", name="parameters"
            )
            metadata = exd.generate_metadata(parameters)

            bytes = json.dumps(parameters).encode("utf-8")
            digester = hashlib.md5(bytes)
            md5 = base64.b64encode(digester.digest()).decode("utf-8")
            metadata["_sumo"] = {"blob_size": len(bytes), "blob_md5": md5}

            upload_res = self.sumo_connection.api.post(
                f"/objects('{fmu_id}')", json=metadata
            )
            self.sumo_connection.api.blob_client.upload_blob(
                blob=bytes, url=upload_res.json()["blob_url"]
            )
        else:
            logger.info("Parameters.txt already exists")

    def register(self):
        """Register this case on Sumo.

        Assumptions: If registering an already existing case, it will be overwritten.
        ("register" might be a bad word for this...)

        Returns:
            sumo_parent_id (uuid4): Unique ID for this case on Sumo
        """

        logger.info("About to register case on Sumo")

        try:
            sumo_parent_id = self._upload_case_metadata(self.case_metadata)
            self._sumo_parent_id = sumo_parent_id

            logger.info("Case registered. SumoID: {}".format(sumo_parent_id))

            return sumo_parent_id
        except Exception as err:
            print(
                "\n\033[31m"
                "Error during registering case on Sumo. "
                "\nFile uploads will also fail. "
                "\033[0m"
            )
            error_string = f"Registering case on Sumo failed: error details: {err} {type(err)}"
            if isinstance(err, httpx.HTTPStatusError):
                if err.response.status_code == 401:
                    print(
                        "\033[31m"
                        "Please verify that you are logged in to Sumo, "
                        "by running sumo_login in a Unix terminal window"
                        " \033[0m"
                    )
                if err.response.status_code == 403:
                    print(
                        "\033[31m"
                        "Please verify that you have write access"
                        " to Sumo (AccessIT)"
                        "\033[0m"
                    )
                error_string = f"{error_string} {err.response.text}"
            error_string = f"{error_string} Case metadata file path: {self._case_metadata_path}"
            print(error_string)
            warnings.warn(error_string)
            return "0"

    def _upload_case_metadata(self, case_metadata: dict):
        """Upload case metadata to Sumo."""

        response = self.sumo_connection.api.post(
            path="/objects", json=case_metadata
        )

        returned_object_id = response.json().get("objectid")

        return returned_object_id


def _load_case_metadata(case_metadata_path: str):
    """Load the case metadata."""

    if not os.path.isfile(case_metadata_path):
        warnings.warn(
            f"Invalid metadata: file does not exist {case_metadata_path}"
        )
        return {}

    try:
        with open(case_metadata_path, "r") as stream:
            yaml_data = yaml.safe_load(stream)
        return yaml_data
    except Exception as err:
        warnings.warn(f"Invalid metadata in yml file {case_metadata_path}")
        return {}


def _find_file_paths(search_string):
    """Find files and return as list of FileOnDisk instances."""

    files = [f for f in glob.glob(search_string) if os.path.isfile(f)]

    if len(files) == 0:
        warnings.warn("No files found! Please, check the search string.")
        warnings.warn(f"Search string: {search_string}")

    return files
