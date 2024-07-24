"""

    The FileOnDisk class objectifies a file as it appears
    on the disk. A file in this context refers to a data/metadata
    pair (technically two files).

"""

import os
import hashlib
import base64
import yaml

from fmu.sumo.uploader._sumofile import SumoFile
from fmu.sumo.uploader._logger import get_uploader_logger


# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


def path_to_yaml_path(path):
    """
    Given a path, return the corresponding yaml file path
    according to FMU standards.
    /my/path/file.txt --> /my/path/.file.txt.yaml
    """

    dir_name = os.path.dirname(path)
    basename = os.path.basename(path)

    return os.path.join(dir_name, f".{basename}.yml")


def parse_yaml(path):
    """From path, parse file as yaml, return data"""
    with open(path, "r") as stream:
        data = yaml.safe_load(stream)
    return data


def file_to_byte_string(path):
    """
    Given an path to a file, read as bytes, return byte string.
    """

    with open(path, "rb") as f:
        byte_string = f.read()

    return byte_string


class FileOnDisk(SumoFile):
    def __init__(self, path: str, metadata_path=None, verbosity="WARNING"):
        """
        path (str): Path to file
        metadata_path (str): Path to metadata file. If not provided,
                             path will be derived from file path.
        """

        logger.setLevel(level=verbosity)

        self.metadata_path = (
            metadata_path if metadata_path else path_to_yaml_path(path)
        )
        self.path = os.path.abspath(path)
        self.metadata = parse_yaml(self.metadata_path)

        self._size = os.path.getsize(self.path)

        self.basename = os.path.basename(self.path)
        self.dir_name = os.path.dirname(self.path)

        self._file_format = None

        self.sumo_object_id = None
        self.sumo_parent_id = None

        self.metadata["_sumo"] = {}

        self.byte_string = file_to_byte_string(path)
        self.metadata["_sumo"]["blob_size"] = len(self.byte_string)
        digester = hashlib.md5(self.byte_string)
        self.metadata["_sumo"]["blob_md5"] = base64.b64encode(
            digester.digest()
        ).decode("utf-8")

    def __repr__(self):
        if not self.metadata:
            return f"\n# {self.__class__} \n# No metadata"

        s = f"\n# {self.__class__}"
        s += f"\n# Disk path: {self.path}"
        s += f"\n# Basename: {self.basename}"
        if self.byte_string is not None:
            s += f"\n# Byte string length: {len(self.byte_string)}"

        if self.sumo_object_id is not None:
            s += f"\n# Uploaded to Sumo. Sumo_ID: {self.sumo_object_id}"

        return s
