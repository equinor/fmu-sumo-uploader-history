"""

    The FileOnDisk class objectifies a file as it appears
    on the disk. A file in this context refers to a data/metadata
    pair (technically two files).

"""

import logging
import hashlib
import base64

from fmu.sumo.uploader._sumofile import SumoFile
from fmu.sumo.uploader._logger import get_uploader_logger


# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()

class FileOnJob(SumoFile):
    def __init__(self, byte_string: str, metadata):
        """
        path (str): Path to file
        metadata_path (str): Path to metadata file. If not provided,
                             path will be derived from file path.
        """
        self.metadata = metadata
        self._size = None
        self._file_format = None
        self.sumo_object_id = None
        self.sumo_parent_id = None

        self.metadata["_sumo"] = {}

        self.byte_string = byte_string
        self.metadata["_sumo"]["blob_size"] = len(self.byte_string)
        digester = hashlib.md5(self.byte_string)
        self.metadata["_sumo"]["blob_md5"] = base64.b64encode(
            digester.digest()
        ).decode("utf-8")

        # TODO hack
        self.metadata["file"]["absolute_path"] = ""
        self.metadata["file"]["checksum_md5"] = self.metadata["_sumo"]["blob_md5"]

