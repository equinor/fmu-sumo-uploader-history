"""

    The FileOnDisk class objectifies a file as it appears
    on the disk. A file in this context refers to a data/metadata
    pair (technically two files).

"""

import os
import datetime
import time
import logging
import hashlib
import base64
import tempfile
import json

import yaml

from sumo.wrapper._request_error import (
    AuthenticationError,
    TransientError,
    PermanentError,
)

from azure.core.exceptions import ResourceExistsError

# pylint: disable=C0103 # allow non-snake case variable names

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)

def parse_yaml(path):
    """From path, parse file as yaml, return data"""
    with open(path, "r") as stream:
        data = yaml.safe_load(stream)

    return data

class FileOnJob:
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

    def _upload_metadata(self, sumo_connection, sumo_parent_id):
        path = f"/objects('{sumo_parent_id}')"
        response = sumo_connection.api.post(path=path, json=self.metadata)
        return response

    def _upload_byte_string(self, sumo_connection, object_id, blob_url):
        response = sumo_connection.api.blob_client.upload_blob(blob=self.byte_string, url=blob_url)
        return response

    def _delete_metadata(self, sumo_connection, object_id):
        path = f"/objects('{object_id}')"
        response = sumo_connection.api.delete(path=path)
        return response

    def upload_to_sumo(self, sumo_parent_id, sumo_connection):
        """Upload this file to Sumo"""

        logger.debug("Starting upload_to_sumo()")

        if not sumo_parent_id:
            raise ValueError(
                f"Upload failed, sumo_parent_id passed to upload_to_sumo: {sumo_parent_id}"
            )

        _t0 = time.perf_counter()
        _t0_metadata = time.perf_counter()

        result = {}

        backoff = [1, 3, 9]

        for i in backoff:
            logger.debug("backoff in outer loop is %s", str(i))

            try:
                response = self._upload_metadata(
                    sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id
                )

                _t1_metadata = time.perf_counter()

                result["metadata_upload_response_status_code"] = response.status_code
                result["metadata_upload_response_text"] = response.text
                result["metadata_upload_time_start"] = _t0_metadata
                result["metadata_upload_time_end"] = _t1_metadata
                result["metadata_upload_time_elapsed"] = _t1_metadata - _t0_metadata

            except TransientError as err:
                logger.debug("TransientError on blob upload. Sleeping %s", str(i))
                result["status"] = "failed"
                result["metadata_upload_response_status_code"] = err.code
                result["metadata_upload_response_text"] = err.message
                time.sleep(i)
                continue

            except AuthenticationError as err:
                result["status"] = "rejected"
                result["metadata_upload_response_status_code"] = err.code
                result["metadata_upload_response_text"] = err.message
                return result
            except PermanentError as err:
                result["status"] = "rejected"
                result["metadata_upload_response_status_code"] = err.code
                result["metadata_upload_response_text"] = err.message
                return result

            break

        if result["metadata_upload_response_status_code"] not in [200, 201]:
            return result

        self.sumo_parent_id = sumo_parent_id
        self.sumo_object_id = response.json().get("objectid")

        blob_url = response.json().get("blob_url")

        # UPLOAD BLOB

        _t0_blob = time.perf_counter()
        upload_response = {}
        for i in backoff:
            logger.debug("backoff in inner loop is %s", str(i))
            try:
                response = self._upload_byte_string(
                    sumo_connection=sumo_connection,
                    object_id=self.sumo_object_id,
                    blob_url=blob_url,
                )
                upload_response["status_code"] = response.status_code
                upload_response["text"] = response.text

                _t1_blob = time.perf_counter()

                result["blob_upload_response_status_code"] = upload_response[
                    "status_code"
                ]
                result["blob_upload_response_status_text"] = upload_response["text"]
                result["blob_upload_time_start"] = _t0_blob
                result["blob_upload_time_end"] = _t1_blob
                result["blob_upload_time_elapsed"] = _t1_blob - _t0_blob
            except ResourceExistsError as err:
                upload_response["status_code"] = 200
                upload_response["text"] = "File hopefully uploaded to Oneseimic"
                _t1_blob = time.perf_counter()

                result["blob_upload_response_status_code"] = upload_response[
                    "status_code"
                ]
                result["blob_upload_response_status_text"] = upload_response["text"]
                result["blob_upload_time_start"] = _t0_blob
                result["blob_upload_time_end"] = _t1_blob
                result["blob_upload_time_elapsed"] = _t1_blob - _t0_blob

            except OSError as err:
                logger.debug("Upload failed: %s", str(err))
                result["status"] = "failed"
                self._delete_metadata(sumo_connection, self.sumo_object_id)
                return result
            except TransientError as err:
                logger.debug("Got TransientError. Sleeping for %i seconds", str(i))
                result["status"] = "failed"
                time.sleep(i)
                continue
            except AuthenticationError as err:
                logger.debug("Upload failed: %s", upload_response["text"])
                result["status"] = "rejected"
                self._delete_metadata(sumo_connection, self.sumo_object_id)
                return result
            except PermanentError as err:
                logger.debug("Upload failed: %s", upload_response["text"])
                result["status"] = "rejected"
                self._delete_metadata(sumo_connection, self.sumo_object_id)
                return result

            break

        if upload_response["status_code"] not in [200, 201]:
            logger.debug("Upload failed: %s", upload_response["text"])
            result["status"] = "failed"
            self._delete_metadata(sumo_connection, self.sumo_object_id)
        else:
            result["status"] = "ok"
        return result
