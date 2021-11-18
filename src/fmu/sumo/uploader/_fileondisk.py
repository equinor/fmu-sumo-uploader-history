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
import oneseismic.scan.__main__ as scan
import oneseismic.upload.__main__ as upload

import yaml

from sumo.wrapper._request_error import (
    AuthenticationError,
    TransientError,
    PermanentError,
)

from azure.core.exceptions import ResourceExistsError

# pylint: disable=C0103 # allow non-snake case variable names


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


def _datetime_now():
    """Return datetime now on FMU standard format"""
    return datetime.datetime.now().isoformat()


class FileOnDisk:
    def __init__(self, path: str, metadata_path=None):
        """
        path (str): Path to file
        metadata_path (str): Path to metadata file. If not provided,
                             path will be derived from file path.
        """
        self.metadata_path = metadata_path if metadata_path else path_to_yaml_path(path)
        self.path = os.path.abspath(path)
        self.metadata = parse_yaml(self.metadata_path)



        self._size = None
        
        self.basename = os.path.basename(self.path)
        self.dir_name = os.path.dirname(self.path)

        self._file_format = None

        self.sumo_object_id = None
        self.sumo_parent_id = None

        self.metadata["_sumo"] = {}

        #if self.metadata["class"] == "seismic":
        if self.metadata["data"]["format"] == "segy":
            self.metadata["_sumo"]["blob_size"] = 0
            self.manifest = json.loads(scan.main([self.path]))
            self.metadata["_sumo"]["blob_sha256"] = self.manifest["guid"]

        else:
            self.byte_string = file_to_byte_string(path)
            self.metadata["_sumo"]["blob_size"] = len(self.byte_string)
            digester = hashlib.md5(self.byte_string)
            self.metadata["_sumo"]["blob_md5"] = base64.b64encode(digester.digest()).decode(
                "utf-8"
            )

    def __repr__(self):
        if not self.metadata:
            return f"\n# {self.__class__} \n# No metadata"

        s = f"\n# {self.__class__}"
        s += f"\n# Disk path: {self.path}"
        s += f"\n# Basename: {self.basename}"
        s += f"\n# Byte string length: {len(self.byte_string)}"

        if self.sumo_object_id is None:
            s += "\n# Not uploaded to Sumo"
        else:
            s += f"\n# Uploaded to Sumo. Sumo_ID: {self.sumo_object_id}"

        return s

    @property
    def size(self):
        """Size of the file"""
        if self._size is None:
            self._size = os.path.getsize(self.path)

        return self._size

    def _upload_metadata(self, sumo_connection, sumo_parent_id):
        response = sumo_connection.api.save_child_level_json(
            json=self.metadata, parent_id=sumo_parent_id
        )
        return response

    def _upload_byte_string(self, sumo_connection, object_id, blob_url):
        response = sumo_connection.api.save_blob(
            blob=self.byte_string, object_id=object_id, url=blob_url
        )
        return response

    def _delete_metadata(self, sumo_connection, object_id):
        response = sumo_connection.api.delete_object(object_id=object_id)
        return response

    def upload_to_sumo(self, sumo_parent_id, sumo_connection):
        """Upload this file to Sumo"""

        if not sumo_parent_id:
            raise ValueError(
                f"Upload failed, sumo_parent_id passed to upload_to_sumo: {sumo_parent_id}"
            )

        _t0 = time.perf_counter()
        _t0_metadata = time.perf_counter()

        result = {}

        backoff = [1,3,9]

        for i in backoff:

            try:

                # We need these included even if returning before blob upload
                result["blob_file_path"] = self.path
                result["blob_file_size"] = self.size

                response = self._upload_metadata(
                    sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id
                )

                _t1_metadata = time.perf_counter()

                result["metadata_upload_response_status_code"] = response.status_code
                result["metadata_upload_response_text"] = response.text
                result["metadata_upload_time_start"] = _t0_metadata
                result["metadata_upload_time_end"] = _t1_metadata
                result["metadata_upload_time_elapsed"] = _t1_metadata - _t0_metadata
                result["metadata_file_path"] = self.metadata_path
                result["metadata_file_size"] = self.size

            except TransientError as err:
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
            try:
                #if self.metadata["class"] == "seismic":
                if self.metadata["data"]["format"] == "segy":
                    with tempfile.NamedTemporaryFile(mode='w+') as temp:
                        json.dump(self.manifest, temp)
                        temp.flush()
                        args = ["--output-auth-method",
                                "connection-string",
                                "--output-connection-string",
                                "BlobEndpoint=" + response.json()["blob_url"],
                                temp.name,
                                self.path]
                        upload.main(args)
                    upload_response["status_code"] = 200
                    upload_response["text"] = "File hopefully uploaded to Oneseimic"
                else:    
                    response = self._upload_byte_string(
                    sumo_connection=sumo_connection,
                    object_id=self.sumo_object_id,
                    blob_url=blob_url,
                    )
                    upload_response["status_code"] = response.status_code
                    upload_response["text"] = response.text



                _t1_blob = time.perf_counter()

                result["blob_upload_response_status_code"] = upload_response["status_code"]
                result["blob_upload_response_text"] = upload_response["text"]
                result["blob_upload_time_start"] = _t0_blob
                result["blob_upload_time_end"] = _t1_blob
                result["blob_upload_time_elapsed"] = _t1_blob - _t0_blob
            except ResourceExistsError as err:
                upload_response["status_code"] = 200
                upload_response["text"] = "File hopefully uploaded to Oneseimic"
                _t1_blob = time.perf_counter()

                result["blob_upload_response_status_code"] = upload_response["status_code"]
                result["blob_upload_response_text"] = upload_response["text"]
                result["blob_upload_time_start"] = _t0_blob
                result["blob_upload_time_end"] = _t1_blob
                result["blob_upload_time_elapsed"] = _t1_blob - _t0_blob
                
            except OSError as err:
                logging.info(f"Upload failed: {err}")
                result["status"] = "failed"
                self._delete_metadata(self.sumo_object_id)
                return result
            except TransientError as err:
                result["status"] = "failed"
                time.sleep(i)
                continue
            except AuthenticationError as err:
                logging.info(f"Upload failed: {upload_response['text']}")
                result["status"] = "rejected"
                self._delete_metadata(self.sumo_object_id)
                return result
            except PermanentError as err:
                logging.info(f"Upload failed: {upload_response['text']}")
                result["status"] = "rejected"
                self._delete_metadata(self.sumo_object_id)
                return result

            break

            
        if upload_response["status_code"] not in [200, 201]:
            logging.info(f"Upload failed: {upload_response['text']}")
            result["status"] = "failed"
            self._delete_metadata(self.sumo_object_id)
        else:
            result["status"] = "ok"
        return result
