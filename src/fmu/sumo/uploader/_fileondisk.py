"""

    The FileOnDisk class objectifies a file as it appears
    on the disk. A file in this context refers to a data/metadata
    pair (technically two files).

"""

import os
import datetime
import time
import sys
import subprocess
import logging
import hashlib
import base64
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


def _get_segyimport_cmdstr(blob_url, object_id, file_path, sample_unit):
    """Return the command string for running OpenVDS SEGYImport"""
    try: 
        url = '"azureSAS:' + blob_url["baseuri"][6:] + '"'  
        url_conn = '"Suffix=?' + blob_url["auth"] + '"'
    except:
        url = '"azureSAS' + blob_url.split(object_id)[0][5:] + '"'  # SEGYImport expects url to container
        url_conn = '"Suffix=?' + blob_url.split("?")[1] + '"'

    persistent_id = '"' + object_id + '"'

    python_path = os.path.dirname(sys.executable)
    path_to_SEGYImport = os.path.join(python_path, '..', 'bin', 'SEGYImport') 
    if sys.platform.startswith("win"):
        path_to_SEGYImport = path_to_SEGYImport + ".exe"
    if not os.path.isfile(path_to_SEGYImport):
        path_to_SEGYImport = os.path.join(python_path, '..', 'shims', 'SEGYImport')

    cmdstr = ' '.join([path_to_SEGYImport, 
        '--compression-method', 'RLE',
        '--brick-size', '64', 
        '--sample-unit', sample_unit, 
        '--url', url,
        '--url-connection', url_conn, 
        '--persistentID', persistent_id,
        file_path])
    return cmdstr


class FileOnDisk:
    def __init__(self, path: str, metadata_path=None, verbosity="INFO"):
        """
        path (str): Path to file
        metadata_path (str): Path to metadata file. If not provided,
                             path will be derived from file path.
        """

        self.verbosity = verbosity
        logger.setLevel(level=self.verbosity)

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

        if self.metadata["data"]["format"] in ["openvds", "segy"]:
            self.metadata["_sumo"]["blob_size"] = 0
            self.byte_string = None
        else:
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

        if not self.sumo_object_id is None:
            s += f"\n# Uploaded to Sumo. Sumo_ID: {self.sumo_object_id}"

        return s

    @property
    def size(self):
        """Size of the file"""
        if self._size is None:
            self._size = os.path.getsize(self.path)

        return self._size

    def _upload_metadata(self, sumo_connection, sumo_parent_id):
        path = f"/objects('{sumo_parent_id}')"
        response = sumo_connection.api.post(path=path, json=self.metadata)
        return response

    def _upload_byte_string(self, sumo_connection, object_id, blob_url):
        response = sumo_connection.api.blob_client.upload_blob(
            blob=self.byte_string, url=blob_url
        )
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
                f"Upload failed, missing sumo_parent_id. Got: {sumo_parent_id}"
            )

        _t0 = time.perf_counter()
        _t0_metadata = time.perf_counter()

        result = {}

        backoff = [1, 3, 9]

        for i in backoff:
            logger.debug("backoff in outer loop is %s", str(i))

            try:

                # We need these included even if returning before blob upload
                result["blob_file_path"] = self.path
                result["blob_file_size"] = self.size

                # Uploader converts segy-files to OpenVDS:
                if self.metadata["data"]["format"] in ["openvds", "segy"]:
                    self.metadata["data"]["format"] = "openvds"
                    self.metadata["file"]["checksum_md5"] = ""

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
                if self.metadata["data"]["format"] in ["openvds", "segy"]:
                    if sys.platform.startswith('darwin'):  
                        # OpenVDS does not support Mac/darwin directly
                        # Outer code expects and interprets http error codes
                        upload_response["status_code"] = 418  
                        upload_response["text"] = "Can not perform SEGY upload since OpenVDS does not support Mac" 
                    else:
                        if self.metadata["data"]["vertical_domain"] == 'depth':
                            sample_unit = 'm'   
                        else:
                            sample_unit = 'ms'  # aka time domain
                        cmd_str = _get_segyimport_cmdstr(blob_url, self.sumo_object_id, self.path, sample_unit)
                        cmd_result = subprocess.run(cmd_str, 
                                capture_output=True, text=True, shell=True)
                        if cmd_result.returncode == 0:
                            upload_response["status_code"] = 200
                            upload_response["text"] = "SEGY uploaded as OpenVDS."
                        else:
                            # Outer code expects and interprets http error codes
                            upload_response["status_code"] = 418  
                            upload_response["text"] = "FAILED SEGY upload as OpenVDS. " + cmd_result.stderr
                else:                
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
                upload_response["text"] = "File hopefully uploaded"
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
                logger.debug("Got TransientError. Sleeping for %s seconds", str(i))
                result["status"] = "failed"
                result["blob_upload_response_status_code"] = err.code
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

        if "status_code" not in upload_response or upload_response[
            "status_code"
        ] not in [200, 201]:
            logger.debug("Upload failed: %s", upload_response["text"])
            result["status"] = "failed"
            self._delete_metadata(sumo_connection, self.sumo_object_id)
        else:
            result["status"] = "ok"

        return result
