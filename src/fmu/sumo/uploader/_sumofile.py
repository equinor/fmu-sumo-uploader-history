"""

Base class for FileOnJob and FileOnDisk classes.

"""


import os
import sys
import time
import subprocess
import logging
import httpx

# pylint: disable=C0103 # allow non-snake case variable names

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def _get_segyimport_cmdstr(blob_url, object_id, file_path, sample_unit):
    """Return the command string for running OpenVDS SEGYImport"""
    try:
        url = "azureSAS:" + blob_url["baseuri"][6:]
        url_conn = "Suffix=?" + blob_url["auth"]
    except KeyError:
        url = (
            "azureSAS" + blob_url.split(object_id)[0][5:]
        )  # SEGYImport expects url to container
        url_conn = "Suffix=?" + blob_url.split("?")[1]

    persistent_id = object_id

    segy_command = "SEGYImport"
    if sys.platform.startswith("win"):
        segy_command = segy_command + ".exe"
    python_path = os.path.dirname(sys.executable)
    # The SEGYImport folder location is not fixed
    locations = [
        os.path.join(python_path, "bin"),
        os.path.join(python_path, "..", "bin"),
        os.path.join(python_path, "..", "shims"),
        "/home/vscode/.local/bin",
        "/usr/local/bin",
    ]
    path_to_executable = None
    for loc in locations:
        path = os.path.join(loc, segy_command)
        if os.path.isfile(path):
            path_to_executable = path
            break
    if path_to_executable is None:
        logger.error("Could not find OpenVDS executables folder location")
    logger.info("Path to OpenVDS executable: " + path_to_executable)

    cmdstr = [
        path_to_executable,
        "--compression-method",
        "RLE",
        "--brick-size",
        "64",
        "--sample-unit",
        sample_unit,
        "--url",
        url,
        "--url-connection",
        url_conn,
        "--persistentID",
        persistent_id,
        file_path,
    ]

    return cmdstr


class SumoFile:
    def __init__(self):
        return

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
        logger.warn("Deleting metadata object", object_id)
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

        # We need these included even if returning before blob upload
        result = {"blob_file_path": self.path, "blob_file_size": self._size}

        # Uploader converts segy-files to OpenVDS:
        if self.metadata["data"]["format"] in ["openvds", "segy"]:
            self.metadata["data"]["format"] = "openvds"
            self.metadata["file"]["checksum_md5"] = ""

        try:
            response = self._upload_metadata(
                sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id
            )

            _t1_metadata = time.perf_counter()

            result.update(
                {
                    "metadata_upload_response_status_code": response.status_code,
                    "metadata_upload_response_text": response.text,
                    "metadata_upload_time_start": _t0_metadata,
                    "metadata_upload_time_end": _t1_metadata,
                    "metadata_upload_time_elapsed": _t1_metadata
                    - _t0_metadata,
                    "metadata_file_path": self.metadata_path,
                    "metadata_file_size": self._size,
                }
            )
            pass
        except (httpx.TimeoutException, httpx.ConnectError) as err:
            logger.warn(
                f"Metadata upload timeout/connection exception {err} {type(err)}"
            )
            result.update(
                {
                    "status": "failed",
                    "metadata_upload_response_status_code": 500,
                    "metadata_upload_response_text": str(err),
                }
            )
            pass
        except httpx.HTTPStatusError as err:
            logger.warn(
                f"Metadata upload statuserror exception {err} {type(err)}"
            )
            result.update(
                {
                    "status": "failed",
                    "metadata_upload_response_status_code": err.response.status_code,
                    "metadata_upload_response_text": err.response.reason_phrase,
                }
            )
            pass
        except Exception as err:
            logger.warn(f"Metadata upload exception {err} {type(err)}")
            result.update(
                {
                    "status": "failed",
                    "metadata_upload_response_status_code": 500,
                    "metadata_upload_response_text": str(err),
                }
            )
            pass

        if result["metadata_upload_response_status_code"] not in [200, 201]:
            logger.warn(
                "Metadata upload unsuccessful, returning",
                result["metadata_upload_response_status_code"],
            )
            return result

        self.sumo_parent_id = sumo_parent_id
        self.sumo_object_id = response.json().get("objectid")

        blob_url = response.json().get("blob_url")

        # UPLOAD BLOB

        _t0_blob = time.perf_counter()
        upload_response = {}

        if self.metadata["data"]["format"] in ["openvds", "segy"]:
            if sys.platform.startswith("darwin"):
                # OpenVDS does not support Mac/darwin directly
                # Outer code expects and interprets http error codes
                upload_response.update(
                    {
                        "status_code": 418,
                        "text": "Can not perform SEGY upload since OpenVDS does not support Mac",
                    }
                )
            else:
                if self.metadata["data"]["vertical_domain"] == "depth":
                    sample_unit = "m"
                else:
                    sample_unit = "ms"  # aka time domain

                cmd_str = _get_segyimport_cmdstr(
                    blob_url, self.sumo_object_id, self.path, sample_unit
                )
                try:
                    cmd_result = subprocess.run(
                        cmd_str, capture_output=True, text=True, shell=False
                    )
                    if cmd_result.returncode == 0:
                        upload_response.update(
                            {
                                "status_code": 200,
                                "text": "SEGY uploaded as OpenVDS.",
                            }
                        )
                    else:
                        # Outer code expects and interprets http error codes
                        logger.warn(
                            "Seismic upload failed with returncode",
                            cmd_result.returncode,
                        )
                        upload_response.update(
                            {
                                "status_code": 418,
                                "text": "FAILED SEGY upload as OpenVDS command "
                                + cmd_result.stderr,
                            }
                        )
                        pass
                    pass
                except Exception as err:
                    logger.warn(f"Seismic upload exception {err} {type(err)}")
                    upload_response.update(
                        {
                            "status_code": 418,
                            "text": "FAILED SEGY upload as OpenVDS "
                            + str(err) + " " + str(type(err)),
                        }
                    )
        else:  # non-seismic blob
            try:
                response = self._upload_byte_string(
                    sumo_connection=sumo_connection,
                    object_id=self.sumo_object_id,
                    blob_url=blob_url,
                )
                upload_response.update(
                    {
                        "status_code": response.status_code,
                        "text": response.text,
                    }
                )
                pass
            except (httpx.TimeoutException, httpx.ConnectError) as err:
                logger.warn(
                    f"Blob upload failed on timeout/connect {err} {type(err)}"
                )
                upload_response.update(
                    {
                        "status": "failed",
                        "status_code": 500,
                        "text": str(err),
                        "blob_upload_response_status_code": 500,
                        "blob_upload_response_text": str(err),
                    }
                )
                pass
            except httpx.HTTPStatusError as err:
                logger.warn(f"Blob upload failed on status {err} {type(err)}")
                upload_response.update(
                    {
                        "status": "failed",
                        "status_code": 500,
                        "text": str(err),
                        "blob_upload_response_status_code": err.response.status_code,
                        "blob_upload_response_text": err.response.reason_phrase,
                    }
                )
                pass
            except Exception as err:
                logger.warn(
                    f"Blob upload failed on exception {err} {type(err)}"
                )
                upload_response.update(
                    {
                        "status": "failed",
                        "status_code": 500,
                        "text": str(err),
                        "blob_upload_response_status_code": 500,
                        "blob_upload_response_text": str(err),
                    }
                )

        _t1_blob = time.perf_counter()

        result.update(
            {
                "blob_upload_response_status_code": upload_response[
                    "status_code"
                ],
                "blob_upload_response_status_text": upload_response["text"],
                "blob_upload_time_start": _t0_blob,
                "blob_upload_time_end": _t1_blob,
                "blob_upload_time_elapsed": _t1_blob - _t0_blob,
            }
        )

        if "status_code" not in upload_response or upload_response[
            "status_code"
        ] not in [200, 201]:
            logger.warn(
                "Deleting metadata since data-upload failed on object uuid "
                + self.sumo_object_id
            )
            result["status"] = "failed"
            self._delete_metadata(sumo_connection, self.sumo_object_id)
        else:
            result["status"] = "ok"

        return result
