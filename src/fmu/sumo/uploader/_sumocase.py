"""

Base class for CaseOnJob and CaseOnDisk classes.

"""

import logging
import warnings
import time
import datetime
import statistics
import httpx


from fmu.sumo.uploader._upload_files import upload_files


# pylint: disable=C0103 # allow non-snake case variable names

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


class SumoCase:
    def __init__(self, case_metadata: str, sumo_connection, verbosity):
        logger.setLevel(verbosity)
        self.sumo_connection = sumo_connection
        self.case_metadata = _sanitize_datetimes(case_metadata)
        self._fmu_case_uuid = self._get_fmu_case_uuid()
        logger.debug("self._fmu_case_uuid is %s", self._fmu_case_uuid)
        self._sumo_parent_id = self._fmu_case_uuid
        logger.debug("self._sumo_parent_id is %s", self._sumo_parent_id)
        self._files = []

        return

    def _get_fmu_case_uuid(self):
        """Return case_id from case_metadata."""

        fmu_case_uuid = self.case_metadata.get("fmu").get("case").get("uuid")

        if not fmu_case_uuid:
            raise ValueError("Could not get fmu_case_uuid from case metadata")

        return fmu_case_uuid

    def upload(self, threads=4):
        """Trigger upload of files.

        Upload all indexed files. Collect the files that have been uploaded OK, the
        ones that have failed and the ones that have been rejected.

        Retry the failed uploads X times."""


        if not self.files:
            raise FileExistsError("No files to upload. Check search string.")

        ok_uploads = []
        failed_uploads = []
        rejected_uploads = []
        files_to_upload = [f for f in self.files]

        _t0 = time.perf_counter()

        logger.debug("files_to_upload: %s", files_to_upload)

        upload_results = upload_files(
            files=files_to_upload,
            sumo_parent_id=self.sumo_parent_id,
            sumo_connection=self.sumo_connection,
            threads=threads,
        )

        ok_uploads += upload_results.get("ok_uploads")
        rejected_uploads += upload_results.get("rejected_uploads")
        failed_uploads = upload_results.get("failed_uploads")

        if failed_uploads:
            if any([res.get("metadata_upload_response_status_code") == 404 for res in failed_uploads]):
                warnings.warn("Case is not registered on Sumo")

        _dt = time.perf_counter() - _t0

        upload_statistics = ""
        if len(ok_uploads) > 0:
            upload_statistics = _calculate_upload_stats(ok_uploads)
            logger.info(upload_statistics)

        if rejected_uploads:
            logger.info(
                f"\n\n{len(rejected_uploads)} files rejected by Sumo. First 5 rejected files:"
            )

            for u in rejected_uploads[0:4]:
                logger.info("\n" + "=" * 50)

                logger.info(f"Filepath: {u.get('blob_file_path')}")
                logger.info(
                    f"Metadata: [{u.get('metadata_upload_response_status_code')}] "
                    f"{u.get('metadata_upload_response_text')}"
                )
                logger.info(
                    f"Blob: [{u.get('blob_upload_response_status_code')}] "
                    f"{u.get('blob_upload_response_status_text')}"
                )
                self._sumo_logger.info(_get_log_msg(self.sumo_parent_id, u))

        if failed_uploads:
            logger.info(
                f"\n\n{len(failed_uploads)} files failed by Sumo. First 5 failed files:"
            )

            for u in failed_uploads[0:4]:
                logger.info("\n" + "=" * 50)

                logger.info(f"Filepath: {u.get('blob_file_path')}")
                logger.info(
                    f"Metadata: [{u.get('metadata_upload_response_status_code')}] "
                    f"{u.get('metadata_upload_response_text')}"
                )
                logger.info(
                    f"Blob: [{u.get('blob_upload_response_status_code')}] "
                    f"{u.get('blob_upload_response_status_text')}"
                )
                self._sumo_logger.info(_get_log_msg(self.sumo_parent_id, u))

        logger.info("Summary:")
        logger.info("Total files count: %s", str(len(self.files)))
        logger.info("OK: %s", str(len(ok_uploads)))
        logger.info("Failed: %s", str(len(failed_uploads)))
        logger.info("Rejected: %s", str(len(rejected_uploads)))
        logger.info("Wall time: %s sec", str(_dt))

        summary = {
            "upload_summary": {
                "parent_id": self.sumo_parent_id,
                "total_files_count": str(len(self.files)),
                "ok_files": str(len(ok_uploads)),
                "failed_files": str(len(failed_uploads)),
                "rejected_files": str(len(rejected_uploads)),
                "wall_time_seconds": str(_dt),
                "upload_statistics": upload_statistics,
            }
        }
        self._sumo_logger.info(str(summary))

        return ok_uploads

    pass


def _get_log_msg(sumo_parent_id, status):
    """Return a suitable logging for upload issues."""

    json = {
        "upload_issue": {
            "case_uuid": str(sumo_parent_id),
            "filepath": str(status.get("blob_file_path")),
            "metadata": {
                "status_code": str(
                    status.get("metadata_upload_response_status_code")
                ),
                "response_text": status.get("metadata_upload_response_text"),
            },
            "blob": {
                "status_code": str(
                    status.get("blob_upload_response_status_code")
                ),
                "response_text": (
                    (status.get("blob_upload_response_status_text"))
                ),
            },
        }
    }
    return json


def _calculate_upload_stats(uploads):
    """Calculate upload statistics.

    Given a list of results from file upload, calculate and return
    timing statistics for uploads."""

    blob_upload_times = [u["blob_upload_time_elapsed"] for u in uploads]
    metadata_upload_times = [
        u["metadata_upload_time_elapsed"] for u in uploads
    ]

    def _get_stats(values):
        return {
            "mean": statistics.mean(values),
            "max": max(values),
            "min": min(values),
            "std": statistics.stdev(values) if len(values) > 1 else 0.0,
        }

    stats = {
        "blob": {
            "upload_time": _get_stats(blob_upload_times),
        },
        "metadata": {
            "upload_time": _get_stats(metadata_upload_times),
        },
    }

    return stats


def _sanitize_datetimes(data):
    """Sanitize datetimes.

    Given a dictionary, find and replace all datetime objects
    with isoformat string, so that it does not cause problems for
    JSON later on."""

    if isinstance(data, datetime.datetime):
        return data.isoformat()

    if isinstance(data, dict):
        for key in data.keys():
            data[key] = _sanitize_datetimes(data[key])

    elif isinstance(data, list):
        data = [_sanitize_datetimes(element) for element in data]

    return data
