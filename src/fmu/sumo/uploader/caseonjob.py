"""Objectify an FMU case (results), for usage on Radix jobs."""

import os
import glob
import time
import logging
import warnings
import datetime

import yaml
import pandas as pd

from fmu.sumo.uploader._fileonjob import FileOnJob
from fmu.sumo.uploader._upload_files import upload_files

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)

# pylint: disable=C0103 # allow non-snake case variable names


class CaseOnJob:
    """Initialize the CaseOnJob object."""

    def __init__(self, case_metadata: str, sumo_connection, verbosity="DEBUG"):
        logger.setLevel(level=verbosity)

        self.sumo_connection = sumo_connection
        self.case_metadata = case_metadata
        self._fmu_case_uuid = self._get_fmu_case_uuid()
        logger.debug("self._fmu_case_uuid is %s", self._fmu_case_uuid)
        self._sumo_parent_id = self._get_sumo_parent_id()
        logger.debug("self._sumo_parent_id is %s", self._sumo_parent_id)
        self._files = []

    @property
    def sumo_parent_id(self):
        return self._sumo_parent_id

    @property
    def fmu_case_uuid(self):
        return self._fmu_case_uuid

    @property
    def files(self):
        return self._files

    def add_files(self, byte_string, metadata):
        try:
            file = FileOnJob(byte_string=byte_string, metadata=metadata)
            self._files.append(file)
        except IOError as err:
            info = f"{err}. No metadata, skipping file."
            warnings.warn(info)

    def _get_sumo_parent_id(self):
        """Get the sumo parent ID.

        Call sumo, check if the case is already there. Use fmu_case_uuid for this."""

        query = f"fmu.case.uuid:{self.fmu_case_uuid}"
        search_results = self.sumo_connection.api.get(
            "/searchroot", query=query, size=2, **{"from": 0}
        )

        # To catch crazy rare situation when index is empty (first upload to new index)
        if not search_results.get("hits"):
            return None

        hits = search_results.get("hits").get("hits")

        if len(hits) == 0:
            return None

        if len(hits) == 1:
            sumo_parent_id = hits[0].get("_id")
            return sumo_parent_id

        raise ValueError(
            f"More than one hit for fmu.case.uuid {self.fmu_case_uuid} found on Sumo"
        )

    def _get_fmu_case_uuid(self):
        """Return case_id from case_metadata."""

        fmu_case_uuid = self.case_metadata.get("fmu").get("case").get("uuid")

        if not fmu_case_uuid:
            raise ValueError("Could not get fmu_case_uuid from case metadata")

        return fmu_case_uuid

    def upload(self, threads=4, max_attempts=1):
        """Trigger upload of files.

        Get sumo_parent_id. If None, case is not registered on Sumo.

        Upload all indexed files. Collect the files that have been uploaded OK, the
        ones that have failed and the ones that have been rejected.

        Retry the failed uploads X times."""

        if self.sumo_parent_id is None:
            logger.info("Case is not registered on Sumo")

        if not self.files:
            raise FileExistsError("No files to upload. Check search string.")

        ok_uploads = []
        failed_uploads = []
        rejected_uploads = []
        files_to_upload = [f for f in self.files]

        attempts = 0
        _t0 = time.perf_counter()

        while files_to_upload and attempts < max_attempts:
            upload_results = upload_files(
                files=files_to_upload,
                sumo_parent_id=self.sumo_parent_id,
                sumo_connection=self.sumo_connection,
                threads=threads,
            )

            ok_uploads += upload_results.get("ok_uploads")
            rejected_uploads += upload_results.get("rejected_uploads")
            failed_uploads = upload_results.get("failed_uploads")

            if not failed_uploads:
                break

            files_to_upload = [f.get("file") for f in failed_uploads]

            attempts += 1

            time.sleep(3)
            logger.debug(
                "Retrying {} failed uploads after waiting 3 seconds".format(
                    len(failed_uploads)
                )
            )

        if failed_uploads:
            warnings.warn("Stopping after {} attempts".format(attempts))

        _dt = time.perf_counter() - _t0

        if len(ok_uploads) > 0:
            upload_statistics = _calculate_upload_stats(ok_uploads)
            logger.info(upload_statistics)

        if failed_uploads:
            logger.info(f"{len(failed_uploads)} files failed to be uploaded")

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

        if failed_uploads:
            logger.info(
                f"\n\n{len(failed_uploads)} files rejected by Sumo. First 5 rejected files:"
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

        logger.info("Summary:")
        logger.info("Total files count: %s", str(len(self.files)))
        logger.info("OK: %s", str(len(ok_uploads)))
        logger.info("Failed: %s", str(len(failed_uploads)))
        logger.info("Rejected: %s", str(len(rejected_uploads)))
        logger.info("Wall time: %s sec", str(_dt))

        return ok_uploads


def _calculate_upload_stats(uploads):
    """Calculate upload statistics.

    Given a list of results from file upload, calculate and return
    timing statistics for uploads."""

    df = pd.DataFrame().from_dict(uploads)

    stats = {
        "blob": {
            "upload_time": {
                "mean": df["blob_upload_time_elapsed"].mean(),
                "max": df["blob_upload_time_elapsed"].max(),
                "min": df["blob_upload_time_elapsed"].min(),
                "std": df["blob_upload_time_elapsed"].std(),
            },
        },
        "metadata": {
            "upload_time": {
                "mean": df["metadata_upload_time_elapsed"].mean(),
                "max": df["metadata_upload_time_elapsed"].max(),
                "min": df["metadata_upload_time_elapsed"].min(),
                "std": df["metadata_upload_time_elapsed"].std(),
            },
        },
    }

    return stats
