"""

    The function that uploads files.

"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import yaml
from fmu.dataio._utils import read_parameters_txt
from fmu.dataio.dataio import ExportData
from fmu.sumo.uploader._fileonjob import FileOnJob
from fmu.sumo.uploader._logger import get_uploader_logger

# pylint: disable=C0103 # allow non-snake case variable names


logger = get_uploader_logger()


def create_parameter_file(
    case_uuid,
    realization_id,
    parameters_path,
    config_path,
    sumo_connection,
):
    """If not already stored, generate a parameters object from the parameters.txt file

    Args:
        case_uuid (str): parent uuid for case
        realization_id (str): the id of the realization
        parameters_path (str): path to the parameters.txt file
        config_path (str): path to the fmu config file
        sumo_connection (SumoClient): Initialized sumo client for performing query

    Returns:
        SumoFile: parameters ready for upload, or None
    """

    bytestring = None
    metadata = None

    query = f"fmu.case.uuid:{case_uuid} AND fmu.realization.uuid:{realization_id} AND data.content:parameters"

    search_res = sumo_connection.api.get("/search", {"$query": query}).json()

    if search_res["hits"]["total"]["value"] > 0:
        logger.info("Parameters already uploaded")
        return None

    logger.info("Trying to read parameters at %s", parameters_path)
    try:
        with open(config_path, "r", encoding="utf-8") as variables_yml:
            global_config = yaml.safe_load(variables_yml)
    except FileNotFoundError:
        logger.warning(
            "No fmu config to read at %s, cannot generate metadata to upload parameters",
            config_path,
        )
        return None

    parameters = read_parameters_txt(parameters_path)

    exd = ExportData(
        config=global_config, content="parameters", name="parameters"
    )
    metadata = exd.generate_metadata(parameters)

    if "fmu" not in metadata:
        logger.warning("No fmu section upload will fail..")

    bytestring = json.dumps(parameters).encode("utf-8")
    paramfile = FileOnJob(bytestring, metadata)
    paramfile.metadata_path = ""
    paramfile.path = ""
    paramfile.size = len(bytestring)
    logger.info("Parameters will be uploaded")
    return paramfile


def maybe_upload_realization_and_iteration(sumo_connection, base_metadata):
    realization_uuid = base_metadata["fmu"]["realization"]["uuid"]
    iteration_uuid = base_metadata["fmu"]["iteration"]["uuid"]

    hits = sumo_connection.api.post(
        "/search",
        json={
            "query": {"ids": {"values": [realization_uuid, iteration_uuid]}},
            "_source": ["class"],
        },
    ).json()["hits"]["hits"]

    classes = [hit["_source"]["class"] for hit in hits]

    if "realization" not in classes:
        realization_metadata = deepcopy(base_metadata)
        del realization_metadata["data"]
        del realization_metadata["file"]
        del realization_metadata["display"]
        realization_metadata["class"] = "realization"
        realization_metadata["fmu"]["context"]["stage"] = "iteration"

        case_uuid = realization_metadata["fmu"]["case"]["uuid"]

        if "iteration" not in classes:
            iteration_metadata = deepcopy(realization_metadata)
            del iteration_metadata["fmu"]["realization"]
            iteration_metadata["class"] = "iteration"
            iteration_metadata["fmu"]["context"]["stage"] = "case"
            sumo_connection.api.post(
                f"/objects('{case_uuid}')", json=iteration_metadata
            )
            print(f"UPLOADING ITERATION OBJECT: {iteration_metadata}")

        sumo_connection.api.post(
            f"/objects('{case_uuid}')", json=realization_metadata
        )
        print(f"UPLOADING REALIZATION OBJECT: {realization_metadata}")


def _upload_files(
    files,
    sumo_connection,
    sumo_parent_id,
    threads=4,
    sumo_mode="copy",
    config_path="fmuconfig/output/global_variables.yml",
    parameters_path="parameters.txt",
):
    """
    Create threads and call _upload in each thread
    """

    base_file = None
    for file in files:
        if "fmu" in file.metadata and "realization" in file.metadata["fmu"]:
            base_file = file
            break

    realization_id = base_file.metadata["fmu"]["realization"]["uuid"]

    # maybe_upload_realization_and_iteration(sumo_connection, base_file.metadata)

    paramfile = create_parameter_file(
        sumo_parent_id,
        realization_id,
        parameters_path,
        config_path,
        sumo_connection,
    )
    if paramfile is not None:
        files.append(paramfile)

    with ThreadPoolExecutor(threads) as executor:
        results = executor.map(
            _upload_file,
            [
                (file, sumo_connection, sumo_parent_id, sumo_mode)
                for file in files
            ],
        )

    return results


def _upload_file(args):
    """Upload a file"""

    file, sumo_connection, sumo_parent_id, sumo_mode = args

    result = file.upload_to_sumo(
        sumo_connection=sumo_connection,
        sumo_parent_id=sumo_parent_id,
        sumo_mode=sumo_mode,
    )

    result["file"] = file

    return result


def upload_files(
    files: list,
    sumo_parent_id: str,
    sumo_connection,
    threads=4,
    sumo_mode="copy",
    config_path="fmuconfig/output/global_variables.yml",
    parameters_path="parameters.txt",
):
    """
    Upload files

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent case

    Upload is kept outside classes to use multithreading.
    """

    results = _upload_files(
        files,
        sumo_connection,
        sumo_parent_id,
        threads,
        sumo_mode,
        config_path,
        parameters_path,
    )

    ok_uploads = []
    failed_uploads = []
    rejected_uploads = []

    for r in results:
        status = r.get("status")

        if not status:
            raise ValueError(
                'File upload result returned with no "status" attribute'
            )

        if status == "ok":
            ok_uploads.append(r)

        elif status == "rejected":
            rejected_uploads.append(r)

        else:
            failed_uploads.append(r)

    return {
        "ok_uploads": ok_uploads,
        "failed_uploads": failed_uploads,
        "rejected_uploads": rejected_uploads,
    }
