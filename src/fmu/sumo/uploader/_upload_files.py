"""

    The function that uploads files.

"""

from concurrent.futures import ThreadPoolExecutor
import json
import yaml
from fmu.dataio._utils import read_parameters_txt
from fmu.dataio.dataio import ExportData
from fmu.sumo.uploader._fileonjob import FileOnJob

# pylint: disable=C0103 # allow non-snake case variable names


def maybe_add_parameters(
    case_uuid,
    realization_id,
    parameters_path,
    config_path,
    sumo_connection,
):
    """Generate a parameters object from the parameters.txt file

    Args:
        case_uuid (str): parent uuid for case
        realization_id (str): the id of the realization
        parameters_path (str): path to the parameters.txt file
        config_path (str): path to the fmu config file
        sumo_connection (SumoClient): Initialized sumo client for performing query

    Returns:
        SumoFile: parameters ready for upload
    """

    bytestring = None
    metadata = None
    status_mess = "Parameters have allready been uploaded"

    query = f"fmu.case.uuid:{case_uuid} AND fmu.realization.uuid:{realization_id} AND data.content:parameters"

    search_res = sumo_connection.api.get("/search", {"$query": query}).json()

    if search_res["hits"]["total"]["value"] > 0:
        status_mess = "Parameters allready uploaded"
        return None

    with open(config_path, "r", encoding="utf-8") as variables_yml:
        global_config = yaml.safe_load(variables_yml)

    parameters = read_parameters_txt(parameters_path)

    exd = ExportData(
        config=global_config, content="parameters", name="parameters"
    )
    metadata = exd.generate_metadata(parameters)

    bytestring = json.dumps(parameters).encode("utf-8")
    paramfile = FileOnJob(bytestring, metadata)
    paramfile.metadata_path = ""
    paramfile.size = len(bytestring)
    print(status_mess)
    return paramfile


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

    realization_id = files[0].metadata["fmu"]["realization"]["uuid"]

    paramfile = maybe_add_parameters(
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
