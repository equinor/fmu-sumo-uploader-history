"""

    The function that uploads files.

"""

from concurrent.futures import ThreadPoolExecutor

# pylint: disable=C0103 # allow non-snake case variable names


def _upload_files(files, sumo_connection, sumo_parent_id, threads=4):
    """
    Create threads and call _upload in each thread
    """

    with ThreadPoolExecutor(threads) as executor:
        results = executor.map(
            _upload_file, [(file, sumo_connection, sumo_parent_id) for file in files]
        )

    return results


def _upload_file(args):
    """Upload a file"""

    file, sumo_connection, sumo_parent_id = args

    result = file.upload_to_sumo(
        sumo_connection=sumo_connection, sumo_parent_id=sumo_parent_id
    )

    result["file"] = file

    return result


def upload_files(files: list, sumo_parent_id: str, sumo_connection, threads=4):
    """
    Upload files

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent case

    Upload is kept outside classes to use multithreading.
    """

    results = _upload_files(
        files=files,
        sumo_connection=sumo_connection,
        sumo_parent_id=sumo_parent_id,
        threads=threads,
    )

    ok_uploads = []
    failed_uploads = []
    rejected_uploads = []

    for r in results:
        status = r.get("status")

        if not status:
            raise ValueError('File upload result returned with no "status" attribute')

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
