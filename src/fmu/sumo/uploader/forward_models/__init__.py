import subprocess
from ert import (
    ForwardModelStepJSON,
    ForwardModelStepPlugin,
    ForwardModelStepValidationError,
)


class SumoUpload(ForwardModelStepPlugin):
    def __init__(self):
        super().__init__(
            name="SUMO_UPLOAD",
            command=[
                "sumo_upload",
                "<SUMO_CASEPATH>",
                "<SEARCHPATH>",
                "<SUMO_ENV>",
                "--config_path",
                "<SUMO_CONFIG_PATH>",
                "--sumo_mode",
                "<SUMO_MODE>",
            ],
            default_mapping={
                "<SUMO_CONFIG_PATH>": "fmuconfig/output/global_variables.yml",
                "<SUMO_MODE>": "copy",
                "<SUMO_ENV>": "prod",
            },
            stderr_file="sumo_upload.stderr",
            stdout_file="sumo_upload.stdout",
        )

    def validate_pre_realization_run(
        self, fm_step_json: ForwardModelStepJSON
    ) -> ForwardModelStepJSON:
        return fm_step_json

    def validate_pre_experiment(
        self, fm_step_json: ForwardModelStepJSON
    ) -> None:
        env = fm_step_json["argList"][2]
        command = f"sumo_login -e {env} -m silent"
        return_code = subprocess.call(command, shell=True)

        err_msg = (
            "Your config uses Sumo"
            ", please authenticate using:"
            f"sumo_login{f' -e {env}' if env != 'prod' else ''}"
        )

        if return_code != 0:
            raise ForwardModelStepValidationError(err_msg)
