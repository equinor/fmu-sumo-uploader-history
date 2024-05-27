import subprocess
from ert.config.forward_model_step import (
    ForwardModelStep,
    ForwardModelStepJSON,
)


class SumoUpload(ForwardModelStep):
    def __init__(self):
        super().__init__(
            name="SUMO_UPLOAD",
            executable="sumo_upload",
            arglist=[
                "<SUMO_CASEPATH>",
                "<SEARCHPATH>",
                "<SUMO_ENV>",
                '"--config_path"',
                "<SUMO_CONFIG_PATH>",
                '"--sumo_mode"',
                "<SUMO_MODE>",
            ],
            min_arg=2,
            max_arg=6,
            arg_types=[
                "STRING",
                "STRING",
                "STRING",
                "STRING",
                "STRING",
                "STRING",
            ],
        )

    def validate_pre_realization_run(
        self, fm_step_json: ForwardModelStepJSON
    ) -> ForwardModelStepJSON:
        return fm_step_json

    def validate_pre_experiment(self) -> None:
        try:
            env = self.private_args["<SUMO_ENV>"]
        except KeyError:
            env = "prod"

        command = f"sumo_login -e {env} -m silent"
        return_code = subprocess.call(command, shell=True)

        assert (
            return_code == 0
        ), "Your config uses Sumo, run sumo_login to authenticate."
