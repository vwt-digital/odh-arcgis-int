import json

from functions.common.gis_service import GISService
from argparse import ArgumentParser, RawTextHelpFormatter
from pathlib import Path
from functions.common.utils import get_secret

parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    "input",
    type=Path,
    metavar="input",
    help="the file with IDs to delete"
)
parser.add_argument(
    "--username",
    type=str,
    required=True,
    metavar="username",
    help="ArcGIS username."
)
parser.add_argument(
    "--gcloud-project",
    type=str,
    required=True,
    metavar="project",
    help="gcloud project to get the secret from."
)
parser.add_argument(
    "--gcloud-secret-key",
    type=str,
    required=True,
    metavar="secret-key",
    help="gcloud secret key for ArcGIS password."
)
parser.add_argument(
    "--service",
    type=str,
    required=True,
    metavar="service",
    help="the feature service url"
)

arguments, unknown_arguments = parser.parse_known_args()

LAYER_ID = 0


def main() -> int:
    success, response = GISService.request_token(
        arguments.username,
        get_secret(arguments.gcloud_project, arguments.gcloud_secret_key)
    )

    if not success:
        print(f"Login failed: {response}")

    gis_service = GISService(
        response,
        arguments.service
    )

    with open(arguments.input, "r") as input_file:
        forms = json.load(input_file)

    feature_ids_to_delete = []
    for form in forms:
        key = form["key"]
        if "feature_id" in form:
            feature_ids_to_delete.append(form["feature_id"])
        else:
            print(f"'{key}' does not have any IDs, skipping...")

    if feature_ids_to_delete:
        result = gis_service.delete_features(LAYER_ID, feature_ids_to_delete)
        if result:
            print(result)
        else:
            print("Delete request failed...")

    return 0


if __name__ == "__main__":
    exit(main())
