import json

from functions.common.gis_service import GISService
from argparse import ArgumentParser, RawTextHelpFormatter
from pathlib import Path

parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    "file",
    type=Path,
    metavar="file",
    help="the file with ids to delete"
)
parser.add_argument(
    "--username",
    type=str,
    required=True,
    metavar="username",
    help="ArcGIS username."
)
parser.add_argument(
    "--password",
    type=str,
    required=True,
    metavar="password",
    help="ArcGIS password."
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
        arguments.password
    )

    if not success:
        print(f"Login failed: {response}")

    gis_service = GISService(
        response,
        arguments.service
    )

    with open(arguments.file, "r") as input_file:
        forms = json.load(input_file)

    feature_ids_to_delete = []
    for form in forms:
        key = form["key"]
        if "feature_ids" in form:
            feature_ids_to_delete = feature_ids_to_delete + form["feature_ids"]
        else:
            print(f"'{key}' does not have any 'object_ids', skipping...")
            continue

    if feature_ids_to_delete:
        gis_service.delete_features(LAYER_ID, feature_ids_to_delete)

    return 0


if __name__ == "__main__":
    exit(main())
