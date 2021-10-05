import json

from argparse import ArgumentParser, RawTextHelpFormatter
from pathlib import Path

from functions.common.gis_service import GISService
from functions.common.utils import get_secret

parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    "input",
    type=Path,
    metavar="input",
    help="file to get the keys from."
)
parser.add_argument(
    "output",
    type=Path,
    metavar="output",
    help="file to export the ids to."
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
    help="the feature service url."
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

    for form in forms:
        key = form["key"]
        # Query all feature ids with a specific 'sleutel'.
        # NOTE: 'sleutel' is derived from an address.
        possible_feature_ids = gis_service.query_features(
            feature_layer=LAYER_ID,
            out_fields=["objectid"],
            query=f"sleutel = '{key}'"
        )

        # Technically multiple IDs can be found, in that case we want to log and investigate them.
        if possible_feature_ids is None:
            print(f"Query failed for key '{key}', skipping...")
        elif not possible_feature_ids:
            print(f"No IDs found for key '{key}', feature might have already been deleted.")
        elif len(possible_feature_ids) != 1:
            print(f"Key '{key}' has multiple IDs {possible_feature_ids}, please check manually...")
        else:
            feature_id = possible_feature_ids[0]["attributes"]["objectid"]
            form["feature_id"] = feature_id

    with open(arguments.output, "w") as output_file:
        json.dump(forms, output_file, indent=4)

    return 0


if __name__ == "__main__":
    exit(main())
