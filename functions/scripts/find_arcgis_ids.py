import json

from functions.common.gis_service import GISService
from argparse import ArgumentParser, RawTextHelpFormatter
from pathlib import Path

parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    "file",
    type=Path,
    metavar="file",
    help="the file to add arcgis ids to."
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

BOP_ATTACHMENT_PREFIX = "fca_FOTOBOP_locatie_"
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

    for form in forms:
        if "feature_ids" not in form:
            # Query all feature ids with a specific 'sleutel'.
            # NOTE: 'sleutel' is derived from an address.
            possible_feature_ids = gis_service.query_features(
                feature_layer=LAYER_ID,
                out_fields=["objectid"],
                query=f"sleutel = '{form['key']}'"
            )

            feature_ids = []

            # We need to make sure that the ID is not a BOP.
            # This can theoretically be the case if an address has both a SCHOUW and BOP form.
            for feature_id in possible_feature_ids:
                is_bop = False
                attachments = gis_service.get_attachments(LAYER_ID, feature_id)
                # Check if attachments contain a BOP-type attachment.
                for attachment in attachments:
                    name = attachment["name"]
                    if name.startswith(BOP_ATTACHMENT_PREFIX):
                        is_bop = True
                        break

                if not is_bop:
                    feature_ids.append(feature_id)

            form["feature_ids"] = feature_ids

    with open(arguments.file, "w") as input_file:
        json.dump(forms, input_file, indent=4)

    return 0


if __name__ == "__main__":
    exit(main())
