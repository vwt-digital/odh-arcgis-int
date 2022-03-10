import json

from argparse import ArgumentParser, RawTextHelpFormatter
from pathlib import Path

parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    "input",
    type=Path,
    metavar="input",
    help="the input log file."
)
parser.add_argument(
    "output",
    type=Path,
    metavar="output",
    help="the output data file."
)

arguments, unknown_arguments = parser.parse_known_args()


def main() -> int:
    files = []
    if arguments.input.is_dir():
        files.extend(arguments.input.glob("**/*.json"))
    else:
        files.append(arguments.input)

    data = []
    for file in files:
        with open(file, "r") as log_file:
            for line in log_file:
                data.append(json.loads(line))

    forms = []
    if arguments.output.exists():
        with open(arguments.output, "r") as output_file:
            key_list = json.load(output_file)
            for key in key_list:
                forms.append(key)

    for log_entry in data:
        text = log_entry.get("textPayload")
        if text.startswith("Excluding 'schouw' form: "):
            _, log_entry_json = text.split("Excluding 'schouw' form: ")
            entry_data = json.loads(log_entry_json)
            if entry_data not in forms:  # no duplicates, please.
                forms.append(entry_data)

    with open(arguments.output, "w") as output_file:
        json.dump(forms, output_file, indent=4)
    return 0


if __name__ == "__main__":
    exit(main())
