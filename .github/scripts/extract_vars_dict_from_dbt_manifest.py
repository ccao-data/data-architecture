#!/usr/bin/env python3
# Read a dbt manifest file from stdin, parse its metadata for each column in
# the DAG, and output a CCAO variable dict as CSV.
#
# Example:
#
#   extract_vars_dict_from_dbt_manifest.py < target/manifest.json > vars_dict.csv
import csv
import json
import sys
import typing

# Constants representing fields in the input/output data
REQUIRED_INPUT_FIELDS = [
  "var_name_hie",
  "var_name_iasworld",
  "var_name_athena",
  "var_name_model",
  "var_name_publish",
  "var_name_pretty",
  "var_type",
  "var_data_type",
]

CODE_INPUT_KEY_FIELD = "var_codes"
CODE_OUTPUT_KEY_FIELD = "var_code"
CODE_OUTPUT_VALUE_FIELDS = ["var_value", "var_value_short"]
CODE_OUTPUT_FIELDS = (
    [CODE_OUTPUT_KEY_FIELD] + CODE_OUTPUT_VALUE_FIELDS
)

OUTPUT_FIELDS = REQUIRED_INPUT_FIELDS + CODE_OUTPUT_FIELDS

def main() -> None:
    """The main entrypoint for this script.

    Reads a dbt manifest.json file from sys.stdin and writes a CSV to
    sys.stdout.
    """
    input_manifest = json.load(sys.stdin)
    output_csv = csv.DictWriter(sys.stdout, fieldnames=OUTPUT_FIELDS)
    output_csv.writeheader()

    # We expect a manifest.json structure like the following:
    # {
    #   "nodes": {  # set of node objects
    #     "model.athena.default.vw_card_res_char": {  # node_name
    #       "columns": {  # set of column objects
    #         "char_air": {  # column_name
    #           "meta": {...}  # metadata object
    #         }
    #       }
    #     }
    #   }
    # }
    for node_name, node in input_manifest["nodes"].items():
        for column_name, column in node["columns"].items():
            if column["meta"]:
                rows = _convert_column_metadata_to_list(node, column_name)
                output_csv.writerows(rows)


def _convert_column_metadata_to_list(
    node: typing.Dict,
    column_name: str
) -> typing.List[typing.Dict]:
    """Takes a node object derived from a node definition in a dbt
    manifest.json file, along with the name of a column in that node, and turns
    it into a list of objects formatted for output to our variable dictionary
    representing the column metadata.

    The reason this outputs a list of objects instead of a singular object is
    that for categorical columns, we need to expand out each enumerated value
    into its own row in the variable dictionary. Non-categorical columns are
    not expanded, and hence are output as a list containing one object."""
    output_list = []
    object_output = _format_column_metadata(node, column_name)
    if var_codes := node["columns"][column_name]["meta"].get(
        CODE_INPUT_KEY_FIELD
    ):
        # The presence of a code input key field indicates a categorical
        # variable, so we need to expand this variable out into multiple
        # objects, one for each enumerated value.
        #
        # We expect a structure like the following:
        #
        # {
        #   "var_codes": {  # code input key field
        #     1: {  # code key
        #       "var_value": "Central A/C"  # code value
        #       "var_value_short": "YES"  # code short value
        #     }
        #   }
        # }
        for code, code_map in var_codes.items():
            code_object = {CODE_OUTPUT_KEY_FIELD: code}
            for output_value in CODE_OUTPUT_VALUE_FIELDS:
                if output_value not in code_map:
                    raise ValueError(
                       f"Metadata field 'var_codes.{code}' in "
                       f"{node['name']} is missing "
                       f"required key {output_value}"
                    )
                code_object[output_value] = code_map[output_value]
            object_output_with_code = {**object_output, **code_object}
            output_list.append(object_output_with_code)
    else:
        output_list.append(object_output)
    return output_list


def _format_column_metadata(node: typing.Dict, column_name: str) -> typing.Dict:
    """Extract the relevant fields from a column in a dbt node object by pulling
    REQUIRED_INPUT_FIELDS from the metadata and imputing the values for
    LINEAGE_OUTPUT_FIELDS."""
    object_output = {}
    column_metadata = node["columns"][column_name]["meta"]
    for field in REQUIRED_INPUT_FIELDS:
        if field not in column_metadata:
            raise ValueError(
                f"Column {node['name']} is "
                f"missing required meta attribute '{field}'"
            )
        object_output[field] = column_metadata[field]
    return object_output


if __name__ == "__main__":
    main()
