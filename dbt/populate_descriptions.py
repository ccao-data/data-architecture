#!/usr/bin/env python3
import csv
import sys
import os

if __name__ == "__main__":
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        fq_name = row["var_from_view"]
        schema, model_name = fq_name.split(".")

        schema_filepath = os.path.join("models", schema, "schema.yml")
        with open(schema_filepath) as schema_fobj:
            schema_file_str = schema_fobj.read()

        str_to_replace = f"- name: {fq_name}\n"

        if str_to_replace not in schema_file_str:
            raise ValueError(
                f"Model {fq_name} is missing expected model definition: "
                f"{str_to_replace}"
            )

        schema_file_str_with_description = schema_file_str.replace(
            str_to_replace,
            (
                f"{str_to_replace}"
                "    columns:\n"
                f"      - name: {row['var_name_athena']}\n"
                f"        description: {row['var_notes']}\n"
            )
        )

        with open(schema_filepath, "w") as schema_fobj:
            schema_fobj.write(schema_file_str_with_description)
