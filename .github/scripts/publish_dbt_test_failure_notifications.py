#!/usr/bin/env python3
# Publish one or more dbt test failure notifications based on the path to
# a metadata file produced by the run_iasworld_data_tests.py script.
#
# We expect the metadata file to be a JSON array of objects with a schema
# like so:
#
# [
#   {
#     "topic_arn": "arn:aws:sns::...",
#     "subject": "Email subject",
#     "message": "Email body"
#   }
# ]
#
# The script will raise an error if any objects in the array do not match
# this schema.
import json
import sys

import boto3

if __name__ == "__main__":
    filepath = sys.argv[1]
    client = boto3.client("sns")

    with open(filepath) as fobj:
        failure_notifications = json.load(fobj)

    # Validate the structure of the failure notifications metadata file
    required_keys = ["topic_arn", "subject", "message"]
    for idx, notification in enumerate(failure_notifications):
        for key in required_keys:
            if key not in notification:
                raise ValueError(
                    f"Missing required key '{key}' in object with index {idx} "
                    f"in {filepath}"
                )

    # Publish failure notifications
    for notification in failure_notifications:
        client.publish(
            TopicArn=notification["topic_arn"],
            Subject=notification["subject"],
            Message=notification["message"],
        )
