import os
import time

import boto3


def upload_logs_to_cloudwatch(
    log_group_name: str, log_stream_name: str, log_file_path: str
) -> None:
    """
    Uploads a log file to a specified CloudWatch log group.

    Args:
        log_group_name: The name of the CloudWatch log group.
        log_stream_name: The name of the CloudWatch log stream to write to.
        log_file_path: The path to the log file to upload.
    """
    try:
        with open(log_file_path, "r") as log_file:
            log_events = []
            for line in log_file:
                log_events.append(
                    {
                        "timestamp": int(time.time() * 1000),
                        "message": line.strip(),
                    }
                )

        try:
            boto3.client("logs").create_log_stream(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
            )
        except boto3.client("logs").exceptions.ResourceAlreadyExistsException:
            pass

        boto3.client("logs").put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=log_events,
        )
        print("Successfully uploaded log file to CloudWatch")

        # Remove the log file after successful upload
        os.remove(log_file_path)
        print(f"Successfully removed log file: {log_file_path}")

    except Exception as e:
        print(f"Failed to upload log file to CloudWatch: {e}")
