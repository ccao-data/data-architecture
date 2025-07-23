import os
import time

import boto3


class AWSClient:
    def __init__(
        self, s3_bucket: str | None = None, s3_prefix: str | None = None
    ):
        """
        Class to store AWS clients and methods for various AWS actions. Clients
        are instantiated from AWS credentials passed via Compose secrets.

        Args:
            s3_bucket: S3 bucket to upload to. Overrides the equivalent
                environmental variable.
            s3_prefix: S3 path prefix within S3 bucket. Overrides the equivalent
                environmental variable.

        Attributes:
            logs_client: Glue client connection for running crawlers.
            glue_client: CloudWatch logs connection for uploading logs.
            s3_client: S3 client connection for uploading extracted files.
            s3_bucket: S3 bucket to upload extracts to.
            s3_prefix: S3 path prefix within S3 bucket. Defaults to "iasworld".
        """
        self.logs_client = boto3.client("logs")
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")
        self.s3_bucket = s3_bucket if s3_bucket else os.getenv("AWS_S3_BUCKET")
        self.s3_prefix = (
            s3_prefix if s3_prefix else os.getenv("AWS_S3_PREFIX", "iasworld")
        )

    def upload_logs_to_cloudwatch(
        self, log_group_name: str, log_stream_name: str, log_file_path: str
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
                self.logs_client.create_log_stream(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                )
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                pass

            self.logs_client.put_log_events(
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
