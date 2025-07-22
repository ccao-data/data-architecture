import os
import time
from datetime import datetime

import boto3

from utils.helpers import create_python_logger

logger = create_python_logger(__name__)


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

    def run_and_wait_for_crawler(self, crawler_name) -> None:
        initial_response = self.glue_client.get_crawler(Name=crawler_name)
        if initial_response["Crawler"]["State"] == "READY":  # type: ignore
            logger.info(f"Starting AWS Glue crawler {crawler_name}")
            self.glue_client.start_crawler(Name=crawler_name)
        else:
            logger.warning(
                f"AWS Glue crawler {crawler_name} is already running"
            )
            return

        # Wait for the crawler to complete before triggering dbt tests
        time_elapsed = 0
        time_increment = 30
        timeout = 1200  # 20 minute timeout
        job_complete = False
        while time_elapsed < timeout:
            response = self.glue_client.get_crawler(Name=crawler_name)
            state = response["Crawler"]["State"]  # type: ignore
            if state in ["READY", "STOPPING"]:
                logger.info(f"Crawler {crawler_name} has finished")
                job_complete = True
                break
            elif state == "RUNNING":
                logger.info(
                    (
                        f"Crawler {crawler_name} is running: "
                        f"{time_elapsed}s elapsed"
                    )
                )
            time.sleep(time_increment)
            time_elapsed += time_increment
        if not job_complete:
            logger.warning(
                (
                    f"Crawler {crawler_name} was still running after the {timeout}s"
                    f"timeout; continuing execution without confirming its status"
                )
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
                log_events: list[dict[str, int | str]] = []
                for line in log_file:
                    timestamp_str, message = line.split(" ", 1)
                    timestamp: int = int(
                        (
                            datetime.strptime(
                                timestamp_str, "%Y-%m-%d_%H:%M:%S.%f"
                            ).timestamp()
                            * 1000
                        )
                    )
                    log_events.append(
                        {
                            "timestamp": timestamp,
                            "message": message.strip(),
                        }
                    )

            # Sort log events by timestamp
            log_events.sort(key=lambda event: event["timestamp"])

            # CloudWatch doesn't allow colon in stream names, so use a dash
            log_stream_name_fmt = log_stream_name.replace(":", "-")
            try:
                self.logs_client.create_log_stream(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name_fmt,
                )
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                pass

            self.logs_client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name_fmt,
                logEvents=log_events,
            )
            print("Successfully uploaded log file to CloudWatch")

            # Remove the log file after successful upload
            os.remove(log_file_path)
            print(f"Successfully removed log file: {log_file_path}")

        except Exception as e:
            print(f"Failed to upload log file to CloudWatch: {e}")
