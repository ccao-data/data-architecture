import logging
import os
from datetime import date

import watchtower


def create_logger(
    name: str,
    log_file_path: str,
    log_group_name: str | None = None,
    stream_name: str | None = None,
) -> logging.Logger:
    """
    Sets up a logger that can optionally send output to CloudWatch.

    Args:
        name: Module name to use for the logger.
        log_file_path: String path to the local log file where logs will be written.
        log_group_name: String name for CloudWatch log group.
        stream_name: Optional string name for log group stream. Defaults to today's date.

    Returns:
        logging.Logger: Generic logger with optional CloudWatch handling.
    """

    # Formatter class to change WARNING to WARN for consistency with Spark
    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    # Create and start the logger
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S.%f",
        force=True,
    )
    logger = logging.getLogger(name)

    # Log to CloudWatch if desired
    if log_group_name is not None:
        stream_name = stream_name or f"{date.today()}"

        cw_handler = watchtower.CloudWatchLogHandler(
            log_group_name=log_group_name,
            stream_name=stream_name,
        )
        logger.addHandler(cw_handler)

    # Add console handler for local debugging regardless of CloudWatch logging
    logger.addHandler(logging.StreamHandler())

    return logger
