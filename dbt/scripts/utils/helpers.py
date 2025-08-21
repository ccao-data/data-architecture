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

    # Remove the log file if it already exists
    if log_file_path is not None and os.path.exists(log_file_path):
        os.remove(log_file_path)

    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S",
    )

    # Create logger and add custom handlers
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Clear handlers in case we accidentally call this function multiple times
    logger.handlers.clear()

    # Always log to the console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Turn off log propagation to the root logger to avoid duplication with
    # the stream handler
    logger.propagate = False

    # Log to CloudWatch if desired
    if log_group_name is not None:
        stream_name = stream_name or f"{date.today()}"

        cw_handler = watchtower.CloudWatchLogHandler(
            log_group_name=log_group_name,
            stream_name=stream_name,
        )

        # Don't push timestamps to CloudWatch, since CloudWatch adds its own
        # timestamps
        cw_handler.setFormatter(
            logging.Formatter(fmt="%(levelname)s - %(message)s")
        )
        logger.addHandler(cw_handler)

    # Add a file handler to write logs to a local file if specified
    if log_file_path is not None:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
