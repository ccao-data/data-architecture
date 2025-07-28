import logging
import os

import watchtower


def create_cloudwatch_logger(
    name: str, log_file_path: str, log_group_name: str, stream_name: str
) -> logging.Logger:
    """
    Sets up a logger that can send output to CloudWatch.

    Args:
        name: Module name to use for the logger.
        log_file_path: String path to the log file where logs will be written.
        log_group_name: String name for CloudWatch log group.
        stream_name: String name for log group stream name.

    Returns:
        logging.Logger: Generic logger with CloudWatch handling.
    """

    # Formatter class to change WARNING to WARN for consistency with Spark
    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    # Create and start the logger, which will log to CloudWatch
    cw_handler = watchtower.CloudWatchLogHandler(
        log_group_name=log_group_name,
        stream_name=stream_name,
    )

    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S.%f",
    )
    logger = logging.getLogger(name)
    logger.addHandler(cw_handler)

    return logger
