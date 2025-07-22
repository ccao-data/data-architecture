import logging

PATH_MODELS_LOG = "/tmp/logs/models.log"


def create_python_logger(
    name: str, log_file_path: str = PATH_MODELS_LOG
) -> logging.Logger:
    """
    Sets up a logger with the same output format and location as the primary
    Spark logger from the JVM. Also used as a fallback in case any part of the
    main job loop fails.

    Args:
        name: Module name to use for the logger.
        log_file_path: String path to the log file where logs will be written.

    Returns:
        logging.Logger: Generic logger with the same log format as Spark.
    """

    # Formatter class to change WARNING to WARN for consistency with Spark
    class CustomFormatter(logging.Formatter):
        def format(self, record):
            if record.levelname == "WARNING":
                record.levelname = "WARN"
            return super().format(record)

    file_formatter = CustomFormatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S",
    )
    stdout_formatter = CustomFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%d/%m/%y %H:%M:%S",
    )

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setFormatter(file_formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stdout_formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
