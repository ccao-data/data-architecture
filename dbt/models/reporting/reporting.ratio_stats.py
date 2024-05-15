# pylint: skip-file

# Load dependency bundle so that we can import deps
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-dbt-athena-ci-us-east-1/dependencies/jeancochrane_439_refactor_reportingratio_stats_to_use_new_pattern_for_python_model_dependencies/reporting.ratio_stats.requirements.zip"  # noqa: E501
)


def model(dbt, spark_session):
    dbt.config(materialized="table")

    import numpy as np  # noqa: E402 F401
    import pandas as pd  # noqa: E402 F401
    from assesspy import boot_ci  # noqa: E402 F401
    from assesspy import cod  # noqa: E402 F401
    from assesspy import prb  # noqa: E402 F401
    from assesspy import prd_met  # noqa: E402 F401
    from assesspy import cod_ci as cod_boot  # noqa: E402 F401
    from assesspy import cod_met, mki, mki_met, prb_met, prd  # noqa: E402 F401
    from assesspy import prd_ci as prd_boot  # noqa: E402 F401

    spark_df = dbt.ref("reporting.ratio_stats_input")
    return spark_df
