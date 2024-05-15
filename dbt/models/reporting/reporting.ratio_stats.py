# pylint: skip-file
def model(dbt, spark_session):
    dbt.config(materialized="table")

    # Load dependency bundle so that we can import deps
    python_model_dependency = dbt.ref("ccao.python_model_dependency")
    s3_dependency_dir = python_model_dependency.first()["s3_dependency_dir"]
    # type: ignore
    sc.addPyFile(  # noqa: F821
        f"{s3_dependency_dir}/reporting.ratio_stats.requirements.zip"
    )

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
