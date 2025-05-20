import functools

from pyspark.sql.functions import lit


def model(dbt, session):
    # configure incremental insert-overwrite by run_id
    dbt.config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partitions_by=[{"field": "run_id", "data_type": "string"}],
        unique_key="run_id",
        on_schema_change="append_new_columns",
    )

    # Collect existing run_ids
    if dbt.is_incremental:
        existing_rows = session.sql(
            f"SELECT DISTINCT run_id FROM {dbt.this.schema}.{dbt.this.identifier}"
        ).collect()
        existing_run_ids = [row["run_id"] for row in existing_rows]
    else:
        existing_run_ids = []

    # Build metadata query, excluding already-loaded run_ids
    base_query = """
        SELECT
          CAST(run_id                  AS STRING) AS run_id,
          CAST(year                    AS STRING) AS year,
          CAST(dvc_md5_assessment_data AS STRING) AS dvc_md5_assessment_data
        FROM model.metadata
        WHERE run_type = 'final'
    """
    if existing_run_ids:
        quoted = ",".join(f"'{rid}'" for rid in existing_run_ids)
        base_query += f" AND run_id NOT IN ({quoted})"

    metadata = session.sql(base_query).toPandas()

    # If no new runs, just return the existing table
    if metadata.empty:
        print(">>> no new run_id found; returning existing data unchanged")
        return session.sql(
            f"SELECT * FROM {dbt.this.schema}.{dbt.this.identifier}"
        )

    bucket = "ccao-data-dvc-us-east-1"
    all_dfs = []

    for _, row in metadata.iterrows():
        run_id = row["run_id"]
        year = int(row["year"])
        dvc_hash = row["dvc_md5_assessment_data"]

        # for post-2023 we have the extra files/md5/ prefix
        prefix = "" if year <= 2023 else "files/md5/"
        key = f"{prefix}{dvc_hash[:2]}/{dvc_hash[2:]}"
        s3_path = f"{bucket}/{key}"

        print(f">>> reading all columns for run {run_id!r}")
        print(f"    →   S3 key = {s3_path}")
        df = session.read.parquet(f"s3://{s3_path}")

        # coerce booleans for data with different types
        if "ccao_is_active_exe_homeowner" in df.columns:
            df = df.withColumn(
                "ccao_is_active_exe_homeowner",
                df["ccao_is_active_exe_homeowner"].cast("boolean"),
            )

        # add run_id column
        df = df.withColumn("run_id", lit(run_id))

        all_dfs.append(df)
        print(f"Processed run_id={run_id}, rows={df.count()}")

    # Union all the new runs together
    return functools.reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True), all_dfs
    )
