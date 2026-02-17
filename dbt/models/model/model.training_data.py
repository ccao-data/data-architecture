import functools

from pyspark.sql.functions import lit


def model(dbt, session):
    dbt.config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partitioned_by=["assessment_year", "run_id", "meta_township_code"],
        on_schema_change="append_new_columns",
    )

    # Get model metadata for every final model. We do this by inner joining
    # The `metadata` table to the `final_model` table instead of filtering
    # the metadata table by `run_type == 'final'` to make it easier to run
    # tests on this table, since we can control the contents of `final_model`
    # via a dbt seed
    metadata_df = (
        dbt.source("model", "metadata")
        .join(
            dbt.ref("model.final_model").select("run_id"),
            on="run_id",
            how="inner",
        )
        .select("run_id", "year", "assessment_year", "dvc_md5_training_data")
    )

    if dbt.is_incremental:
        # anti-join out any run_ids already in the target
        existing = (
            session.table(f"{dbt.this.schema}.{dbt.this.identifier}")
            .select("run_id")
            .distinct()
        )
        metadata_df = metadata_df.join(existing, on="run_id", how="left_anti")

        # if there’s nothing new, return an *empty* DataFrame
        if metadata_df.limit(1).count() == 0:
            print(">>> no new run_id found; skipping incremental update")
            # this returns zero rows but preserves the full target schema
            return session.table(
                f"{dbt.this.schema}.{dbt.this.identifier}"
            ).limit(0)

    # Collect remaining metadata
    metadata = metadata_df.toPandas()

    bucket = "ccao-data-dvc-us-east-1"
    all_dfs = []

    for _, row in metadata.iterrows():
        run_id = row["run_id"]
        year = int(row["year"])
        h = row["dvc_md5_training_data"]

        prefix = "" if year <= 2023 else "files/md5/"
        key = f"{prefix}{h[:2]}/{h[2:]}"
        s3p = f"{bucket}/{key}"

        print(f">>> reading all columns for run {run_id!r}")
        print(f"    →   S3 key = {s3p}")
        df = session.read.parquet(f"s3://{s3p}")

        # coerce booleans for mismatched types
        if "ccao_is_active_exe_homeowner" in df.columns:
            df = df.withColumn(
                "ccao_is_active_exe_homeowner",
                df["ccao_is_active_exe_homeowner"].cast("boolean"),
            )

        # add run_id and assessment_year columns
        df = df.withColumn("run_id", lit(run_id)).withColumn(
            "assessment_year", lit(row["assessment_year"])
        )

        all_dfs.append(df)
        print(f"Processed run_id={run_id}, rows={df.count()}")

    # Union all the new runs together
    return functools.reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True), all_dfs
    )
