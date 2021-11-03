from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import Row, DataFrame, SparkSession, Window, Row, SparkSession, Window
from pyspark import SparkContext, SparkConf, SQLContext
import boto3
import datetime
import sys
import yaml

with open("iasworld-daily-dump-config.yaml", 'r') as f:
    doc = yaml.safe_load(f)

file_location_dict = doc['File_Locations']
fact_dict = doc['Fact_Tables']
dimension_dict = doc['Dimension_Tables']

s3 = boto3.resource('s3')
LANDING_BUCKET = file_location_dict['LANDING_BUCKET']
BUCKET = file_location_dict['STAGE_BUCKET']
LANDING_FOLDER = file_location_dict['LANDING_FOLDER']
STAGE_RAW_FOLDER = file_location_dict['STAGE_RAW_FOLDER']
STAGE_ARCHIVE_FOLDER = file_location_dict['STAGE_ARCHIVE_FOLDER']
STAGE_AUDIT_LOG_FOLDER = file_location_dict['STAGE_AUDIT_LOG_FOLDER']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
composite_keys = ""
client = boto3.client('s3')


def replace_file(table):
    s3_resource = boto3.resource('s3')
    old_bucket = s3_resource.Bucket(BUCKET)
    new_bucket = s3_resource.Bucket(BUCKET)
    old_bucket_name = BUCKET
    old_prefix = STAGE_RAW_FOLDER + "_temp_upsert"
    new_bucket_name = BUCKET
    new_prefix = STAGE_RAW_FOLDER + "/" + table
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=new_prefix)
    try:
        for object in response['Contents']:
            s3_client.delete_object(Bucket=BUCKET, Key=object['Key'])
        for obj in old_bucket.objects.filter(Prefix=old_prefix):
            old_source = {'Bucket': old_bucket_name,
                          'Key': obj.key}
            # replace the prefix
            new_key = obj.key.replace(old_prefix, new_prefix)
            new_obj = new_bucket.Object(new_key)
            new_obj.copy(old_source)
            obj.delete()
    except Exception as e:
        print("error with replacing file")



def etl_fact_tables():
    if fact_dict:
        for table, keys1 in fact_dict.items():
            composite_keys = [x.strip() for x in keys1.split(',')]
            print("fact ", table, " : ", composite_keys)
            read_parquet_path = "s3://" + BUCKET + "/" + STAGE_RAW_FOLDER + "/" + table + "/"
            read_input_path = "s3://" + LANDING_BUCKET + "/" + LANDING_FOLDER + "/" + table + "/"
            write_parquet_raw_path = "s3a://" + BUCKET + "/" + STAGE_RAW_FOLDER + "/" + table
            write_parquet_upsert_temp_path = "s3a://" + BUCKET + "/" + STAGE_RAW_FOLDER + "_temp_upsert"
            write_parquet_archive_prefix_path = "s3a://" + BUCKET + "/" + STAGE_ARCHIVE_FOLDER + "/" + table
            write_parquet_audit_log_prefix_path = "s3a://" + BUCKET + "/" + STAGE_AUDIT_LOG_FOLDER + "/" + table
            try:
                df_new = spark.read.option("header", "true").parquet(read_input_path)
                print("read new file: ", read_input_path)
            except Exception as e:
                print("error: ", e)
            else:
                try:
                    df_old = spark.read.option("header", "true").parquet(read_parquet_path)
                    print("succcesfully read old file")
                except Exception as e:
                    print("error reading old table write new: ", table, "\n error: ", str(e))
                    try:
                        if ("TAXYR" in df_new.columns):
                            df_new.write.partitionBy("TAXYR").option("header", "true").parquet(write_parquet_raw_path,
                                                                                               mode="overwrite")
                        else:
                            df_new.write.option("header", "true").parquet(write_parquet_raw_path, mode="overwrite")

                        now = datetime.datetime.now()
                        df_new_archive = df_new.withColumn("year", year(F.current_date())).withColumn("month", month(
                                F.current_date())).withColumn("day", dayofmonth(F.current_date())) \
                                .withColumn("hour", hour(F.current_date())).withColumn("minute",
                                                                                       minute(F.current_date())).withColumn(
                                "second", second(F.current_date()))
                        df_new_archive.write.partitionBy("year", "month", "day", "hour", "minute", "second").option(
                                "header", "true").parquet(write_parquet_archive_prefix_path, mode="append")
                        upload_time = datetime.datetime.now()
                        num_source_records = str(df_new.count())
                        num_old_records = "0"
                        num_new_records = num_source_records
                        num_duplicate_records = "0"
                        timestampStr = upload_time.strftime("%d-%b-%Y (%H:%M:%S)")
                        deptDF = sc.parallelize([[table, num_source_records, num_old_records, num_new_records,
                                                      num_duplicate_records, upload_time]]).toDF(("table_name",
                                                                                                  "num_source_records",
                                                                                                  "num_old_records",
                                                                                                  "num_new_records",
                                                                                                  "num_duplicate_records",
                                                                                                  "upload_time"))
                        deptDF.write.option("header", "true").parquet(write_parquet_audit_log_prefix_path, mode="append")
                        bucket = s3.Bucket(LANDING_BUCKET)
                        bucket.objects.filter(Prefix=LANDING_FOLDER + "/" + table).delete()
                        print("deleted table path after writing new : ", LANDING_FOLDER + "/" + table + "/")
                    except Exception as e:
                        print("error writing new with table: ", table, " error: ", str(e))
                else:
                    try:
                        # archive data
                        df_new_archive = df_new.withColumn("year", year(F.current_date())).withColumn("month", month(
                            F.current_date())).withColumn("day", dayofmonth(F.current_date())) \
                            .withColumn("hour", hour(F.current_date())).withColumn("minute",
                                                                                   minute(F.current_date())).withColumn(
                            "second", second(F.current_date()))
                        df_new_archive.write.partitionBy("year", "month", "day", "hour", "minute", "second").option(
                            "header", "true").parquet(write_parquet_archive_prefix_path, mode="append")
                        num_source_records = str(df_new.count())
                        num_old_records = str(df_old.count())
                        num_new_records = str(df_new.join(df_old, df_new.columns, "left_anti").count())
                        num_duplicate_records = str(df_new.join(df_old, df_new.columns, "inner").count())
                        # order column
                        df_old = df_old.select(df_new.columns)
                        # add column for new,old
                        df_new = df_new.withColumn("file_status", F.current_timestamp())
                        df_old = df_old.withColumn("file_status", F.current_timestamp() - expr("INTERVAL 1 DAY"))
                        # union old and new
                        df_merge = df_old.union(df_new).dropDuplicates(df_new.columns)
                        df_upsert = df_merge.withColumn("rn", F.row_number().over(
                            Window.partitionBy(composite_keys).orderBy(F.col("file_status").desc())))
                        # drop old rows that are being updated
                        # log date,table_name,num_records, duplicates,number of new rows, number rejected
                        df_upsert = df_upsert.filter(F.col("rn") == 1).drop("rn")
                        now = datetime.datetime.now()
                        dt = str(now.strftime("%Y_%m_%d_%H_%M_%S"))
                        upload_time = datetime.datetime.now()
                        timestampStr = upload_time.strftime("%d-%b-%Y (%H:%M:%S)")
                        df_upsert.write.partitionBy("TAXYR").option("header", "true").parquet(write_parquet_upsert_temp_path,
                                                                                               mode="overwrite")
                        # replace file
                        replace_file(table)
                        deptDF = sc.parallelize([[table, num_source_records, num_old_records, num_new_records,
                                                  num_duplicate_records, upload_time]]).toDF(("table_name",
                                                                                              "num_source_records",
                                                                                              "num_old_records",
                                                                                              "num_new_records",
                                                                                              "num_duplicate_records",
                                                                                              "upload_time"))
                        deptDF.write.option("header", "true").parquet(write_parquet_audit_log_prefix_path, mode="append")
                        bucket = s3.Bucket(LANDING_BUCKET)
                        bucket.objects.filter(Prefix=LANDING_FOLDER + "/" + table).delete()
                        print("deleted table path after update : ", LANDING_FOLDER + "/" + table + "/")
                    except Exception as e:
                        print("error with table: ", table, " error: ", str(e))


def etl_dimension_tables():
    if dimension_dict:
        for table, keys1 in dimension_dict.items():
            composite_keys = [x.strip() for x in keys1.split(',')]
            print(table, " : ", composite_keys)
            read_parquet_path = "s3a://" + BUCKET + "/" + STAGE_RAW_FOLDER + "/" + table + "/"
            read_input_path = "s3a://" + LANDING_BUCKET + "/" + LANDING_FOLDER + "/" + table + "/"
            write_parquet_raw_path = "s3a://" + BUCKET + "/" + STAGE_RAW_FOLDER + "/" + table
            write_parquet_upsert_temp_path = "s3a://" + BUCKET + "/" + STAGE_RAW_FOLDER + "_temp_upsert"
            write_parquet_archive_prefix_path = "s3a://" + BUCKET + "/" + STAGE_ARCHIVE_FOLDER + "/" + table
            write_parquet_audit_log_prefix_path = "s3a://" + BUCKET + "/" + STAGE_AUDIT_LOG_FOLDER + "/" + table
            try:
                df_new = spark.read.option("header", "true").parquet(read_input_path)
                print("read new file: ", read_input_path)
            except Exception as e:
                print("error: ", e)
            else:
                try:
                    df_old = spark.read.option("header", "true").parquet(read_parquet_path)
                    print("succesfully read old file: ", read_parquet_path)
                except Exception as e:
                    print("error reading old table write new: ", table, "\n error: ", str(e))
                    try:
                        current_date = date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss')
                        df_new = df_new.withColumn('start_date', lit('1999-12-31 23:59:59')).withColumn('end_date', lit(""))
                        df_new.write.option("header", "true").parquet(write_parquet_raw_path, mode="overwrite")
                        df_new_archive = df_new.withColumn("year", year(F.current_date())).withColumn("month", month(
                            F.current_date())).withColumn("day", dayofmonth(F.current_date())).withColumn("hour", hour(
                            F.current_timestamp())).withColumn("minute", minute(F.current_timestamp())).withColumn("second",
                                                                                                                   second(
                                                                                                                       F.current_timestamp()))
                        df_new_archive.write.partitionBy("year","month","day","hour","minute","second").option("header","true").parquet(write_parquet_archive_prefix_path,mode="append")
                        num_source_records = str(df_new.count())
                        upload_time = datetime.datetime.now()
                        num_old_records = "0"
                        num_new_records = num_source_records
                        num_duplicate_records = "0"
                        timestampStr = upload_time.strftime("%d-%b-%Y (%H:%M:%S)")
                        deptDF = sc.parallelize([[table, num_source_records, num_old_records, num_new_records,
                                                  num_duplicate_records, upload_time]]).toDF(("table_name",
                                                                                              "num_source_records",
                                                                                              "num_old_records",
                                                                                              "num_new_records",
                                                                                              "num_duplicate_records",
                                                                                              "upload_time"))
                        deptDF.write.option("header", "true").parquet(write_parquet_audit_log_prefix_path, mode="append")
                        bucket = s3.Bucket(LANDING_BUCKET)
                        bucket.objects.filter(Prefix=LANDING_FOLDER + "/" + table).delete()
                        print("deleted table path after writing new : ", LANDING_FOLDER + "/" + table + "/")
                    except Exception as e:
                        print("error writing new with table: ", table, " error: ", str(e))
                else:
                    try:
                        df_new_archive = df_new.withColumn("year", year(F.current_date())).withColumn("month", month(
                            F.current_date())).withColumn("day", dayofmonth(F.current_date())) \
                            .withColumn("hour", hour(F.current_timestamp())).withColumn("minute", minute(
                            F.current_timestamp())).withColumn("second", second(F.current_timestamp()))
                        df_new_archive.write.partitionBy("year", "month", "day", "hour", "minute", "second").option(
                            "header", "true").parquet(write_parquet_archive_prefix_path, mode="append")
                        # get audit info
                        num_source_records = str(df_new.count())
                        num_old_records = str(df_old.count())
                        num_new_records = str(df_new.join(df_old, df_new.columns, "left_anti").count())
                        num_duplicate_records = str(df_new.join(df_old, df_new.columns, "inner").count())
                        # order column
                        column_names = df_new.columns
                        current_date = date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss')
                        df_new = df_new.join(df_old, column_names, "left_anti")
                        df_new = df_new.withColumn('start_date', lit(current_date)).withColumn('end_date', lit(""))
                        # add column for new,old
                        df_new = df_new.withColumn("file_status", F.current_timestamp())
                        df_old = df_old.withColumn("file_status", F.current_timestamp() - expr("INTERVAL 1 DAY"))
                        # drop duplicates and union old data with new data
                        df_merge = df_old.union(df_new).dropDuplicates(column_names)
                        df_upsert = df_merge.withColumn("rn", F.row_number().over(
                            Window.partitionBy(composite_keys).orderBy(desc("start_date"), desc("end_date"))))
                        # add end date column and switch dates if not current
                        current_date = date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss')
                        df_old_upsert = df_upsert.withColumn("end_date", when(df_upsert.rn == 2, current_date).otherwise(
                            df_upsert.end_date))
                        df_old_upsert = df_old_upsert.filter(F.col("rn") != 1).drop("rn")
                        df_old_upsert = df_old_upsert.drop("file_status")
                        # drop old rows that are being updated
                        df_new_upsert = df_upsert.filter(F.col("rn") == 1).drop("rn")
                        df_new_upsert = df_new_upsert.drop("file_status")
                        # finalize and write to temp path with parquet, then replace file
                        # (because of lazy writing the input data is split between nodes.
                        # so Pypspark would be overwriting and reading from the same file)
                        df_final = df_old_upsert.union(df_new_upsert).dropDuplicates(column_names)
                        df3 = df_final.coalesce(1)
                        df3.write.option("header", "true").parquet(write_parquet_upsert_temp_path, mode="overwrite")
                        replace_file(table)
                        upload_time = datetime.datetime.now()
                        timestampStr = upload_time.strftime("%d-%b-%Y (%H:%M:%S)")
                        # write to audit log
                        deptDF = sc.parallelize([[table, num_source_records, num_old_records, num_new_records,
                                                  num_duplicate_records, upload_time]]).toDF(("table_name",
                                                                                              "num_source_records",
                                                                                              "num_old_records",
                                                                                              "num_new_records",
                                                                                              "num_duplicate_records",
                                                                                              "upload_time"))
                        deptDF.write.option("header", "true").parquet(write_parquet_audit_log_prefix_path, mode="append")
                        bucket = s3.Bucket(LANDING_BUCKET)
                        bucket.objects.filter(Prefix=LANDING_FOLDER + "/" + table).delete()
                        print("deleted table path after update : ", LANDING_FOLDER + "/" + table + "/")
                    except Exception as e:
                        print("error with table: ", table, " error: ", str(e))


def move_from_landing_to_stage():
    print("running etl")
    etl_fact_tables()
    etl_dimension_tables()


if __name__ == '__main__':
    move_from_landing_to_stage()
    print(" ")
    print("\n done")
