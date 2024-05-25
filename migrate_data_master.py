import os
import logging
import datetime
import shutil
from pyspark.sql import SparkSession

# Configuration and environment setup
def load_config(config_path="config.yaml"):
    import yaml
    with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)

def setup_logging(log_path="migration.log"):
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    return logging.getLogger('migration')

def get_spark():
    return SparkSession.builder \
        .master("yarn") \
        .appName("data-migration") \
        .config("spark.yarn.queue", "root.prod") \
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/opt/conda/anaconda3/bin/python") \
        .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/conda/anaconda3/bin/python") \
        .config("spark.ui.enabled", "false") \
        .config("spark.debug.maxToStringFields", 1000) \
        .config("spark.executor.memoryOverhead", 2500) \
        .config("spark.hadoop.fs.permissions.umask-mode", "022") \
        .config("spark.driver.memory", "4G") \
        .config("spark.driver.memoryOverhead", "4G") \
        .config("spark.executor.memory", "16G") \
        .config("spark.executor.memoryOverhead", "2G") \
        .getOrCreate()

# Utility functions for SQL and data handling
def run_sql_for_row(log, conn, sql):
    try:
        log.info("Executing SQL: {}".format(sql))
        rows = conn.execute(sql, timeout=300)
        for row in rows:
            return row[0]
        return None
    except Exception as e:
        log.error("Error executing query {}: {}".format(sql, e))
        raise e

def check_row_expected(log, rows, idx, expected):
    try:
        for row in rows:
            log.info(row)
            if row[idx].lower() != expected.lower():
                return False
        return True
    except Exception as e:
        log.error("Row check failed: {}".format(e))
        raise e

def get_row_count(log, rows, idx):
    try:
        for row in rows:
            return int(row[idx])
    except Exception as e:
        log.error("Failed to get row count: {}".format(e))
        raise e

def get_columns_for_parquet(log, conn, table):
    try:
        rows = conn.execute("DESCRIBE TABLE {}".format(table))
        columns = ", ".join([row[0] for row in rows])
        return columns
    except Exception as e:
        log.error("Error getting columns for table {}: {}".format(table, e))
        raise e

# Migration function
def migrate_table(lock, engine, hdfs, folder, completed_log_file, failed_log_file):
    table = folder.strip("/").split('/')[-1].lower()
    log.info("Processing folder: {}".format(folder))

    part_years = hdfs_list_by_time(hdfs, folder)
    for part_year in part_years:
        try:
            year_segment = part_year.split('/')[-1]
            year = int(year_segment.split('=')[-1]) if '=' in year_segment else int(year_segment)
        except Exception as e:
            log.warn("Skipping invalid year partition {}: {}".format(part_year, e))
            continue

        part_months = hdfs_list(hdfs, part_year)
        for part_month in part_months:
            try:
                month_segment = part_month.split('/')[-1]
                month = int(month_segment.split('=')[-1]) if '=' in month_segment else int(month_segment)
            except Exception as e:
                log.warn("Skipping invalid month partition {}: {}".format(part_month, e))
                continue

            part_days = hdfs_list(hdfs, part_month)
            for part_day in part_days:
                try:
                    day_segment = part_day.split('/')[-1]
                    day = int(day_segment.split('=')[-1]) if '=' in day_segment else int(day_segment)

                    ing_date = datetime.date(year=year, month=month, day=day)
                    start_date = datetime.datetime.strptime(params['start_date'], "%Y-%m-%d").date()
                    end_date = datetime.datetime.strptime(params['end_date'], "%Y-%m-%d").date()

                    if ing_date < start_date or ing_date > end_date:
                        continue

                    log.info("Processing table={}, year={}, month={}, day={}, ing_date={}".format(
                        table, year, month, day, ing_date))

                    local_dir = "/tmp/{}_{}_{}".format(table, year, month)
                    os.makedirs(local_dir, exist_ok=True)
                    log.info("Copying Parquet files from HDFS {} to local {}".format(part_day, local_dir))

                    parquet_files = hdfs_list_by_time(hdfs, part_day)
                    if not parquet_files:
                        log.warn("Skipping empty partition: {}".format(part_day))
                        continue

                    for parquet_file in parquet_files:
                        local_file_path = os.path.join(local_dir, os.path.basename(parquet_file))
                        with open(local_file_path, 'wb') as output_file:
                            hdfs.download(parquet_file, output_file)

                    stage = "stage_{}".format(table)
                    copy_to_snowflake(lock, log, conn, local_dir, database, schema, table, stage, ing_date, params)

                    completed_log_file.write("Completed: table={}, folder={}, partition={}_{}\n".format(
                        table, part_day, year, month))
                    completed_log_file.flush()
                except Exception as e:
                    failed_log_file.write("Failed: table={}, folder={}, partition={}_{}\n".format(
                        table, part_day, year, month))
                    failed_log_file.flush()
                    log.error("Error processing partition {}: {}".format(part_day, e))
                finally:
                    log.info("Removing local directory: {}".format(local_dir))
                    shutil.rmtree(local_dir)



"""Explanation:

Configuration and Environment Setup:

load_config: Loads configuration from a YAML file.
setup_logging: Sets up logging with the specified log file path.
get_spark: Initializes and returns a Spark session with specific configurations.
Utility Functions:

run_sql_for_row: Executes a SQL query and returns the first row.
check_row_expected: Checks if a specific row matches an expected value.
get_row_count: Returns the row count from a query result.
get_columns_for_parquet: Retrieves columns for a given table.
Migration Function:

migrate_table: Main function to handle data migration for year, month, and day partitions.
Logs processing details, downloads Parquet files, and calls copy_to_snowflake to copy data to Snowflake.
Ensures proper logging and error handling."""