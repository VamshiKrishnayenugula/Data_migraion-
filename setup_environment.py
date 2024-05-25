import os
import findspark
from pyspark.sql import SparkSession

# Initialize Spark
findspark.init("/opt/cloudera/parcels/CDH/lib/spark/")

# Set environment variables
os.environ["HADOOP_HOME"] = "/opt/cloudera/parcels/CDH/lib/hadoop"
os.environ["ARROW_LIBHDFS_DIR"] = "/opt/cloudera/parcels/CDH/lib/hdfs"

def get_spark():
    return SparkSession.builder \
        .master("yarn") \
        .appName("fix-schema") \
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


"""Explanation:

Imports:

os: For setting environment variables.
findspark: To initialize Spark.
SparkSession from pyspark.sql: To create a Spark session.
Initialization:

findspark.init("/opt/cloudera/parcels/CDH/lib/spark/"): Initializes Spark with the specified installation path.
Environment Variables:

os.environ["HADOOP_HOME"]: Sets the Hadoop home directory.
os.environ["ARROW_LIBHDFS_DIR"]: Sets the Arrow HDFS directory.
get_spark() Function:

Creates and returns a Spark session with specific configurations for memory, queue, and environment settings.
master("yarn"): Specifies YARN as the cluster manager.
appName("fix-schema"): Sets the application name.
Various .config() calls: Configure the Spark session settings, including memory and environment variables."""