import sys
import traceback

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext


def get_optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    for i, token in enumerate(sys.argv):
        if token == flag and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def log(msg: str):
    print(msg, flush=True)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

REDSHIFT_CONNECTION_NAME = get_optional_arg("REDSHIFT_CONNECTION_NAME", "Redshift connection")
REDSHIFT_DATABASE = get_optional_arg("REDSHIFT_DATABASE", "dev")
REDSHIFT_SCHEMA = get_optional_arg("REDSHIFT_SCHEMA", "public")
REDSHIFT_TMP_DIR = get_optional_arg("REDSHIFT_TMP_DIR", "s3://readydoc-ravi-local/redshift-temp/")
TARGET_TABLE = get_optional_arg("TARGET_TABLE", "glue_direct_test")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

try:
    log("DEBUG DIRECT REDSHIFT TEST STARTED")

    data = [
        (1, "test row 1"),
        (2, "test row 2"),
        (3, "test row 3")
    ]

    df = spark.createDataFrame(data, ["id", "message"])
    log("Spark DataFrame created")

    df.show(10, False)
    log(f"Row count = {df.count()}")

    dyf = DynamicFrame.fromDF(df, glue_context, "dyf_test")
    log("DynamicFrame created")

    preactions = f"""
    DROP TABLE IF EXISTS {REDSHIFT_SCHEMA}.{TARGET_TABLE};
    CREATE TABLE {REDSHIFT_SCHEMA}.{TARGET_TABLE} (
        id INTEGER,
        message VARCHAR(255)
    );
    """

    log(f"About to write to Redshift table {REDSHIFT_SCHEMA}.{TARGET_TABLE}")
    log(f"Using temp dir: {REDSHIFT_TMP_DIR}")

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=REDSHIFT_CONNECTION_NAME,
        connection_options={
            "dbtable": f"{REDSHIFT_SCHEMA}.{TARGET_TABLE}",
            "database": REDSHIFT_DATABASE,
            "preactions": preactions
        },
        redshift_tmp_dir=REDSHIFT_TMP_DIR,
        transformation_ctx="redshift_direct_test_sink"
    )

    log("Redshift write finished successfully")

except Exception as e:
    log(f"ERROR: {str(e)}")
    log(traceback.format_exc())
    raise

job.commit()
log("JOB COMMITTED")