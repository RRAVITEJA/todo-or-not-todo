import sys
import traceback

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

RDS_CONNECTION_NAME = "Postgresql connection"
REDSHIFT_CONNECTION_NAME = "Redshift connection"

RDS_TABLE = "readydoc.provider_profile"
REDSHIFT_TABLE = "public.glue_redshift_test"
REDSHIFT_DATABASE = "dev"
REDSHIFT_TMP_DIR = "s3://aws-glue-assets-058264370635-us-east-2/temporary/"


# ---------------------------------------------------------
# Setup
# ---------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def test_rds_read():
    print("=== Step 1: Testing RDS read ===")
    print(f"Connection: {RDS_CONNECTION_NAME}")
    print(f"Table: {RDS_TABLE}")

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": RDS_CONNECTION_NAME,
            "dbtable": RDS_TABLE,
        },
        transformation_ctx="rds_test",
    )

    df = dyf.toDF()
    row_count = df.limit(1).count()

    print(f"RDS read successful. Sample row check count = {row_count}")
    df.show(5, truncate=False)


def test_redshift_write():
    print("=== Step 2: Testing Redshift write ===")
    print(f"Connection: {REDSHIFT_CONNECTION_NAME}")
    print(f"Table: {REDSHIFT_TABLE}")

    test_df = spark.createDataFrame(
        [
            (1, "new"),
            (2, "world"),
        ],
        ["id", "message"],
    )

    test_df.show(truncate=False)

    test_dyf = DynamicFrame.fromDF(test_df, glue_context, "test_dyf")

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=test_dyf,
        catalog_connection=REDSHIFT_CONNECTION_NAME,
        connection_options={
            "dbtable": REDSHIFT_TABLE,
            "database": REDSHIFT_DATABASE,
            "preactions": f"""
                CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE} (
                    id INT,
                    message VARCHAR(255)
                );
            """,
        },
        redshift_tmp_dir=REDSHIFT_TMP_DIR,
    )

    print("Redshift write successful.")


def run_test(step_name, func):
    try:
        func()
        print(f"{step_name} PASSED\n")
    except Exception as e:
        print(f"{step_name} FAILED")
        print("Error message:", str(e))
        print(traceback.format_exc())
        raise


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
run_test("RDS READ TEST", test_rds_read)
run_test("REDSHIFT WRITE TEST", test_redshift_write)

print("SUCCESS: Both tests passed.")
job.commit()