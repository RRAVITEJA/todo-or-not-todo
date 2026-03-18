import sys
import traceback
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

try:
    print("Testing RDS read...")

    rds_test = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": "Postgresql connection2",
            "dbtable": "readydoc.provider_profile"
        },
        transformation_ctx="rds_test"
    )

    print("RDS read successful, sample count:", rds_test.toDF().limit(1).count())

    print("Testing Redshift write...")

    df = spark.createDataFrame([
        (1, "hello"),
        (2, "world")
    ], ["id", "message"])

    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="Redshift connection",
        connection_options={
            "dbtable": "public.glue_redshift_test",
            "database": "dev",
            "preactions": """
                CREATE TABLE IF NOT EXISTS public.glue_redshift_test (
                    id INT,
                    message VARCHAR(255)
                );
            """
        },
        redshift_tmp_dir="s3://aws-glue-assets-058264370635-us-east-2/temporary/"
    )

    print("SUCCESS: RDS read and Redshift write both worked")

except Exception as e:
    print("ERROR:", str(e))
    print(traceback.format_exc())
    raise

job.commit()