import sys
import re
import traceback

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# ----------------------------------------------------------
# Job args / config
# ----------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

RDS_CONNECTION_NAME = "Postgresql connection"
REDSHIFT_CONNECTION_NAME = "Redshift connection"
REDSHIFT_DATABASE = "dev"
REDSHIFT_TMP_DIR = "s3://aws-glue-assets-058264370635-us-east-2/temporary/"

PROVIDER_PROFILE_TABLE = "readydoc.provider_profile"
SECTION_TABLE = "readydoc.section"
ATTRIBUTE_TABLE = "readydoc.attribute"
ATTRIBUTE_VALUE_TABLE = "readydoc.attribute_value"

WRITE_MODE = "overwrite"   # overwrite = drop/recreate each target table


# ----------------------------------------------------------
# Setup
# ----------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.shuffle.partitions", "200")


# ----------------------------------------------------------
# Helpers
# ----------------------------------------------------------
def log(msg):
    print(f"[INFO] {msg}")


def read_rds_table(table_name, ctx_name):
    log(f"Reading RDS table: {table_name}")

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": RDS_CONNECTION_NAME,
            "dbtable": table_name,
        },
        transformation_ctx=ctx_name,
    )

    df = dyf.toDF()
    log(f"Loaded table: {table_name}")
    return df


def sanitize_column_name(name: str) -> str:
    """
    Redshift-safe column name.
    Example:
      123      -> attr_123
      abc-def  -> abc_def
    """
    name = str(name).strip()
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)

    if not name:
        name = "col"

    if name[0].isdigit():
        name = f"attr_{name}"

    return name.lower()


def redshift_type_from_spark_type(spark_type_str: str) -> str:
    t = spark_type_str.lower()

    if t in ("bigint", "long"):
        return "BIGINT"
    if t in ("int", "integer"):
        return "INTEGER"
    if t in ("double",):
        return "DOUBLE PRECISION"
    if t in ("float",):
        return "REAL"
    if t.startswith("decimal"):
        return spark_type_str.upper()
    if t == "boolean":
        return "BOOLEAN"
    if t == "timestamp":
        return "TIMESTAMP"
    if t == "date":
        return "DATE"

    # all pivoted values are strings anyway
    return "VARCHAR(65535)"


def build_create_table_sql(table_name: str, df):
    columns_sql = []

    for field in df.schema.fields:
        col_name = sanitize_column_name(field.name)
        col_type = redshift_type_from_spark_type(field.dataType.simpleString())
        columns_sql.append(f'"{col_name}" {col_type}')

    columns_sql_str = ",\n    ".join(columns_sql)

    return f"""
        CREATE TABLE {table_name} (
            {columns_sql_str}
        );
    """


def write_df_to_redshift(df, table_name):
    if df.rdd.isEmpty():
        log(f"Skipping empty DataFrame for table {table_name}")
        return

    # sanitize all column names before write
    renamed_df = df
    for old_name in df.columns:
        new_name = sanitize_column_name(old_name)
        if old_name != new_name:
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)

    create_sql = build_create_table_sql(table_name, renamed_df)

    if WRITE_MODE == "overwrite":
        preactions = f"""
            DROP TABLE IF EXISTS {table_name};
            {create_sql}
        """
    else:
        preactions = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join([f'"{c}" VARCHAR(65535)' for c in renamed_df.columns])}
            );
        """

    log(f"Writing to Redshift table: {table_name}")
    log(f"Columns: {renamed_df.columns}")

    dyf = DynamicFrame.fromDF(renamed_df, glue_context, f"dyf_{table_name.replace('.', '_')}")

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=REDSHIFT_CONNECTION_NAME,
        connection_options={
            "dbtable": table_name,
            "database": REDSHIFT_DATABASE,
            "preactions": preactions,
        },
        redshift_tmp_dir=REDSHIFT_TMP_DIR,
    )

    log(f"Finished writing table: {table_name}")


# ----------------------------------------------------------
# Read source tables from RDS
# ----------------------------------------------------------
try:
    provider_profile_df = (
        read_rds_table(PROVIDER_PROFILE_TABLE, "provider_profile_ctx")
        .select(
            F.col("id").alias("record_id"),
            "section_id",
            "org_id",
            F.col("is_deleted").alias("record_is_deleted"),
        )
    )

    section_df = (
        read_rds_table(SECTION_TABLE, "section_ctx")
        .select(
            F.col("id").alias("section_id"),
            F.col("name").alias("section_name"),
            F.col("is_deleted").alias("section_is_deleted"),
            F.col("is_active").alias("section_is_active"),
        )
    )

    attribute_df = (
        read_rds_table(ATTRIBUTE_TABLE, "attribute_ctx")
        .select(
            F.col("id").alias("attribute_id"),
            "section_id",
            "org_id",
        )
        .filter(F.col("section_id").isNotNull())
        .filter(F.col("attribute_id").isNotNull())
    )

    attribute_value_df = (
        read_rds_table(ATTRIBUTE_VALUE_TABLE, "attribute_value_ctx")
        .select(
            F.col("provider_profile_id").alias("record_id"),
            "attribute_id",
            F.col("value").cast("string").alias("value"),
        )
        .filter(F.col("record_id").isNotNull())
        .filter(F.col("attribute_id").isNotNull())
    )

    log("Finished reading all source tables from RDS")

except Exception as e:
    log("Failed while reading source tables from RDS")
    print(str(e))
    print(traceback.format_exc())
    raise


# ----------------------------------------------------------
# Filter active / non-deleted records
# ----------------------------------------------------------
provider_profile_df = provider_profile_df.filter(
    F.col("record_is_deleted").isNull() | (F.col("record_is_deleted") == False)
)

section_df = section_df.filter(
    (F.col("section_is_deleted").isNull() | (F.col("section_is_deleted") == False))
    & (F.col("section_is_active").isNull() | (F.col("section_is_active") == True))
)


# ----------------------------------------------------------
# Join context
# ----------------------------------------------------------
records_df = (
    provider_profile_df.join(section_df, on="section_id", how="inner")
    .select(
        "record_id",
        "org_id",
        "section_id",
        "section_name",
        "provider_id"
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

values_with_context_df = (
    attribute_value_df.join(
        records_df.select("record_id", "org_id", "section_id"),
        on="record_id",
        how="inner",
    )
    .select(
        "org_id",
        "section_id",
        "record_id",
        "attribute_id",
        "value",
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

records_df = records_df.repartition(F.col("org_id"), F.col("section_id")).persist(StorageLevel.MEMORY_AND_DISK)
values_with_context_df = values_with_context_df.repartition(F.col("org_id"), F.col("section_id")).persist(StorageLevel.MEMORY_AND_DISK)

records_df.count()
values_with_context_df.count()


# ----------------------------------------------------------
# Get org + section combinations
# ----------------------------------------------------------
org_section_rows = (
    records_df.select("org_id", "section_id", "section_name")
    .distinct()
    .orderBy("org_id", "section_id")
    .collect()
)

log(f"Found {len(org_section_rows)} org + section combinations")


# ----------------------------------------------------------
# Build and write one Redshift table per org + section
# Table name format:
#   public.org_<org_id>_section_<section_id>
# Columns:
#   record_id, attr_<attribute_id>, attr_<attribute_id>, ...
# ----------------------------------------------------------
for row in org_section_rows:
    org_id = row["org_id"]
    section_id = row["section_id"]
    section_name = row["section_name"]

    log(f"Processing org_id={org_id}, section_id={section_id}, section_name={section_name}")

    current_section_records_df = (
        records_df.filter(
            (F.col("org_id") == org_id) &
            (F.col("section_id") == section_id)
        )
        .select("record_id", "provider_id")
    )

    current_section_values_df = (
        values_with_context_df.filter(
            (F.col("org_id") == org_id) &
            (F.col("section_id") == section_id)
        )
        .select("record_id", "attribute_id", "value")
    )

    current_section_attribute_rows = (
        attribute_df.filter(
            (F.col("org_id") == org_id) &
            (F.col("section_id") == section_id)
        )
        .select("attribute_id")
        .distinct()
        .orderBy("attribute_id")
        .collect()
    )

    current_section_attribute_ids = [r["attribute_id"] for r in current_section_attribute_rows]

    if current_section_attribute_ids:
        pivot_df = (
            current_section_values_df
            .groupBy("record_id")
            .pivot("attribute_id", current_section_attribute_ids)
            .agg(F.first("value"))
        )

        # rename pivoted attribute columns to attr_<id>
        for attr_id in current_section_attribute_ids:
            old_col = str(attr_id)
            new_col = f"attr_{attr_id}"
            if old_col in pivot_df.columns:
                pivot_df = pivot_df.withColumnRenamed(old_col, new_col)
    else:
        pivot_df = current_section_records_df

    final_df = current_section_records_df.join(pivot_df, "record_id", "left")

    target_table = f"public.org_{org_id}_section_{section_id}"

    try:
        write_df_to_redshift(final_df, target_table)
    except Exception as e:
        log(f"Failed writing table {target_table}")
        print(str(e))
        print(traceback.format_exc())
        raise


log("SUCCESS: all org/section tables written to Redshift")
job.commit()