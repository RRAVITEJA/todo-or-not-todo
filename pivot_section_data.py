import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# ----------------------------------------------------------
# Job setup
# ----------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

BASE_PATH = "s3://readydoc-ravi-local/dynamic-profile-tables-export"
OUTPUT_BASE_PATH = f"{BASE_PATH}/oputput"
WRITE_MODE = "overwrite"

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", "200")


# ----------------------------------------------------------
# Input paths
# ----------------------------------------------------------
ATTRIBUTE_VALUE_PATH = f"{BASE_PATH}/readydoc.attribute_value.parquet"
SECTION_PATH = f"{BASE_PATH}/readydoc.section.parquet"
PROVIDER_PROFILE_PATH = f"{BASE_PATH}/readydoc.provider_profile.parquet"
ATTRIBUTE_PATH = f"{BASE_PATH}/readydoc.attribute.parquet"

# ----------------------------------------------------------
# provider_profile DF
# ----------------------------------------------------------
provider_profile_df = (
    glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [PROVIDER_PROFILE_PATH],
            "recurse": True,
        },
    )
    .toDF()
    .select(
        F.col("id").alias("record_id"),
        "section_id",
        "org_id",
        F.col("is_deleted").alias("record_is_deleted"),
    )
)

# ----------------------------------------------------------
#  section DF
# ----------------------------------------------------------
section_df = (
    glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [SECTION_PATH],
            "recurse": True,
        },
    )
    .toDF()
    .select(
        F.col("id").alias("section_id"),
        F.col("name").alias("section_name"),
        F.col("is_deleted").alias("section_is_deleted"),
        F.col("is_active").alias("section_is_active"),
    )
)

# ----------------------------------------------------------
#  attribute DF
# ----------------------------------------------------------
attribute_df = (
    glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [ATTRIBUTE_PATH],
            "recurse": True,
        },
    )
    .toDF()
    .select(
        F.col("id").alias("attribute_id"),
        "section_id",
        "org_id",
    )
    .filter(F.col("section_id").isNotNull())
    .filter(F.col("attribute_id").isNotNull())
)

# ----------------------------------------------------------
#  attribute_value DF
# ----------------------------------------------------------
attribute_value_df = (
    glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [ATTRIBUTE_VALUE_PATH],
            "recurse": True,
        },
    )
    .toDF()
    .select(
        F.col("provider_profile_id").alias("record_id"),
        "attribute_id",
        F.col("value").cast("string").alias("value"),
    )
    .filter(F.col("record_id").isNotNull())
    .filter(F.col("attribute_id").isNotNull())
)


# ----------------------------------------------------------
# Keep only active / non-deleted records and sections
# ----------------------------------------------------------
provider_profile_df = provider_profile_df.filter(
    F.col("record_is_deleted").isNull() | (F.col("record_is_deleted") == False)
)

section_df = section_df.filter(
    (F.col("section_is_deleted").isNull() | (F.col("section_is_deleted") == False))
    & (F.col("section_is_active").isNull() | (F.col("section_is_active") == True))
)


# ----------------------------------------------------------
# reciords df : join provider_profile with section to get org_id + section_id for each record
# ----------------------------------------------------------
records_df = (
    provider_profile_df.join(section_df, on="section_id", how="inner")
    .select(
        "record_id",
        "org_id",
        "section_id",
        "section_name",
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)


# ----------------------------------------------------------
# Attach org + section info to attribute values
# ----------------------------------------------------------
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


# ----------------------------------------------------------
# Get org + section list to process
# ----------------------------------------------------------
org_section_rows = (
    records_df.select("org_id", "section_id", "section_name")
    .distinct()
    .orderBy("org_id", "section_id")
    .collect()
)

print(f"Found {len(org_section_rows)} org + section combinations to export")


# ----------------------------------------------------------
# Repartition once
# ----------------------------------------------------------
records_df = records_df.repartition(F.col("org_id"), F.col("section_id")).persist(StorageLevel.MEMORY_AND_DISK)
values_with_context_df = values_with_context_df.repartition(F.col("org_id"), F.col("section_id")).persist(StorageLevel.MEMORY_AND_DISK)

records_df.count()
values_with_context_df.count()


# ----------------------------------------------------------
# Build one parquet output per org + section
# Output columns:
#   record_id, <attribute_id>, <attribute_id>, ...
# ----------------------------------------------------------
for row in org_section_rows:
    org_id = row["org_id"]
    section_id = row["section_id"]
    section_name = row["section_name"]

    print(f"Processing org_id={org_id}, section_id={section_id}, section_name={section_name}")

    current_section_records_df = records_df.filter(
        (F.col("org_id") == org_id) &
        (F.col("section_id") == section_id)
    )
    
    # get values for this org + section
    current_section_values_df = values_with_context_df.filter(
        (F.col("org_id") == org_id) &
        (F.col("section_id") == section_id)
    ).select("record_id", "attribute_id", "value")

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

    # python list comprehension operation to create attribute_id list from rows
    current_section_attribute_ids = [row["attribute_id"] for row in current_section_attribute_rows] 

    if current_section_attribute_ids:
        pivot_df = (
            current_section_values_df
            .groupBy("record_id")
            .pivot("attribute_id", current_section_attribute_ids)
            .agg(F.first("value"))
        )
    else:
        pivot_df = spark.createDataFrame([], current_section_records_df.select("record_id").schema)

    final_df = (
        current_section_records_df
        .join(pivot_df, "record_id", "left")
    )

    output_path = f"{OUTPUT_BASE_PATH}/org_id_{org_id}/section_{section_id}"

    print(f"Writing parquet to {output_path}")

    (
        final_df.write
        .mode(WRITE_MODE)
        .option("compression", "snappy")
        .parquet(output_path)
    )

job.commit()