import json
import os
import time
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

redshift = boto3.client("redshift-data")

REDSHIFT_WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]


def lambda_handler(event, context):
    try:
        print(JSON.dumps(event))
        body = parse_body(event)

        name = body.get("name")
        description = body.get("description", "")
        config = body.get("config", [])

        if not name:
            return response(400, {"error": "name is required"})

        if not isinstance(config, list):
            return response(400, {"error": "config must be a list"})

        # ✅ 1. Insert report and get ID
        insert_report_sql = f"""
            INSERT INTO reports (name, description)
            VALUES ('{escape(name)}', '{escape(description)}')
            RETURNING report_id;
        """

        report_id = execute_sql_and_get_single_value(insert_report_sql)

        if not report_id:
            raise Exception("Failed to insert report")

        # ✅ 2. Prepare bulk inserts (your style)
        section_values = []
        attribute_values = []

        for item in config:
            section_id = int(item["sectionId"])
            attrs = item.get("attributeIds", [])

            section_values.append(f"({report_id}, {section_id})")

            for attr in attrs:
                attribute_values.append(f"({report_id}, {section_id}, {int(attr)})")

        # ✅ 3. Insert sections
        if section_values:
            insert_sections_sql = f"""
                INSERT INTO report_sections (report_id, section_id)
                VALUES {", ".join(section_values)};
            """
            execute_sql(insert_sections_sql)

        # ✅ 4. Insert attributes
        if attribute_values:
            insert_attributes_sql = f"""
                INSERT INTO report_section_attributes (report_id, section_id, attribute_id)
                VALUES {", ".join(attribute_values)};
            """
            execute_sql(insert_attributes_sql)

        return response(200, {
            "message": "Report saved successfully",
            "reportId": report_id
        })

    except Exception as e:
        logger.exception("Error saving report")
        return response(500, {"error": str(e)})


# -------------------------
# Helpers
# -------------------------

def execute_sql(sql: str):
    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )
    wait(result["Id"])


def execute_sql_and_get_single_value(sql: str):
    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )

    statement_id = result["Id"]
    wait(statement_id)

    res = redshift.get_statement_result(Id=statement_id)
    records = res.get("Records", [])

    if not records:
        return None

    cell = records[0][0]

    if "longValue" in cell:
        return cell["longValue"]

    if "stringValue" in cell:
        return cell["stringValue"]

    return None


def wait(statement_id):
    while True:
        desc = redshift.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status == "FINISHED":
            return
        if status in ["FAILED", "ABORTED"]:
            raise Exception(desc.get("Error"))

        time.sleep(1)


def parse_body(event):
    if "body" in event:
        if isinstance(event["body"], str):
            return json.loads(event["body"])
        return event["body"]
    return event


def escape(val):
    if val is None:
        return ""
    return str(val).replace("'", "''")


def response(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }