import json
import os
import time
import random
import logging
import boto3
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)

redshift = boto3.client("redshift-data")

REDSHIFT_WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]


def lambda_handler(event, context):
    try:
        print("Received event: " + json.dumps(event, indent=2))

        token = get_authorization_token(event)
        if not token:
            return response(401, {"error": "Missing authorization token"})

        org_id = get_org_id_from_token(token)
        if not org_id:
            return response(401, {"error": "org_id not found in token"})

        body = parse_body(event)

        name = body.get("name")
        description = body.get("description", "")
        config = body.get("config", [])

        if not name:
            return response(400, {"error": "name is required"})

        if not isinstance(config, list):
            return response(400, {"error": "config must be a list"})

        validate_config_for_org(config, org_id)

        report_id = generate_report_id()

        insert_report_sql = f"""
            INSERT INTO reports (report_id, org_id, name, description)
            VALUES ({report_id}, '{escape(org_id)}', '{escape(name)}', '{escape(description)}');
        """
        execute_sql(insert_report_sql)

        section_values = []
        attribute_values = []

        for item in config:
            section_id = int(item["sectionId"])
            attribute_ids = item.get("attributeIds", [])

            section_values.append(f"({report_id}, '{escape(org_id)}', {section_id})")

            for attribute_id in attribute_ids:
                attribute_values.append(
                    f"({report_id}, '{escape(org_id)}', {section_id}, {int(attribute_id)})"
                )

        if section_values:
            insert_sections_sql = f"""
                INSERT INTO report_sections (report_id, org_id, section_id)
                VALUES {", ".join(section_values)};
            """
            execute_sql(insert_sections_sql)

        if attribute_values:
            insert_attributes_sql = f"""
                INSERT INTO report_section_attributes (report_id, org_id, section_id, attribute_id)
                VALUES {", ".join(attribute_values)};
            """
            execute_sql(insert_attributes_sql)

        return response(200, {
            "message": "Report saved successfully",
            "reportId": report_id,
            "orgId": org_id
        })

    except Exception as e:
        logger.exception("Error saving report")
        return response(500, {"error": str(e)})


def get_authorization_token(event):
    headers = event.get("headers") or {}
    return headers.get("authorization") or headers.get("Authorization")


def get_org_id_from_token(token: str):
    if token.startswith("Bearer "):
        token = token[7:]

    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid JWT token")

    payload_b64 = parts[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)

    payload_json = base64.urlsafe_b64decode(payload_b64).decode("utf-8")
    payload = json.loads(payload_json)

    return payload.get("org_id")


def validate_config_for_org(config, org_id):
    sql = f"""
        SELECT
            section_id,
            attribute_id
        FROM public.attribute_section
        WHERE org_id = '{escape(org_id)}';
    """

    records = run_query(sql)

    valid_section_ids = set()
    valid_attributes_by_section = {}

    for record in records:
        section_id = parse_value(record[0])
        attribute_id = parse_value(record[1])

        if section_id is None:
            continue

        valid_section_ids.add(int(section_id))

        if section_id not in valid_attributes_by_section:
            valid_attributes_by_section[int(section_id)] = set()

        if attribute_id is not None:
            valid_attributes_by_section[int(section_id)].add(int(attribute_id))

    for item in config:
        if "sectionId" not in item:
            raise Exception("sectionId is required in each config item")

        section_id = int(item["sectionId"])
        attribute_ids = item.get("attributeIds", [])

        if section_id not in valid_section_ids:
            raise Exception(f"Invalid sectionId for org_id {org_id}: {section_id}")

        for attribute_id in attribute_ids:
            attribute_id = int(attribute_id)
            if attribute_id not in valid_attributes_by_section.get(section_id, set()):
                raise Exception(
                    f"Invalid attributeId {attribute_id} for sectionId {section_id} and org_id {org_id}"
                )


def parse_value(field):
    if field.get("isNull"):
        return None
    if "stringValue" in field:
        return field["stringValue"]
    if "longValue" in field:
        return field["longValue"]
    if "doubleValue" in field:
        return field["doubleValue"]
    if "booleanValue" in field:
        return field["booleanValue"]
    return None


def run_query(sql: str):
    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )

    statement_id = result["Id"]
    wait(statement_id)

    records = []
    next_token = None

    while True:
        if next_token:
            output = redshift.get_statement_result(Id=statement_id, NextToken=next_token)
        else:
            output = redshift.get_statement_result(Id=statement_id)

        records.extend(output.get("Records", []))
        next_token = output.get("NextToken")

        if not next_token:
            break

    return records


def generate_report_id():
    return int(f"{int(time.time() * 1000)}{random.randint(100, 999)}")


def execute_sql(sql: str):
    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )
    wait(result["Id"])


def wait(statement_id):
    while True:
        desc = redshift.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status == "FINISHED":
            return

        if status in ["FAILED", "ABORTED"]:
            raise Exception(desc.get("Error", f"Statement failed: {statement_id}"))

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
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }