import json
import os
import time
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
        print("Received event: ",event)

        token = get_authorization_token(event)
        if not token:
            return response(401, {"error": "Missing authorization token"})

        org_id = get_org_id_from_token(token)
        if not org_id:
            return response(401, {"error": "org_id not found in token"})

        report_id = get_report_id(event)
        if not report_id:
            return response(400, {"error": "reportId is required"})

        if not str(report_id).isdigit():
            return response(400, {"error": "reportId must be numeric"})

        report_sql = f"""
            SELECT
                report_id,
                org_id,
                name,
                description,
                status,
                created_at,
                updated_at
            FROM reports
            WHERE report_id = {report_id}
              AND org_id = '{escape(org_id)}'
            LIMIT 1;
        """
        report_records = run_query(report_sql)

        if not report_records:
            return response(404, {"error": "Report not found"})

        report_row = report_records[0]

        report = {
            "reportId": parse_value(report_row[0]),
            "orgId": parse_value(report_row[1]),
            "name": parse_value(report_row[2]),
            "description": parse_value(report_row[3]),
            "status": parse_value(report_row[4]),
            "createdAt": parse_value(report_row[5]),
            "updatedAt": parse_value(report_row[6]),
        }

        config_sql = f"""
            SELECT
                rs.section_id,
                rsa.attribute_id
            FROM report_sections rs
            LEFT JOIN report_section_attributes rsa
                ON rs.report_id = rsa.report_id
               AND rs.org_id = rsa.org_id
               AND rs.section_id = rsa.section_id
            WHERE rs.report_id = {report_id}
              AND rs.org_id = '{escape(org_id)}'
            ORDER BY rs.section_id, rsa.attribute_id;
        """
        config_records = run_query(config_sql)
        config = build_config(config_records)

        filters_sql = f"""
            SELECT
                section_id,
                attribute_id,
                operator,
                value
            FROM report_filters
            WHERE report_id = {report_id}
              AND org_id = '{escape(org_id)}'
            ORDER BY section_id, attribute_id;
        """
        filter_records = run_query(filters_sql)
        filters = []
        for row in filter_records:
            filters.append({
                "sectionId": parse_value(row[0]),
                "attributeId": parse_value(row[1]),
                "operator": parse_value(row[2]),
                "value": parse_value(row[3]),
            })

        sorts_sql = f"""
            SELECT
                section_id,
                attribute_id,
                direction,
                sort_order
            FROM report_sorts
            WHERE report_id = {report_id}
              AND org_id = '{escape(org_id)}'
            ORDER BY sort_order ASC, section_id, attribute_id;
        """
        sort_records = run_query(sorts_sql)
        sorts = []
        for row in sort_records:
            sorts.append({
                "sectionId": parse_value(row[0]),
                "attributeId": parse_value(row[1]),
                "direction": parse_value(row[2]),
                "sortOrder": parse_value(row[3]),
            })

        return response(200, {
            "message": "Report fetched successfully",
            "report": {
                **report,
                "config": config,
                "filters": filters,
                "sorts": sorts
            }
        })

    except Exception as e:
        logger.exception("Error fetching report")
        return response(500, {"error": str(e)})


def get_report_id(event):
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    if path_params.get("reportId"):
        return path_params.get("reportId")

    if query_params.get("reportId"):
        return query_params.get("reportId")

    body = parse_body(event)
    return body.get("reportId")


def build_config(records):
    config_map = {}

    for row in records:
        section_id = parse_value(row[0])
        attribute_id = parse_value(row[1])

        if section_id is None:
            continue

        section_id = str(section_id)

        if section_id not in config_map:
            config_map[section_id] = {
                "sectionId": section_id,
                "attributeIds": []
            }

        if attribute_id is not None:
            config_map[section_id]["attributeIds"].append(str(attribute_id))

    return list(config_map.values())


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
    if "body" in event and event["body"] is not None:
        if isinstance(event["body"], str):
            return json.loads(event["body"])
        return event["body"]
    return {}


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
        "body": json.dumps(body, default=str)
    }