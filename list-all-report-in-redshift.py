import json
import os
import time
import logging
import boto3
import base64

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

redshift = boto3.client("redshift-data")

REDSHIFT_WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]

POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1"))
MAX_WAIT_SECONDS = int(os.getenv("MAX_WAIT_SECONDS", "60"))


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


def run_query(sql, statement_name="query", parameters=None):
    request = {
        "WorkgroupName": REDSHIFT_WORKGROUP_NAME,
        "Database": REDSHIFT_DATABASE,
        "SecretArn": REDSHIFT_SECRET_ARN,
        "Sql": sql,
        "StatementName": statement_name,
    }

    if parameters:
        request["Parameters"] = parameters

    response = redshift.execute_statement(**request)

    statement_id = response["Id"]
    start_time = time.time()

    while True:
        result = redshift.describe_statement(Id=statement_id)
        status = result["Status"]

        if status == "FINISHED":
            break

        if status in ["FAILED", "ABORTED"]:
            raise Exception(result.get("Error", f"Query {status.lower()}"))

        if time.time() - start_time > MAX_WAIT_SECONDS:
            raise TimeoutError(f"Query timed out after {MAX_WAIT_SECONDS} seconds")

        time.sleep(POLL_INTERVAL_SECONDS)

    records = []
    next_token = None

    while True:
        if next_token:
            result = redshift.get_statement_result(Id=statement_id, NextToken=next_token)
        else:
            result = redshift.get_statement_result(Id=statement_id)

        records.extend(result.get("Records", []))
        next_token = result.get("NextToken")

        if not next_token:
            break

    return records


def build_reports(records):
    reports = []

    for record in records:
        reports.append({
            "reportId": parse_value(record[0]),
            "name": parse_value(record[1]),
            "description": parse_value(record[2]),
            "createdAt": parse_value(record[3]),
            "updatedAt": parse_value(record[4]),
            "status": parse_value(record[5]),       
            "lastRunDate": parse_value(record[6]),  
        })

    return reports


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


def lambda_handler(event, context):
    logger.info("Lambda started")

    try:
        headers = event.get("headers") or {}
        token = headers.get("authorization") or headers.get("Authorization")

        if not token:
            return {
                "statusCode": 401,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"message": "Missing authorization token"}),
            }

        org_id = get_org_id_from_token(token)

        if not org_id:
            return {
                "statusCode": 401,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"message": "org_id not found in token"}),
            }

        sql = """
        SELECT
            report_id,
            name,
            description,
            created_at,
            updated_at,
            status,
            last_run_date
        FROM reports
        WHERE org_id = :org_id
        ORDER BY updated_at DESC, report_id DESC;
        """

        records = run_query(
            sql,
            "fetch_reports_by_org",
            parameters=[
                {
                    "name": "org_id",
                    "value": str(org_id)
                }
            ]
        )

        reports = build_reports(records)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
            },
            "body": json.dumps(reports, default=str),
        }

    except Exception as e:
        logger.exception("Lambda failed")

        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({
                "error": "Internal server error",
                "message": str(e),
            }),
        }