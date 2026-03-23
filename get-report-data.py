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
        body = parse_body(event)

        report_id = int(body["reportId"])
        limit = int(body.get("limit", 100))

        if limit <= 0:
            return build_response(400, {"error": "limit must be greater than 0"})

        mv_name = f"report_{report_id}"

        sql = f"SELECT * FROM {mv_name} LIMIT {limit};"
        logger.info("Executing query: %s", sql)

        result = execute_query_with_column_names(sql)

        return build_response(200, {
            "reportId": report_id,
            "materializedView": mv_name,
            "count": len(result["rows"]),
            "data": result["rows"]
        })

    except Exception as e:
        logger.exception("Error fetching report data")
        return build_response(400, {"error": str(e)})


def execute_query_with_column_names(sql: str):
    response = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )

    statement_id = response["Id"]
    wait_for_statement(statement_id)

    result = redshift.get_statement_result(Id=statement_id)

    columns = [col["name"] for col in result.get("ColumnMetadata", [])]
    records = result.get("Records", [])

    rows = []
    for record in records:
        row = {}
        for i, cell in enumerate(record):
            row[columns[i]] = parse_cell_value(cell)
        rows.append(row)

    return {
        "columns": columns,
        "rows": rows
    }


def parse_cell_value(cell: dict):
    if "stringValue" in cell:
        return cell["stringValue"]
    if "longValue" in cell:
        return cell["longValue"]
    if "doubleValue" in cell:
        return cell["doubleValue"]
    if "booleanValue" in cell:
        return cell["booleanValue"]
    if cell.get("isNull"):
        return None
    return None


def wait_for_statement(statement_id: str):
    while True:
        result = redshift.describe_statement(Id=statement_id)
        status = result["Status"]

        if status == "FINISHED":
            return

        if status in ("FAILED", "ABORTED"):
            raise Exception(result.get("Error", f"Statement failed: {statement_id}"))

        time.sleep(1)


def parse_body(event):
    if "body" in event:
        if isinstance(event["body"], str):
            return json.loads(event["body"])
        return event["body"]
    return event


def build_response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, default=str)
    }