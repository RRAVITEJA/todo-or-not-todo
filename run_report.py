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

        org_id = int(body["orgId"])
        report_id = int(body["reportId"])

        rows = get_report_rows(report_id)

        if not rows:
            return build_response(404, {"error": f"No config found for reportId {report_id}"})

        sql = build_query_from_rows(org_id, rows)
        print(sql)
        return build_response(200, {"query": sql})

    except Exception as e:
        logger.exception("Error generating query")
        return build_response(400, {"error": str(e)})


def get_report_rows(report_id: int):
    sql = f"""
        SELECT section_id, attribute_id
        FROM report_section_attributes
        WHERE report_id = {report_id}
        ORDER BY section_id, attribute_id;
    """
    return execute_query(sql)


def build_query_from_rows(org_id: int, rows: list[list[int]]) -> str:
    section_attrs = {}
    section_order = []

    for row in rows:
        section_id = int(row[0])
        attribute_id = int(row[1])

        if section_id not in section_attrs:
            section_attrs[section_id] = []
            section_order.append(section_id)

        section_attrs[section_id].append(attribute_id)

    if not section_order:
        raise ValueError("No sections found for report")

    aliases = {section_id: f"s{section_id}" for section_id in section_order}

    first_section_id = section_order[0]
    first_alias = aliases[first_section_id]

    select_cols = [f"{first_alias}.provider_id"]

    for section_id in section_order:
        alias = aliases[section_id]
        for attribute_id in section_attrs[section_id]:
            select_cols.append(f"{alias}.attr_{attribute_id}")

    sql_parts = [
        "SELECT",
        "    " + ",\n    ".join(select_cols),
        f"FROM org_{org_id}_section_{first_section_id} {first_alias}"
    ]

    for section_id in section_order[1:]:
        alias = aliases[section_id]
        sql_parts.append(
            f"LEFT JOIN org_{org_id}_section_{section_id} {alias}\n"
            f"    ON {first_alias}.provider_id = {alias}.provider_id"
        )

    return "\n".join(sql_parts) + ";"


def execute_query(sql: str):
    logger.info("Executing SQL: %s", sql)

    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )

    statement_id = result["Id"]
    wait_for_statement(statement_id)

    query_result = redshift.get_statement_result(Id=statement_id)
    records = query_result.get("Records", [])

    parsed_rows = []

    for record in records:
        parsed_row = []
        for cell in record:
            if "longValue" in cell:
                parsed_row.append(cell["longValue"])
            elif "stringValue" in cell:
                parsed_row.append(cell["stringValue"])
            elif "doubleValue" in cell:
                parsed_row.append(cell["doubleValue"])
            elif cell.get("isNull"):
                parsed_row.append(None)
            else:
                parsed_row.append(None)
        parsed_rows.append(parsed_row)

    return parsed_rows


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
        "body": json.dumps(body)
    }