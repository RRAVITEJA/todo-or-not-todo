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
    trigger_type = detect_trigger_type(event)

    org_id = None
    report_id = None

    try:
        logger.info("Received event: %s", json.dumps(event))

        org_id, report_id, trigger_type = extract_request_context(event)

        update_report_status(report_id, "running")

        attribute_rows = get_report_rows(report_id)
        filter_rows = get_report_filters(report_id)

        if not attribute_rows:
            update_report_status(report_id, "failed")
            return build_response(
                404,
                {"error": f"No config found for reportId {report_id}"},
                trigger_type
            )

        query = build_query_from_rows(org_id, attribute_rows, filter_rows)

        logger.info("Generated query:\n%s", query)

        mv_name = f"report_{report_id}"

        create_materialized_view(mv_name, query)

        mark_report_completed(report_id)

        return build_response(
            200,
            {
                "message": "Materialized view created successfully",
                "reportId": report_id,
                "materializedView": mv_name,
                "query": query
            },
            trigger_type
        )

    except Exception as e:
        logger.exception("Error")

        if report_id:
            try:
                update_report_status(report_id, "failed")
            except:
                pass

        return build_response(400, {"error": str(e)}, trigger_type)


# -----------------------------
# FETCH DATA
# -----------------------------

def get_report_rows(report_id):
    sql = f"""
        SELECT section_id, attribute_id
        FROM report_section_attributes
        WHERE report_id = {report_id};
    """
    return execute_query(sql)


def get_report_filters(report_id):
    sql = f"""
        SELECT section_id, attribute_id, operator, value
        FROM report_filters
        WHERE report_id = {report_id};
    """
    return execute_query(sql)


# -----------------------------
# QUERY BUILDER (NO ORDER BY)
# -----------------------------

def build_query_from_rows(org_id, rows, filter_rows):
    section_attrs = {}
    section_order = []

    for row in rows:
        section_id = str(parse_value(row[0]))
        attribute_id = str(parse_value(row[1]))

        if section_id not in section_attrs:
            section_attrs[section_id] = []
            section_order.append(section_id)

        section_attrs[section_id].append(attribute_id)

    if not section_order:
        raise ValueError("No sections found")

    aliases = {sid: f"s{sid}" for sid in section_order}

    first_section_id = section_order[0]
    first_alias = aliases[first_section_id]

    select_cols = [f"{first_alias}.provider_id"]

    for sid in section_order:
        alias = aliases[sid]
        for attr in section_attrs[sid]:
            select_cols.append(f"{alias}.attr_{attr}")

    sql_parts = [
        "SELECT",
        "    " + ",\n    ".join(select_cols),
        f"FROM org_{org_id}_section_{first_section_id} {first_alias}"
    ]

    for sid in section_order[1:]:
        alias = aliases[sid]
        sql_parts.append(
            f"LEFT JOIN org_{org_id}_section_{sid} {alias} "
            f"ON {first_alias}.provider_id = {alias}.provider_id"
        )

    where_clauses = []

    for row in filter_rows:
        section_id = str(parse_value(row[0]))
        attribute_id = str(parse_value(row[1]))
        operator = str(parse_value(row[2])).strip().upper()
        value = parse_value(row[3])

        alias = aliases.get(section_id)
        if not alias:
            continue

        column = f"{alias}.attr_{attribute_id}"

        if value is None:
            continue

        value_str = str(value).strip().replace("'", "''")

        if operator == "=":
            where_clauses.append(f"{column} = '{value_str}'")

        elif operator in ["!=", "<>"]:
            where_clauses.append(f"{column} <> '{value_str}'")

        elif operator == ">":
            where_clauses.append(f"{column} > '{value_str}'")

        elif operator == "<":
            where_clauses.append(f"{column} < '{value_str}'")

        elif operator == "CONTAINS":
            where_clauses.append(f"{column} ILIKE '%{value_str}%'")

        elif operator == "IN":
            values = [v.strip().replace("'", "''") for v in value_str.split(",") if v.strip()]
            if values:
                in_values = ", ".join([f"'{v}'" for v in values])
                where_clauses.append(f"{column} IN ({in_values})")

        else:
            raise ValueError(f"Unsupported operator: {operator}")

    if where_clauses:
        sql_parts.append("WHERE")
        sql_parts.append("    " + "\n    AND ".join(where_clauses))

    return "\n".join(sql_parts)

# -----------------------------
# CREATE MV
# -----------------------------

def create_materialized_view(name, query):
    sql = f"""
        DROP MATERIALIZED VIEW IF EXISTS {name};

        CREATE MATERIALIZED VIEW {name}
        AUTO REFRESH NO
        AS
        {query};
    """
    execute_sql(sql)


# -----------------------------
# REPORT STATUS
# -----------------------------

def update_report_status(report_id, status):
    sql = f"""
        UPDATE reports
        SET status = '{status}', updated_at = GETDATE()
        WHERE report_id = {report_id};
    """
    execute_sql(sql)


def mark_report_completed(report_id):
    sql = f"""
        UPDATE reports
        SET status = 'completed',
            last_run_date = GETDATE(),
            updated_at = GETDATE()
        WHERE report_id = {report_id};
    """
    execute_sql(sql)


# -----------------------------
# EXECUTION HELPERS
# -----------------------------

def execute_sql(sql):
    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )
    wait(result["Id"])


def execute_query(sql):
    result = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql
    )

    statement_id = result["Id"]
    wait(statement_id)

    output = redshift.get_statement_result(Id=statement_id)
    return output.get("Records", [])


def wait(statement_id):
    while True:
        res = redshift.describe_statement(Id=statement_id)
        status = res["Status"]

        if status == "FINISHED":
            return

        if status in ["FAILED", "ABORTED"]:
            raise Exception(res.get("Error"))

        time.sleep(1)


def parse_value(cell):
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


# -----------------------------
# REQUEST HELPERS
# -----------------------------

def detect_trigger_type(event):
    if "detail" in event:
        return "eventbridge"
    return "api"


def extract_request_context(event):
    if "detail" in event:
        detail = event["detail"]
        if isinstance(detail, str):
            detail = json.loads(detail)
        return detail["orgId"], detail["reportId"], "eventbridge"

    token = get_authorization_token(event)
    org_id = get_org_id_from_token(token)

    report_id = int(event["pathParameters"]["reportId"])

    return org_id, report_id, "api"


def get_authorization_token(event):
    headers = event.get("headers") or {}
    return headers.get("authorization") or headers.get("Authorization")


def get_org_id_from_token(token):
    if token.startswith("Bearer "):
        token = token[7:]

    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)

    decoded = base64.urlsafe_b64decode(payload).decode()
    return json.loads(decoded)["org_id"]


def build_response(code, body, trigger_type):
    if trigger_type == "api":
        return {
            "statusCode": code,
            "body": json.dumps(body)
        }
    return body