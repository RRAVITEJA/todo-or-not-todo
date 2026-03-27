import json
import os
import time
import logging
import boto3
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)

redshift = boto3.client("redshift-data")
eventbridge = boto3.client("events")

REDSHIFT_WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
EVENT_BUS_ARN = os.environ["EVENT_BUS_ARN"]


def lambda_handler(event, context):
    try:
        print("Received event: " + json.dumps(event, indent=2))

        token = get_authorization_token(event)
        if not token:
            return response(401, {"error": "Missing authorization token"})

        org_id = get_org_id_from_token(token)
        if not org_id:
            return response(401, {"error": "org_id not found in token"})

        path_params = event.get("pathParameters") or {}
        if "reportId" not in path_params:
            return response(400, {"error": "reportId is required in pathParameters"})

        report_id = int(path_params["reportId"])

        body = parse_body(event)

        name = body.get("name")
        description = body.get("description", "")
        config = body.get("config", [])
        filters = body.get("filters") or []
        sorts = body.get("sorts") or []

        if not name:
            return response(400, {"error": "name is required"})

        if not isinstance(config, list):
            return response(400, {"error": "config must be a list"})

        if not isinstance(filters, list):
            return response(400, {"error": "filters must be a list"})

        if not isinstance(sorts, list):
            return response(400, {"error": "sorts must be a list"})

        # validate report belongs to org
        validate_report_exists_for_org(report_id, org_id)

        # validate new config
        validate_config_for_org(config, filters, sorts, org_id)

        # update main report row
        update_report_sql = f"""
            UPDATE reports
            SET
                name = '{escape(name)}',
                description = '{escape(description)}',
                status = 'created',
                updated_at = GETDATE()
            WHERE report_id = {report_id}
              AND org_id = '{escape(org_id)}';
        """
        execute_sql(update_report_sql)

        # clear existing config rows
        execute_sql(f"DELETE FROM report_sections WHERE report_id = {report_id} AND org_id = '{escape(org_id)}';")
        execute_sql(f"DELETE FROM report_section_attributes WHERE report_id = {report_id} AND org_id = '{escape(org_id)}';")
        execute_sql(f"DELETE FROM report_filters WHERE report_id = {report_id} AND org_id = '{escape(org_id)}';")
        execute_sql(f"DELETE FROM report_sorts WHERE report_id = {report_id} AND org_id = '{escape(org_id)}';")

        section_values = []
        attribute_values = []
        filter_values = []
        sort_values = []

        for item in config:
            section_id = str(item["sectionId"])
            attribute_ids = item.get("attributeIds", [])

            section_values.append(
                f"({report_id}, '{escape(org_id)}', '{escape(section_id)}')"
            )

            for attribute_id in attribute_ids:
                attribute_id = str(attribute_id)
                attribute_values.append(
                    f"({report_id}, '{escape(org_id)}', '{escape(section_id)}', '{escape(attribute_id)}')"
                )

        for item in filters:
            section_id = str(item["sectionId"])
            attribute_id = str(item["attributeId"])
            operator = str(item["operator"]).strip()
            value = item.get("value", "")

            filter_values.append(
                f"({report_id}, '{escape(org_id)}', '{escape(section_id)}', '{escape(attribute_id)}', "
                f"'{escape(operator)}', '{escape(value)}')"
            )

        for item in sorts:
            section_id = str(item["sectionId"])
            attribute_id = str(item["attributeId"])
            direction = str(item.get("direction", "asc")).lower()
            sort_order = int(item.get("sortOrder", 1))

            sort_values.append(
                f"({report_id}, '{escape(org_id)}', '{escape(section_id)}', '{escape(attribute_id)}', "
                f"'{escape(direction)}', {sort_order})"
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

        if filter_values:
            insert_filters_sql = f"""
                INSERT INTO report_filters (report_id, org_id, section_id, attribute_id, operator, value)
                VALUES {", ".join(filter_values)};
            """
            execute_sql(insert_filters_sql)

        if sort_values:
            insert_sorts_sql = f"""
                INSERT INTO report_sorts (report_id, org_id, section_id, attribute_id, direction, sort_order)
                VALUES {", ".join(sort_values)};
            """
            execute_sql(insert_sorts_sql)

        # recreate materialized view
        detail_type = "CretaeMaterialized"
        payload = {
            "orgId": int(org_id) if str(org_id).isdigit() else org_id,
            "reportId": report_id
        }

        send_eventbridge_event(detail_type, payload)

        return response(200, {
            "message": "Report updated successfully",
            "reportId": report_id,
            "orgId": org_id
        })

    except Exception as e:
        logger.exception("Error updating report")
        return response(500, {"error": str(e)})


def validate_report_exists_for_org(report_id, org_id):
    sql = f"""
        SELECT report_id
        FROM reports
        WHERE report_id = {report_id}
          AND org_id = '{escape(org_id)}'
        LIMIT 1;
    """

    records = run_query(sql)

    if not records:
        raise Exception(f"Report {report_id} not found for org_id {org_id}")


def send_eventbridge_event(detail_type, payload):
    event = {
        "Source": "lambda.readydoc.intivahealth.com",
        "DetailType": detail_type,
        "Detail": json.dumps(payload),
        "EventBusName": EVENT_BUS_ARN,
    }

    result = eventbridge.put_events(Entries=[event])

    if result["FailedEntryCount"] > 0:
        error_entry = result["Entries"][0]
        raise Exception(
            f"Failed to publish EventBridge event: "
            f"{error_entry.get('ErrorCode')} - {error_entry.get('ErrorMessage')}"
        )

    logger.info("EventBridge event sent successfully: %s", json.dumps(payload))


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


def validate_config_for_org(config, filters, sorts, org_id):
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

        section_id = str(section_id)
        valid_section_ids.add(section_id)

        if section_id not in valid_attributes_by_section:
            valid_attributes_by_section[section_id] = set()

        if attribute_id is not None:
            valid_attributes_by_section[section_id].add(str(attribute_id))

    for item in config:
        if "sectionId" not in item:
            raise Exception("sectionId is required in each config item")

        section_id = str(item["sectionId"])
        attribute_ids = item.get("attributeIds", [])

        if section_id not in valid_section_ids:
            raise Exception(f"Invalid sectionId for org_id {org_id}: {section_id}")

        for attribute_id in attribute_ids:
            attribute_id = str(attribute_id)
            if attribute_id not in valid_attributes_by_section.get(section_id, set()):
                raise Exception(
                    f"Invalid attributeId {attribute_id} for sectionId {section_id} and org_id {org_id}"
                )

    for item in filters:
        if "sectionId" not in item or "attributeId" not in item or "operator" not in item:
            raise Exception("Each filter must contain sectionId, attributeId, and operator")

        section_id = str(item["sectionId"])
        attribute_id = str(item["attributeId"])
        operator = str(item["operator"]).strip()

        if section_id not in valid_section_ids:
            raise Exception(f"Invalid filter sectionId for org_id {org_id}: {section_id}")

        if attribute_id not in valid_attributes_by_section.get(section_id, set()):
            raise Exception(
                f"Invalid filter attributeId {attribute_id} for sectionId {section_id} and org_id {org_id}"
            )

        allowed_operators = {"=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"}
        if operator.upper() not in allowed_operators:
            raise Exception(f"Invalid filter operator: {operator}")

    for item in sorts:
        if "sectionId" not in item or "attributeId" not in item:
            raise Exception("Each sort must contain sectionId and attributeId")

        section_id = str(item["sectionId"])
        attribute_id = str(item["attributeId"])
        direction = str(item.get("direction", "asc")).lower()

        if section_id not in valid_section_ids:
            raise Exception(f"Invalid sort sectionId for org_id {org_id}: {section_id}")

        if attribute_id not in valid_attributes_by_section.get(section_id, set()):
            raise Exception(
                f"Invalid sort attributeId {attribute_id} for sectionId {section_id} and org_id {org_id}"
            )

        if direction not in {"asc", "desc"}:
            raise Exception(f"Invalid sort direction: {direction}")


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