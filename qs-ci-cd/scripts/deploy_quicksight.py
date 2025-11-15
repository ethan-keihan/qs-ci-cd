import json
import sys
import boto3
from botocore.exceptions import ClientError

def load_config(path):
    with open(path) as f:
        return json.load(f)

def load_template_definition(path):
    with open(path) as f:
        return json.load(f)

def upsert_template(qs, cfg, template_definition):
    account_id = cfg["AccountId"]
    region = cfg["Region"]
    template_id = cfg["TemplateId"]
    template_name = cfg["TemplateName"]

    definition = template_definition

    try:
        print(f"Updating template {template_id} in account {account_id}...")
        resp = qs.update_template(
            AwsAccountId=account_id,
            TemplateId=template_id,
            Name=template_name,
            Definition=definition
        )
        print("Template updated. VersionArn:", resp.get("VersionArn"))
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"Template {template_id} not found. Creating...")
            resp = qs.create_template(
                AwsAccountId=account_id,
                TemplateId=template_id,
                Name=template_name,
                Definition=definition
            )
            print("Template created. VersionArn:", resp.get("VersionArn"))
        else:
            print("Error updating/creating template:", e)
            raise

def build_dataset_references(cfg):
    return [
        {
            "DataSetPlaceholder": d["Placeholder"],
            "DataSetArn": d["DataSetArn"]
        }
        for d in cfg["DataSetPlaceholders"]
    ]

def upsert_dashboard(qs, cfg):
    account_id = cfg["AccountId"]
    region = cfg["Region"]
    template_id = cfg["TemplateId"]
    dashboard_id = cfg["DashboardId"]
    dashboard_name = cfg["DashboardName"]

    template_arn = f"arn:aws:quicksight:{region}:{account_id}:template/{template_id}"
    data_set_refs = build_dataset_references(cfg)

    source_entity = {
        "SourceTemplate": {
            "Arn": template_arn,
            "DataSetReferences": data_set_refs
        }
    }

    try:
        print(f"Updating dashboard {dashboard_id}...")
        resp = qs.update_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id,
            Name=dashboard_name,
            SourceEntity=source_entity
        )
        print("Dashboard updated. Status:", resp.get("Status"))

        # publish latest version
        pub = qs.update_dashboard_published_version(
            AwsAccountId=account_id,
            DashboardId=dashboard_id
        )
        print("Dashboard published. VersionArn:", pub.get("DashboardVersionArn"))
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"Dashboard {dashboard_id} not found. Creating...")
            resp = qs.create_dashboard(
                AwsAccountId=account_id,
                DashboardId=dashboard_id,
                Name=dashboard_name,
                SourceEntity=source_entity
            )
            print("Dashboard created. Status:", resp.get("Status"))
        else:
            print("Error updating/creating dashboard:", e)
            raise

def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/deploy_quicksight.py <env>")
        print("Example: python scripts/deploy_quicksight.py dev")
        sys.exit(1)

    env = sys.argv[1]  # dev or prod

    cfg_path = f"config/{env}.json"
    template_path = "templates/sales_template.json"

    cfg = load_config(cfg_path)
    template_definition = load_template_definition(template_path)

    qs = boto3.client("quicksight", region_name=cfg["Region"])

    print(f"Deploying QuickSight assets for env={env}, account={cfg['AccountId']}")

    upsert_template(qs, cfg, template_definition)
    upsert_dashboard(qs, cfg)

if __name__ == "__main__":
    main()