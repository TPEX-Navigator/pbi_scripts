from weekly_eh_check.email_utils import email_operation
from azure.kusto.data.exceptions import KustoServiceError
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from pbi_versioncontrol.validation import DB_TABLES, Buildings_Query, building_name, client, config
load_dotenv()

def stale_data_check(building_code):
    dataset_to_check = []
    timestamp = []

    def check(tables):
        for table in tables:
                QUERY = f"{table} | order by TimeStamp desc | take 1"
                try:
                    response = client.execute(db, QUERY)

                    latest_timestamp = response.primary_results[0][0]["TimeStamp"]
                    now_utc = datetime.now(timezone.utc)

                    if latest_timestamp + timedelta(days=2) < now_utc:
                        if table in {"NSI_Energy"}:
                            print("NSI Energy Skipped. NSI only send a CSV for us to ingest every month")
                        else:
                            dataset_to_check.append(table)
                            timestamp.append(latest_timestamp.strftime("%d %b %Y %H:%M"))
                except KustoServiceError as ex:
                    print(f"Query failed for {table} in {db}: {ex}")
        return dataset_to_check, timestamp

    if building_code is None:
        building = None
        for db, tables in DB_TABLES.items():
            operation = check(tables)
            dataset_to_check = operation[0]
            timestamp = operation[1]
    else:
        building = building_name.get(building_code)
        db = Buildings_Query.get(building)
        if db: 
            tables = config['DB_TABLES'].get(db, [])
        else:
            tables = []
        operation = check(tables)
        dataset_to_check = operation[0]
        timestamp = operation[1]

    if dataset_to_check and timestamp:
        email_operation("data validation", f"Dataset stale warning {building if building is not None else "- All Buildings"} ", 
                                        f"""The following datasets in PowerBI has not been updated for at least 2 days (& Their respective timestamp):
{dataset_to_check}. 
{timestamp} 
Update the table by going to ADX, run .set_or_replace table with Create_ command, OR run databaseupdate.py script in PBI-versioncontrol github.""", 
                                        {"data validation": "k.chu@tpex.com"}, None)
    else:
        print(f"All dataset checked up to date for all buildings")