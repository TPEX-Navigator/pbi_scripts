from weekly_eh_check.email_utils import email_operation
from azure.kusto.data.exceptions import KustoApiError
import pandas as pd
from dotenv import load_dotenv
from pbi_versioncontrol.validation import DB_TABLES, client
load_dotenv()

def find_dataset_by_table(db_tables, table_name):
    for dataset_name, tables in db_tables.items():
        if table_name in tables:
            return dataset_name
    return None

def get_variable_name(var, scope):
    for name, val in scope.items():
        if val is var:
            return name
    return None

def categorize_flagged_data_conditions(condition_name, tables, day, updated_frozen, updated_zero, updated_zero_zero, removed_sensors):
    for table in tables:
        name = table["name"]
        db = find_dataset_by_table(DB_TABLES, name)
        for sensor in table["sensors"]:
            if table not in ['ED_Wellbeing', 'NSI_Wellbeing']:
                values = []
                if table != 'NSI_Occupancy':
                    QUERY = f"{name} | where SensorID == '{sensor}' | order by TimeStamp desc | take {day} * 24"
                else:
                    QUERY = f"{name} | where SensorID == '{sensor}' | order by TimeStamp desc | take {day} * 24 * 12"
                response = client.execute(db, QUERY)
                returned = response.primary_results[0]
                try:
                    values = [item["Max"] for item in returned]
                except KeyError:
                    try:
                        values = [item["people_count"] for item in returned]
                    except KeyError:
                        try:
                            values = [item["Difference"] for item in returned]
                        except KeyError:
                            print(f"Failed to retrieve values to check for: {sensor}, {table}, {db}. No 'Max', 'people_count', or 'Difference'")
                            continue
            
                if values:
                    all_equal = all(x == values[0] for x in values)
                    if not all_equal:
                        removed_sensors.append([sensor, name, "", condition_name])
                    elif all_equal:
                        if condition_name == "current_frozen_tables":
                            updated_frozen.append([sensor, name])
                        elif condition_name == "current_zero_aware_tables":
                            updated_zero.append([sensor, name])
                        elif condition_name == "current_zero_tables":
                            updated_zero_zero.append([sensor, name])

            else:
                Key = []
                try:
                    QUERY = f"{name} | where SensorID == '{sensor}' | distinct Key"
                    response = client.execute(db, QUERY)
                    result = response.primary_results[0]
                    Key = [item["Key"] for item in result]
                except KustoApiError:
                    QUERY = f"{name} | where SensorID == '{sensor}' | distinct Id"
                    response = client.execute(db, QUERY)
                    result = response.primary_results[0]
                    Key = [item["Id"] for item in result]

                for examed_key in Key:
                    values = []
                    try:
                        QUERY = f"{name} | where SensorID == '{sensor}' and Key == '{examed_key}' | order by TimeStamp desc | take {day} * 24"
                        response = client.execute(db, QUERY)
                    except KustoApiError:
                        QUERY = f"{name} | where SensorID == '{sensor}' and Id == '{examed_key}' | order by TimeStamp desc | take {day} * 24"
                        response = client.execute(db, QUERY)
                    returned = response.primary_results[0]

                    try:
                        values = [item["Value"] for item in returned]
                    except KeyError:
                        print(f"Failed to retrieve values to check for: {sensor}, {name}, {db}. No 'Value'")
                        continue

                    if values:
                        all_equal = all(x == values[0] for x in values)
                    if not all_equal:
                        removed_sensors.append([sensor, name, examed_key, condition_name])
                    elif all_equal:
                        if condition_name == "current_frozen_tables":
                            updated_frozen.append([sensor, name])
                        elif condition_name == "current_zero_aware_tables":
                            updated_zero.append([sensor, name])
                        elif condition_name == "current_zero_tables":
                            updated_zero_zero.append([sensor, name])



def email_normal_sensors(removed_sensors):
    for i in range(len(removed_sensors)):
        label = removed_sensors[i][-1]
        if label == "current_frozen_tables":
            removed_sensors[i][-1] = "Used to be frozen, but no longer"
        elif label == "current_zero_aware_tables":
            removed_sensors[i][-1] = "Used to be measured, have zeros for a while, but now has measures again"
        elif label == "current_zero_tables":
            removed_sensors[i][-1] = "Used to be always zero, but has started to measure something"

    df = pd.DataFrame(removed_sensors, columns = ['SensorID', 'OriginalTable', 'Key', 'Removal Reason'])

    if not df.empty:
        email_operation("data validation", f"Sensors that returned to normal", 
                                        f"""See attachment for sensors that was flagged as abnormal, and has now started to measure again.
Would be nice to run query and check them out to make sure.
                                        """, 
                                        {"data validation": "k.chu@tpex.com"}, df)
    return df