from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import os
from dotenv import load_dotenv
import json
load_dotenv()

global zero_sensors
global zero_aware
global frozen_sensors
global DB_TABLES 
global building_name
global Buildings_Query
global config
global client
global current_zero_aware_tables
global current_frozen_tables
global current_zero_tables

current_zero_aware_tables = []
current_frozen_tables = []
current_zero_tables = []

CLUSTER = os.getenv("ADX_CLUSTER") or "https://tpexdataexplorer.westeurope.kusto.windows.net"
kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(CLUSTER)
client = KustoClient(kcsb)

def load_sensor_config():
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        zero_sensors = sum(config.get('Zero-value', {}).values(), [])
        zero_aware = sum(config.get('Zero-aware', {}).values(), [])
        frozen_sensors = sum(config.get('Frozen Sensors', {}).values(), [])
        return zero_sensors, zero_aware, frozen_sensors

    except json.JSONDecodeError as e:
        print(f"Error reading config file: {e}")
        return [], [], []

# Open configuration file
script_dir = os.path.dirname(os.path.abspath(__file__))  
data_path = os.path.join(script_dir, "data", "configuration.json")

#Initial load
with open(data_path, 'r', encoding='utf-8') as f:
    try:
        config = json.load(f)
        DB_TABLES = config.get('DB_TABLES')
        current_zero_aware_tables = [
            {"name": table_name, "sensors": sensors}
            for table_name, sensors in config["Zero-aware"].items()
        ]
        current_frozen_tables = [
            {"name": table_name, "sensors": sensors}
            for table_name, sensors in config["Frozen Sensors"].items()
        ]
        current_zero_tables = [
            {"name": table_name, "sensors": sensors}
            for table_name, sensors in config["Zero-value"].items()
        ]
        zero_sensors = sum(config.get('Zero-value', {}).values(), [])
        zero_aware = sum(config.get('Zero-aware', {}).values(), [])
        frozen_sensors = sum(config.get('Frozen Sensors', {}).values(), [])
        config_last_updated = config.get("List Last Updated")
        Buildings_Query = config.get('Buildings Query')
        building_name = config.get("User Input To Name")
    except json.JSONDecodeError as e:
        print(f"Error reading config file: {e}")

def user_input():
    while True:
        list = ["1", "2", "3", "4", "5", "6"]
        name = input(f"""Which building to check? Choose from the following codes
1: Edge Stadium  , 2: WTC Den Haag
3: NSI           , 4: HNK Scheepsvaart,
5: HNK Sloterdijk, 6: TSH
Press Enter to check all buildings: """)
        if name == '':
            name = None
            break
        elif name in list:
            break
        else:
            print("Invalid building code: Enter correct number")
    
    while True:
        day = input("How many days ago to check? Press Enter to check by default (7): ")
        if day == '':
            day = 7
            break
        try:
            day = int(day)
            break
        except (ValueError, TypeError):
            print("Invalid input: please enter a number.")
    return name, day


def main():
    from pbi_versioncontrol import stale_sensor
    from pbi_versioncontrol import stale_dataset
    from pbi_versioncontrol import previous_flagged_data
    
    name, day = user_input()
    
    #Check dataset level
    stale_dataset.stale_data_check(name)

    #Check for abnormal sensors that are now normal
    updated_frozen = []
    updated_zero = []
    updated_zero_zero = []
    removed_sensors = []

    tables_to_check = [
    ("current_frozen_tables", current_frozen_tables),
    ("current_zero_aware_tables", current_zero_aware_tables),
    ("current_zero_tables", current_zero_tables)
    ]
    
    for condition_name, tables in tables_to_check:
        previous_flagged_data.categorize_flagged_data_conditions(condition_name, tables, day, updated_frozen, updated_zero, updated_zero_zero, removed_sensors)

    stale_sensor.update_config(("Frozen Sensors", updated_frozen))
    stale_sensor.update_config(("Zero-aware", updated_zero))
    stale_sensor.update_config(("Zero-value", updated_zero_zero))

    previous_flagged_data.email_normal_sensors(removed_sensors)

    #Check for all sensors for new broken ones
    load_sensor_config()
    frozen_sensors_new, zeros = stale_sensor.standstill_or_weird_data_check(name, day)
    stale_sensor.update_config(("Frozen Sensors", frozen_sensors_new))
    stale_sensor.update_config(("Zero-aware", zeros))

    
if __name__ == "__main__":
    main()