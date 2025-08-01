from provisioning.fetch import get_provisioning_records
from weekly_eh_check.email_utils import email_operation
import pandas as pd
import json
import numpy as np
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoApiError
import os
from dotenv import load_dotenv
from datetime import datetime
from pbi_versioncontrol.validation import zero_sensors, zero_aware, frozen_sensors, DB_TABLES, building_name, Buildings_Query, config, config_last_updated, data_path
load_dotenv()
    
CLUSTER = os.getenv("ADX_CLUSTER") or "https://tpexdataexplorer.westeurope.kusto.windows.net"
kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(CLUSTER)
client = KustoClient(kcsb)

def extract_between_number_and_quote(s, num):
    s = s[num:]
    if ',' in s:
        return s.split('"')[0] 
    return s

def get_sensor_info():
    # For Edge Stadium
    raw_sheet = get_provisioning_records(sheet_name="Edge Stadium Provisioning Sheet")
    raw_sheet_df = pd.DataFrame(raw_sheet)
    raw_sheet_df = raw_sheet_df.merge(raw_sheet_df, how='left', left_on='Location', right_on='Unique identifier', suffixes=(None, '_right'))
    raw_sheet_df = raw_sheet_df.merge(raw_sheet_df, how = 'left', left_on = 'Location_right', right_on = 'Unique identifier_right', suffixes = (None, "_right"))
    EDGE_sensors_df = raw_sheet_df[~raw_sheet_df['Entity type'].isin(['RecBuilding', 'Floor', 'Space'])][['Unique identifier', 'Additional information', 'Additional information_right_right']]
    EDGE_sensors_df['Additional information'] = EDGE_sensors_df['Additional information'].apply(lambda s: extract_between_number_and_quote(s, 17))
    EDGE_sensors_df['Additional information_right_right'] = EDGE_sensors_df['Additional information_right_right'].apply(lambda s: extract_between_number_and_quote(s, 10))
    EDGE_sensors_df = EDGE_sensors_df.rename(columns={'Additional information': 'Name', 'Additional information_right_right': 'Floor'})
    EDGE_sensors_df.drop_duplicates(inplace = True)
    list = ['AirQualityIndexSensor', 'PM2.5Sensor', 'SoundPressureLevelSensor', 'CO2Sensor', 'HumiditySensor', 'AirTemperatureSensor', 'VOCSensor']
    temp_df = []
    for _, row in EDGE_sensors_df.iterrows():
        val = row['Unique identifier']
        original_name = row['Name']
        floor = row['Floor']
        if 'Well sensor' in str(val):
            for item in list:  
                temp_df.append({
                    'Unique identifier': val,
                    'Name': item,
                    'Floor': floor
            })
        else:
            temp_df.append({
                'Unique identifier': val,
                'Name': original_name,
                'Floor': floor
            })
    EDGE_sensors_df = pd.DataFrame(temp_df)

    EDGE_sensors_df['Building'] = 'Edge Stadium'

    # For WTC Den Haag      
    raw_sheet = get_provisioning_records(sheet_name="WTC Den Haag Provisioning Sheet")
    raw_sheet_df = pd.DataFrame(raw_sheet)
    raw_sheet_df = raw_sheet_df.merge(raw_sheet_df, how='left', left_on='Location', right_on='Unique identifier', suffixes=(None, '_right'))
    WTC_sensors_df = raw_sheet_df[~raw_sheet_df['Entity type'].isin(['Building', 'Level', 'Zone'])][['Entity type', 'Unique identifier', 'Location', 'Location_right']]
    WTC_sensors_df['Entity type'] = WTC_sensors_df['Entity type'].apply(lambda s: extract_between_number_and_quote(s, 6))
    WTC_sensors_df['Location'] = WTC_sensors_df['Entity type'] + ' - ' + WTC_sensors_df['Location']
    WTC_sensors_df = WTC_sensors_df[['Unique identifier', 'Location', 'Location_right']]
    WTC_sensors_df = WTC_sensors_df.rename(columns={'Location': 'Name','Location_right': 'Floor'})
    WTC_sensors_df.drop_duplicates(inplace = True)
    WTC_sensors_df['Building'] = 'WTC Den Haag'

    # For NSI
    raw_sheet = get_provisioning_records(sheet_name="NSI Digital Twin Provisioning Sheet")
    raw_sheet_df = pd.DataFrame(raw_sheet)
    NSI_sensors_df = raw_sheet_df[raw_sheet_df['Entity type'].isin(['ObixMultiSensor', 'SingleElectricalMeterWithActiveEnergyDelivered'])][['Unique identifier']]
    list = ['PM10Sensor', 'PM2.5Sensor', 'AirTemperatureSensor', 'AirQualitySensor', 'HumiditySensor', 'LightIntensitySensor', 'TVOCSensor']
    temp_df = []
    for val in NSI_sensors_df['Unique identifier']:
        if str(val).isdigit():
            for item in list:
                temp_df.append({'Unique identifier': val, 'Name': item})
        else:
            temp_df.append({'Unique identifier': val, 'Name': 'Energy Sensor'})
    NSI_sensors_df = pd.DataFrame(temp_df)
    NSI_sensors_df['Floor'] = 'Level 0'
    NSI_sensors_df['Building'] = 'NSI'

    sensor_df = pd.concat([EDGE_sensors_df, WTC_sensors_df, NSI_sensors_df], ignore_index= True)
    
    return sensor_df

def check(db, tables, day):
    zeros = []
    frozen_sensors_new = []
    frozen_update_sensors = []

    for table in tables:
        sensor_ids = []
        try:
            QUERY = f"{table} | distinct SensorID"
            response = client.execute(db, QUERY)
            sensors = response.primary_results[0]
            sensor_ids = [item["SensorID"] for item in sensors]
        except KustoServiceError as ex:
            try:
                QUERY = f"{table} | distinct SensorId"
                response = client.execute(db, QUERY)
                sensors = response.primary_results[0]
                sensor_ids = [item["SensorID"] for item in sensors]
            except KustoServiceError as ex:
                print(f"Query failed for {table} in {db} - No SensorID error: {ex}")
                continue

        for sensor in sensor_ids:
            if table not in {'ED_Wellbeing', 'NSI_Wellbeing'}:
                key = ''
                values = []
                if table != 'NSI_Occupancy':
                    QUERY = f"{table} | where SensorID == '{sensor}' | order by TimeStamp desc | take {day} * 24"
                else:
                    QUERY = f"{table} | where SensorID == '{sensor}' | order by TimeStamp desc | take {day} * 24 * 12"
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
                    if all_equal:
                        if values[0] == 0 and sensor not in zero_aware and sensor not in zero_sensors:
                            zeros.append([sensor, table, key])
                        elif values[0] != 0 and sensor in frozen_sensors:
                            frozen_update_sensors.append([sensor, table, key])
                        elif values[0] != 0 and sensor not in frozen_sensors:
                            frozen_sensors_new.append([sensor, table, key])

            else:
                Key = []
                try:
                    QUERY = f"{table} | where SensorID == '{sensor}' | distinct Key"
                    response = client.execute(db, QUERY)
                    result = response.primary_results[0]
                    Key = [item["Key"] for item in result]
                except KustoApiError:
                    QUERY = f"{table} | where SensorID == '{sensor}' | distinct Id"
                    response = client.execute(db, QUERY)
                    result = response.primary_results[0]
                    Key = [item["Id"] for item in result]

                for examed_key in Key:
                    values = []
                    try:
                        QUERY = f"{table} | where SensorID == '{sensor}' and Key == '{examed_key}' | order by TimeStamp desc | take {day} * 24"
                        response = client.execute(db, QUERY)
                    except KustoApiError:
                        QUERY = f"{table} | where SensorID == '{sensor}' and Id == '{examed_key}' | order by TimeStamp desc | take {day} * 24"
                        response = client.execute(db, QUERY)
                    returned = response.primary_results[0]

                    try:
                        values = [item["Value"] for item in returned]
                    except KeyError:
                        print(f"Failed to retrieve values to check for: {sensor}, {table}, {db}. No 'Value'")
                        continue

                    if values:
                        key_all_equal = all(x == values[0] for x in values)
                        if key_all_equal:
                            if values[0] == 0 and sensor not in zero_aware and sensor not in zero_sensors:
                                zeros.append([sensor, table, examed_key])
                            elif values[0] != 0 and sensor in frozen_sensors:
                                frozen_update_sensors.append([sensor, table, examed_key])
                            elif values[0] != 0 and sensor not in frozen_sensors:
                                frozen_sensors_new.append([sensor, table, examed_key])

    return zeros, frozen_sensors_new, frozen_update_sensors


def standstill_or_weird_data_check(building_code, day):
    zeros = []
    frozen_sensors_new = []
    frozen_update_sensors = []

    def accumulate(result):
        nonlocal zeros, frozen_sensors_new, frozen_update_sensors
        z, fsn, fus = result
        zeros += z
        frozen_sensors_new += fsn
        frozen_update_sensors += fus

    if building_code is None:
        building = None
        for db, tables in DB_TABLES.items():
            result = check(db, tables, day)
            accumulate(result)
    else:
        building = building_name.get(building_code)
        db = Buildings_Query.get(building)
        tables = config['DB_TABLES'].get(db, []) if db else []
        result = check(db, tables, day)
        accumulate(result)
    
    Status = [f"{day} days with zero values every hour", 'Known frozen values, needs updating on the table', f"Values standing still for {day} days"]
    Order = [zeros, frozen_update_sensors, frozen_sensors_new]
    df = pd.DataFrame()

    for i in range(3):
        name = Order[i]
        status = Status[i]
        temp_df = pd.DataFrame(name, columns=['SensorID', 'OriginalTable', 'Sensor Type'])
        if not temp_df.empty:
            temp_df['Status'] = status
            df = pd.concat([df, temp_df], ignore_index=True)
    

    sensor_info = get_sensor_info()
    
    multi_df = df[df['OriginalTable'].isin(['ED_Wellbeing', 'NSI_Wellbeing', 'NSI_Occupancy'])]
    multi_df = multi_df.merge(sensor_info, how = 'left', left_on=['SensorID', 'Sensor Type'], right_on=['Unique identifier', 'Name'])

    non_multi_df = df[~df['OriginalTable'].isin(['ED_Wellbeing', 'NSI_Wellbeing', 'NSI_Occupancy'])]
    non_multi_df = non_multi_df.merge(sensor_info, how = 'left', left_on= 'SensorID', right_on= 'Unique identifier')

    df = pd.concat([multi_df, non_multi_df], ignore_index= True)
    df = df[['SensorID', 'OriginalTable', 'Status', 'Name', 'Floor', 'Building']]

    # NSI Occupancy was part of the multi sensor from NSI, but had its own table in ADX. Due to data structure, rows with NSI Occupancy cannot join dictionary
    # So we are just filling in manually
    df.loc[df['OriginalTable'].str.contains('NSI_Occupancy', na=False), 'Floor'] = 'Level 0'
    df.loc[df['OriginalTable'].str.contains('NSI_Occupancy', na=False), 'Building'] = 'NSI'

    if building is not None:
        df = df[df['Building'] == building]
    
    df['ADX Query'] = (
    df['OriginalTable'] + 
    " | where SensorID == \"" + df['SensorID'].astype(str) + 
    np.where(
        df['OriginalTable'].isin(['ED_Wellbeing', 'NSI_Wellbeing']),
        " and Id == \"" + df['Name'].astype(str) + "\"",
        ""
    ) +
    "\" | order by TimeStamp desc | take " + str(day * 24)
    )
    
    today = datetime.now()

    if not df.empty:
        email_operation("data validation", f"Abnormal sensor data warning {building}", 
                                        f"""Date of reference: {today}
Number of days from the past in reference: {day}

See attachment for sensors that should be checked. Reference sheet last updated on {config_last_updated}

Clarification:
    - Zero values every hour: Sensor's recorded value is always 0 for the past {day} days. Historically (Past 2 months) it is not an unused sensor (value not always 0)
    - Known frozen values: We found out that the sensors is not properly sending data for ingestion in DT, and should check up on them.
    - Values standing still: Sensor has been sending the same value every hour to the DT for the past X day, signifying that the sensor's data is not properly communicated with DT.
                                        """, 
                                        {"data validation": "k.chu@tpex.com"}, df)
    return frozen_sensors_new, zeros

def update_config(labeled_table):
    label, lst = labeled_table
    sensors_to_add = [item[:2] for item in lst]
    old_table = config.get(label)

    #Add new sensors
    for sensor, table in sensors_to_add:
        if table not in old_table:
            old_table[table] = []
        if sensor not in old_table[table]:
            old_table[table].append(sensor)

    #Remove sensors
    sensors_to_keep = {}
    for sensor, table in sensors_to_add:
        sensors_to_keep.setdefault(table, set()).add(sensor)
    for table, sensors in old_table.items():
        keep_set = sensors_to_keep.get(table, set())
        old_table[table] = [s for s in sensors if s in keep_set]

    config[label] = old_table
    with open(data_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)
