import os
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from dotenv import load_dotenv

load_dotenv()


def main():
# Configuration
    username = os.getenv("GRAPH_API_CLIENT_ID")
    password = os.getenv("GRAPH_API_CLIENT_SECRET") 
    authority_id = os.getenv("GRAPH_API_TENANT_ID") 
    
    CLUSTER = os.getenv("ADX_CLUSTER") or "https://tpexdataexplorer.westeurope.kusto.windows.net"
    

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    CLUSTER,
    username,
    password,
    authority_id
    )

    client = KustoClient(kcsb)

    # Define database-table mapping
    DB_TABLES = {
        "edgestadium-data": ["ED_Energy", "ED_Occupancy", "ED_Thermal", "ED_Water", "ED_Wellbeing", "TDemo", "Test_Table"],
        "hnkscheepsvaart-data": ["HNK_Scheepsvaart"],
        "hnksloterdijk-data": ["HNK_Sloterdijk"],
        "tshhub-data": ["TSH_Occupancy"],
        "wtcdenhaag-data": ["BI_Energy", "BI_Occupancy", "BI_Wellbeing"],
        "nsihqrgcenterpoint-data": ["NSI_Wellbeing", "NSI_Energy", "NSI_Occupancy"]
    }

    for db, tables in DB_TABLES.items():
        for table in tables:
            QUERY = f".set-or-replace {table} <| Create_{table}"
            try:
                response = client.execute(db, QUERY)
                for row in response.primary_results[0]:
                    print(row)
            except KustoServiceError as ex:
                print(f"Query failed for {table} in {db}: {ex}")

if __name__ == "__main__":
    main()
