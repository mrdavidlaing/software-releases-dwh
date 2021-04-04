from collections import namedtuple

DatawarehouseInfo = namedtuple(
    "DatawarehouseInfo",
    "datalake_uri datalake_schema datamart_schema connection execute_sql add_to_lake replace_partition"
)
