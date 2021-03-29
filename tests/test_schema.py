import pandas as pd


def test_can_create_in_memory_sqlite_instance_with_alemic(in_memory_sqlite_datamart_connection):
    df = pd.read_sql("SELECT * FROM release", in_memory_sqlite_datamart_connection)
    assert df.shape[0] > 0, "release table shouldn't be empty"
