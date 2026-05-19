import duckdb

conn = duckdb.connect("data/warehouse.duckdb")

print("\n=== TABLES ===")
print(conn.sql("show tables").df())

print("\n=== DAILY ACTIVE USERS ===")
print(conn.sql("select * from mart_dau limit 10").df())

print("\n=== CURRENT USERS ===")
print(conn.sql("select * from dim_user_current limit 10").df())