import duckdb
from src.config import DB_PATH

con = duckdb.connect(str(DB_PATH))

print(con.execute("DESCRIBE raw_listings").fetchall())

con.close()