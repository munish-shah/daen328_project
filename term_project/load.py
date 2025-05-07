import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# load .env, build engine…
load_dotenv()
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# 1) DROP & RECREATE
drop_sql = "DROP TABLE IF EXISTS inspections CASCADE;"
create_sql = """
CREATE TABLE inspections (
    inspection_id BIGINT PRIMARY KEY,
    dba_name VARCHAR(255),
    aka_name VARCHAR(255),
    license_ BIGINT,
    facility_type VARCHAR(255),
    risk VARCHAR(100),
    address TEXT,
    city VARCHAR(100),
    state CHAR(2),
    zip VARCHAR(10),
    inspection_date DATE,
    inspection_type VARCHAR(255),
    results VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    violations TEXT,
    facility_type_clean VARCHAR(255),
    violations_list TEXT[],
    violation_num_list INTEGER[]
);
"""

with engine.begin() as conn:
    conn.execute(text(drop_sql))
    conn.execute(text(create_sql))
    print("✔ Table 'inspections' dropped and recreated.")

# 2) Load your cleaned CSV
import ast
df = pd.read_csv("data/cleaned_chicago_data.csv")
df["violations_list"] = df["violations_list"].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])
df["violation_num_list"] = df["violation_num_list"].apply(
    lambda x: list(map(int, ast.literal_eval(x))) if pd.notna(x) and x != 'nan' else []
)

df.to_sql("inspections", engine, if_exists="append", index=False)
print("✔ Data loaded into 'inspections'.")