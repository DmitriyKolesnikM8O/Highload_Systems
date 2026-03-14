import pandas as pd
from sqlalchemy import create_engine
import os

engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

files = [f"./data/MOCK_DATA{f}.csv" for f in ["", " (1)", " (2)", " (3)", " (4)", " (5)", " (6)", " (7)", " (8)", " (9)"]] 

for f in files:
    print(f"Loading {f}...")
    df = pd.read_csv(f)
    df.to_sql('mock_data', engine, if_exists='append', index=False)
