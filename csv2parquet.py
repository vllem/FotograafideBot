import pandas as pd 
import sys

csv = sys.argv[1]
parquet = sys.argv[2]

df = pd.read_csv(csv)
df.to_parquet(parquet)

