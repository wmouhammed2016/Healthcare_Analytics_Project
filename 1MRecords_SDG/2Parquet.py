import pandas as pd
df = pd.read_csv('./output/encounters_1m.csv')
df.to_parquet('./output/encounters_1m.parquet', engine='pyarrow', compression='snappy')