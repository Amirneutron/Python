import dask.dataframe as dd
from dask import persist

data = 'data.parquet'

ericssonData = dd.read_parquet(data).head(n=150)
print('Reading...')
saved_Data = ericssonData.persist()
#print(saved_Data.compute())ju
print('File stored in memory')

print(saved_Data.compute())






