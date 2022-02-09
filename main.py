import pandas as pd
import numpy as np
import matplotlib as plt


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/



import dask.dataframe as dd
from collections import defaultdict
from dask import persist
ericsson = dd.read_parquet('data.parquet')

ericsson.persist()
ericsson.compute()
ericsson['satellite_class'].value_counts().compute()

teleMetry = ericsson.telemetry_log()
tele_dict = {}
for dictionary in teleMetry:
    for name, value in dictionary.items():
        if value is not None:
            if name not in tele_dict:
                tele_dict[name] = 1
            else:
                tele_dict[name] += 1


tele_dict = defaultdict(lambda : 0)
for dictionary in teleMetry:
    for name, value in dictionary.items():
        if value is not None:
            tele_dict[name] += 1