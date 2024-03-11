import time
from implementations import all_implementations
import numpy as np
import pandas as pd

SIZE_OF_ARRAY = 10000
NUM_OF_TIMES = 50

list_timing_res = []
list_names = []

for i in range(NUM_OF_TIMES):
    random_array = np.random.randint(0, 1000000, SIZE_OF_ARRAY)
    for sort in all_implementations:
        st = time.time()
        res = sort(random_array)
        en = time.time()
        diff = en - st
        list_names.append(sort.__name__)
        list_timing_res.append(diff)

df_timing_data = pd.DataFrame(
    {
        'name': list_names,
        'timing': list_timing_res
    }
)
# print(df_timing_data)

df_timing_data.to_csv('data.csv', index=False)


