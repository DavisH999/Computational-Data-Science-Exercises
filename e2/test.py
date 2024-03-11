import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

countries = pd.DataFrame({
    'country': ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola'],
    'population': [38928346, 2877797, 43851044, 77265, 32866272],
    'area': [652230, 28748, 2381741, 468, 1246700]
})

continents = pd.DataFrame({
    'country': ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola'],
    'continent': ['Asia', 'Europe', 'Africa', 'Europe', 'Africa']
})
print("countries")
print(countries)
print("continents")
print(continents)

df = continents.merge(countries, how='inner').groupby('continent').aggregate('sum').reset_index()
q1 = df[['continent', 'area']]
print('df')
print(df)
print('q1')
print(q1)

# print(newdf)

q2 = continents.merge(countries)[['country', 'area']]
print('q2')
print(q2)
q2 = df.rename(columns={'area': 'total_sum'}).merge(q2, on='country', how='outer')
print(q2)

