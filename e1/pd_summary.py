import pandas as pd

# print(pd.read_csv('totals.csv'))
pd_total = pd.read_csv('totals.csv').set_index(keys=['name'])
pd_count = pd.read_csv('counts.csv').set_index(keys=['name'])
# print(pd_total)
# print(pd_count)

print('City with lowest total precipitation:')
print(pd_total.sum(axis=1).idxmin())

print('Average precipitation in each month:')
print(pd_total.sum(axis=0) / pd_count.sum(axis=0))

print('Average precipitation in each city:')
print(pd_total.sum(axis=1) / pd_count.sum(axis=1))

