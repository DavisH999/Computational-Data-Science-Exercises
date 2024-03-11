import numpy as np

data = np.load('monthdata.npz')
totals = data['totals']
counts = data['counts']
print(totals)
# print(counts)
print('Row with lowest total precipitation:')
print(np.argmin(totals.sum(axis=1)))

print('Average precipitation in each month:')
print(totals.sum(axis=0) / counts.sum(axis=0))

print('Average precipitation in each city:')
print(totals.sum(axis=1) / counts.sum(axis=1))

print('Quarterly precipitation totals:')
# print(np.array([1,2,3,4,5,6,7,8,9,10,11,12]).reshape(-1,3))
np_reshaped_tot = totals.reshape((-1, 3))
print(np_reshaped_tot)
print(np_reshaped_tot.sum(axis=1).reshape(-1, 4))




