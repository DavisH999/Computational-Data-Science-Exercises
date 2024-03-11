import pandas as pd
# Create a sample DataFrame
data = {'A': [1, 2, 3, 4, 5],
        'B': [10, 20, 30, 40, 50]}
print(data)
df = pd.DataFrame(data)
grouped = df.groupby('A')

# Calculate the sum and mean of column 'B' within each group using agg() or aggregate()
result1 = grouped['B'].agg(['sum', 'mean'])
result2 = grouped['B'].aggregate(['sum', 'mean'])

print(result1)
print(result2)