import pandas as pd

# Your provided dataframes
countries = pd.DataFrame({
    'country': ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola'],
    'population': [38928346, 2877797, 43851044, 77265, 32866272],
    'area': [652230, 28748, 2381741, 468, 1246700]
})

continents = pd.DataFrame({
    'country': ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola'],
    'continent': ['Asia', 'Europe', 'Africa', 'Europe', 'Africa']
})

# Merging countries and continents dataframes
merged_df = countries.merge(continents, on='country')

# Calculating the total area and population for each continent
total = merged_df.groupby('continent').sum().reset_index()
import pandas as pd

# Your provided dataframes
countries = pd.DataFrame({
    'country': ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola'],
    'population': [38928346, 2877797, 43851044, 77265, 32866272],
    'area': [652230, 28748, 2381741, 468, 1246700]
})

continents = pd.DataFrame({
    'country': ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola'],
    'continent': ['Asia', 'Europe', 'Africa', 'Europe', 'Africa']
})

# Merging countries and continents dataframes
merged_df = countries.merge(continents, on='country')

# Calculating the total area and population for each continent
total = merged_df.groupby('continent').sum().reset_index()



# Merging the total dataframe with the merged_df dataframe
result = merged_df.merge(total)
print(result)

# Calculating the continent_fraction
result['continent_fraction'] = result['area'] / result['area_total']

# Selecting the required columns for the final result
final_result = result[['continent', 'country', 'population', 'area', 'area_total', 'continent_fraction']]


print(total)


# Merging the total dataframe with the merged_df dataframe
result = merged_df.merge(total, on='continent', suffixes=('', '_total'))

# Calculating the continent_fraction
result['continent_fraction'] = result['area'] / result['area_total']

# Selecting the required columns for the final result
final_result = result[['continent', 'country', 'population', 'area', 'area_total', 'continent_fraction']]

