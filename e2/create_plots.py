import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def read_csv_from_txt(filename):
    return pd.read_csv(filename, sep=' ', header=None, index_col=1, names=['lang', 'page', 'views', 'bytes'])


pd_file1_data = read_csv_from_txt(sys.argv[1])
pd_file2_data = read_csv_from_txt(sys.argv[2])


pd_file1_data.sort_values(by='views', ascending=False, inplace=True)   # adapted from https://www.geeksforgeeks.org/what-does-inplace-mean-in-pandas/
pd_file2_data.sort_values(by='views', ascending=False, inplace=True)   # adapted from https://www.geeksforgeeks.org/what-does-inplace-mean-in-pandas/
plt.figure(figsize=(10, 5))  # change the size to something sensible
first_plt = plt.subplot(1, 2, 1)  # subplots in 1 row, 2 columns, select the first
second_plt = plt.subplot(1, 2, 2)
# first plot
first_plt.plot(np.arange(len(pd_file1_data['views'].values)), pd_file1_data['views'].values)
first_plt.set_title('Popularity Distribution')
first_plt.set_xlabel('Rank')
first_plt.set_ylabel('Views')

# second plot

inner_joined_data = pd_file1_data.merge(pd_file2_data, how='inner', left_on='page', right_on='page')  #left_on and right_on should be the col we joined
second_plt.plot(inner_joined_data['views_x'], inner_joined_data['views_y'], 'b.', alpha=1)
# plt.xscale('log')
# plt.yscale('log')
second_plt.set_xscale('log')
second_plt.set_yscale('log')
second_plt.set_title('Hourly Correlation')
second_plt.set_xlabel('Hour 1 views')
second_plt.set_ylabel('Hour 2 views')

plt.savefig('wikipedia.png')


