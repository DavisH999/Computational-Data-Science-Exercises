import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.pyplot import plot
import sys
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd


def main():
    datafile = sys.argv[1]
    df_data = pd.read_csv(datafile)
    df_grouped_data = df_data.groupby('name')
    df_data = df_grouped_data['timing'].apply(list).reset_index()
    df_mean = df_grouped_data['timing'].mean().reset_index()
    df_sorted = df_mean.sort_values(by='timing')
    print(df_sorted)

    # print(df_data)
    p_value_anova = stats.f_oneway(df_data.iat[0, 1], df_data.iat[1, 1], df_data.iat[2, 1],
                                   df_data.iat[3, 1], df_data.iat[4, 1], df_data.iat[5, 1], df_data.iat[6, 1]).pvalue
    print('p value of anova test:', p_value_anova)

    pd_new_data = pd.DataFrame(
        {'merge1': df_data.iat[0, 1], 'partition_sort': df_data.iat[1, 1], 'qs1': df_data.iat[2, 1],
         'qs2': df_data.iat[3, 1], 'qs3': df_data.iat[4, 1], 'qs4': df_data.iat[5, 1], 'qs5': df_data.iat[6, 1]})
    pd_melt_data = pd.melt(pd_new_data)
    # print(pd_melt_data)
    post_hoc_tukey_test = pairwise_tukeyhsd(pd_melt_data['value'], pd_melt_data['variable'], alpha=0.05)
    post_hoc_tukey_test.plot_simultaneous()
    # plt.show()
    print(post_hoc_tukey_test)
    print('num of pairs:', len(post_hoc_tukey_test.summary().data) - 1)


if __name__ == '__main__':
    main()
