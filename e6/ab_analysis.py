import sys
import pandas as pd
from matplotlib.pyplot import plot
from scipy.stats import chi2_contingency
from scipy.stats import mannwhitneyu



OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def control_treatment(df):
    df_odd_searchdata = df[df['uid'] % 2 != 0].reset_index()
    df_even_searchdata = df[df['uid'] % 2 == 0].reset_index()
    return df_odd_searchdata, df_even_searchdata


def search_count_category(df):
    df_at_least_once = df[df['search_count'] > 0].reset_index()
    df_never_search = df[df['search_count'] == 0].reset_index()
    return len(df_at_least_once), len(df_never_search)


def main():
    searchdata_file = sys.argv[1]
    df_searchdata = pd.read_json(searchdata_file, orient='records', lines=True)
    # for all people
    df_odd_searchdata, df_even_searchdata = control_treatment(df_searchdata)
    len_at_least_once_on_odd_group, len_never_search_on_odd_group = search_count_category(df_odd_searchdata)
    len_at_least_once_on_even_group, len_never_search_on_even_group = search_count_category(df_even_searchdata)
    p_value_chi2_all = chi2_contingency([[len_at_least_once_on_odd_group, len_never_search_on_odd_group],
                           [len_at_least_once_on_even_group, len_never_search_on_even_group]]).pvalue
    p_value_mannwhitneyu_all = mannwhitneyu(df_odd_searchdata['search_count'], df_even_searchdata['search_count'],
                                            alternative='two-sided').pvalue
    # print(p_value_chi2_all)
    # print(p_value_mannwhitneyu_all)

    # for only instructors
    df_searchdata_i = df_searchdata[df_searchdata['is_instructor']]
    df_odd_searchdata_i, df_even_searchdata_i = control_treatment(df_searchdata_i)
    len_at_least_once_on_odd_group_i, len_never_search_on_odd_group_i = search_count_category(df_odd_searchdata_i)
    len_at_least_once_on_even_group_i, len_never_search_on_even_group_i = search_count_category(df_even_searchdata_i)
    p_value_chi2_i = chi2_contingency([[len_at_least_once_on_odd_group_i, len_never_search_on_odd_group_i],
                                        [len_at_least_once_on_even_group_i, len_never_search_on_even_group_i]]).pvalue
    p_value_mannwhitneyu_i = mannwhitneyu(df_odd_searchdata_i['search_count'], df_even_searchdata_i['search_count'],
                                            alternative='two-sided').pvalue

    # print(p_value_chi2_i)
    # print(p_value_mannwhitneyu_i)


    # print(df_searchdata)
    # print(df_odd_searchdata)
    # print(df_even_searchdata)
    # print(df_at_least_once_searchdata)
    # print(df_never_search__searchdata)


    # ...

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=p_value_chi2_all,
        more_searches_p=p_value_mannwhitneyu_all,
        more_instr_p=p_value_chi2_i,
        more_instr_searches_p=p_value_mannwhitneyu_i,
    ))


if __name__ == '__main__':
    main()
