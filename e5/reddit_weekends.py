import sys
import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
from datetime import date

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]
    df_reddit_counts = pd.read_json(reddit_counts, lines=True)

    df_reddit_counts = df_reddit_counts[((df_reddit_counts['date'].dt.year == 2012) |
                                         (df_reddit_counts['date'].dt.year == 2013)) &
                                        (df_reddit_counts['subreddit'] == 'canada')].reset_index(drop=True)

    df_reddit_count_weekend = df_reddit_counts[(df_reddit_counts['date'].dt.weekday == 5) |
                                               (df_reddit_counts['date'].dt.weekday == 6)].reset_index(drop=True)

    df_reddit_count_weekday = df_reddit_counts[(df_reddit_counts['date'].dt.weekday == 0) |
                                               (df_reddit_counts['date'].dt.weekday == 1) |
                                               (df_reddit_counts['date'].dt.weekday == 2) |
                                               (df_reddit_counts['date'].dt.weekday == 3) |
                                               (df_reddit_counts['date'].dt.weekday == 4)].reset_index(drop=True)
    # print(df_reddit_count_weekday['comment_count'].mean())
    # print(df_reddit_count_weekend['comment_count'].mean())
    # initial data
    # p < 0.05, reject, not same means
    p_value_t_test = stats.ttest_ind(df_reddit_count_weekend['comment_count'],
                                     df_reddit_count_weekday['comment_count']).pvalue
    # p < 0.05, reject, not Normal
    p_value_normal_test_weekday = stats.normaltest(df_reddit_count_weekday['comment_count']).pvalue
    # p < 0.05, reject, not Normal
    p_value_normal_test_weekend = stats.normaltest(df_reddit_count_weekend['comment_count']).pvalue
    # p < 0.05, reject, not equal variance
    p_value_levene_test = stats.levene(df_reddit_count_weekday['comment_count'],
                                       df_reddit_count_weekend['comment_count']).pvalue

    # plt.hist((df_reddit_count_weekend['comment_count'], df_reddit_count_weekday['comment_count']))
    # plt.show()

    # Fix 1: transforming data might save us.
    # plt.hist((np.sqrt(df_reddit_count_weekend['comment_count']), np.sqrt(df_reddit_count_weekday['comment_count'])))
    # plt.show()

    p_value_normal_test_weekday_transformed = stats.normaltest(np.sqrt(df_reddit_count_weekday['comment_count'])).pvalue
    p_value_normal_test_weekend_transformed = stats.normaltest(np.sqrt(df_reddit_count_weekend['comment_count'])).pvalue
    p_value_levene_test_transformed = stats.levene(np.sqrt(df_reddit_count_weekend['comment_count']),
                                                   np.sqrt(df_reddit_count_weekday['comment_count'])).pvalue

    # print(stats.ttest_ind(np.sqrt(df_reddit_count_weekend['comment_count']),
    #                       np.sqrt(df_reddit_count_weekday['comment_count'])).pvalue)

    # Fix 2: the Central Limit Theorem might save us.

    year_weekday = df_reddit_count_weekday['date'].dt.isocalendar().year
    week_weekday = df_reddit_count_weekday['date'].dt.isocalendar().week
    df_group_reddit_count_weekday = df_reddit_count_weekday[['date', 'comment_count']]
    df_group_reddit_count_weekday = df_group_reddit_count_weekday.groupby([year_weekday, week_weekday]).mean()
    p_value_normal_test_weekday_weekly = stats.normaltest(df_group_reddit_count_weekday['comment_count']).pvalue

    year_weekend = df_reddit_count_weekend['date'].dt.isocalendar().year
    week_weekend = df_reddit_count_weekend['date'].dt.isocalendar().week
    df_group_reddit_count_weekend = df_reddit_count_weekend[['date', 'comment_count']]
    df_group_reddit_count_weekend = df_group_reddit_count_weekend.groupby([year_weekend, week_weekend]).mean()
    p_value_normal_test_weekend_weekly = stats.normaltest(df_group_reddit_count_weekend['comment_count']).pvalue

    p_value_levene_test_weekly = stats.levene(df_group_reddit_count_weekday['comment_count'],
                                              df_group_reddit_count_weekend['comment_count']).pvalue
    p_value_t_test_weekly = stats.ttest_ind(df_group_reddit_count_weekday['comment_count'],
                                            df_group_reddit_count_weekend['comment_count']).pvalue

    # Fix 3: a non-parametric test might save us.
    p_value_mannwhitneyu_test = stats.mannwhitneyu(x=df_reddit_count_weekday['comment_count'],
                                                   y=df_reddit_count_weekend['comment_count'],
                                                   alternative='two-sided').pvalue

    # print(df_reddit_count_weekend)
    # print(df_reddit_count_weekday)
    # print(df_reddit_counts)

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=p_value_t_test,
        initial_weekday_normality_p=p_value_normal_test_weekday,
        initial_weekend_normality_p=p_value_normal_test_weekend,
        initial_levene_p=p_value_levene_test,
        transformed_weekday_normality_p=p_value_normal_test_weekday_transformed,
        transformed_weekend_normality_p=p_value_normal_test_weekend_transformed,
        transformed_levene_p=p_value_levene_test_transformed,
        weekly_weekday_normality_p=p_value_normal_test_weekday_weekly,
        weekly_weekend_normality_p=p_value_normal_test_weekend_weekly,
        weekly_levene_p=p_value_levene_test_weekly,
        weekly_ttest_p=p_value_t_test_weekly,
        utest_p=p_value_mannwhitneyu_test,
    ))


if __name__ == '__main__':
    main()
