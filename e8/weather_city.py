import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler, StandardScaler, FunctionTransformer
from sklearn.ensemble import GradientBoostingClassifier


def main():
    # Parameters
    df_monthly_labelled = pd.read_csv(sys.argv[1])
    df_monthly_unlabelled = pd.read_csv(sys.argv[2])
    output_file_name = sys.argv[3]

    # Train
    df_monthly_labelled_temp = df_monthly_labelled.copy()
    X = df_monthly_labelled_temp.drop(['city', 'year'], axis=1)
    y = df_monthly_labelled_temp['city']
    X_train, X_valid, y_train, y_valid = train_test_split(X, y)
    model = make_pipeline(StandardScaler(),
                          GradientBoostingClassifier(n_estimators=50, max_depth=3, min_samples_leaf=0.1),
                          )
    model.fit(X_train, y_train)

    # Predict
    X_unlabelled_temp = df_monthly_unlabelled.copy()
    X_unlabelled_temp = X_unlabelled_temp.drop(['city', 'year'], axis=1)
    y_predict = model.predict(X_unlabelled_temp)
    df_monthly_unlabelled['city'] = y_predict
    pd.Series(y_predict).to_csv(output_file_name, index=False, header=False)
    print(model.score(X_valid, y_valid))

    # df = pd.DataFrame({'truth': y_valid, 'prediction': model.predict(X_valid)})
    # print(df[df['truth'] != df['prediction']])


if __name__ == '__main__':
    main()
