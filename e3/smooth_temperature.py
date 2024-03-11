import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pykalman import KalmanFilter
from statsmodels.nonparametric.smoothers_lowess import lowess


def read_csv_from_text(filename):
    # timestamp, temperature, sys_load_1, cpu_percent, fan_rpm.
    return pd.read_csv(filename, parse_dates=['timestamp'])


df_sysinfo = read_csv_from_text(sys.argv[1])
df_sysinfo['time_float64'] = (df_sysinfo['timestamp'] - df_sysinfo['timestamp'].min()).dt.total_seconds()

plt.figure(figsize=(12, 4))
plt.plot(df_sysinfo['timestamp'], df_sysinfo['temperature'], 'b.', alpha=0.5)
# plt.plot(df_sysinfo['timestamp'], df_sysinfo['cpu_percent'], 'b.', alpha=0.5)
# plt.plot(df_sysinfo['timestamp'], df_sysinfo['sys_load_1'], 'b.', alpha=0.5)
# plt.plot(df_sysinfo['timestamp'], df_sysinfo['fan_rpm'], 'b.', alpha=0.5)
# plt.savefig('fan_rpm.svg')

# LOESS
loess_smoothed = lowess(df_sysinfo['temperature'], df_sysinfo['time_float64'], frac=0.065)
# print(loess_smoothed)
plt.plot(df_sysinfo['timestamp'], loess_smoothed[:, 1], 'r-')

# KALMAN
df_kalman_data = df_sysinfo[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]

initial_state = df_kalman_data.iloc[0]
observation_covariance = np.diag([1.5, 0.01, 0.5, 12]) ** 2  # TODO: shouldn't be zero
transition_covariance = np.diag([0.5, 30, 0.001, 40]) ** 2  # TODO: shouldn't be zero
transition_matrices = [[0.98, 0.5, 0.2, -0.001],
                       [0.1, 0.4, 2.1, 0],
                       [0, 0, 0.98, 0],
                       [0, 0, 0, 1]]  # TODO: shouldn't (all) be zero

kalman_smoothed, _ = KalmanFilter(
    initial_state_mean=initial_state,
    observation_covariance=observation_covariance,
    transition_covariance=transition_covariance,
    transition_matrices=transition_matrices
).smooth(df_kalman_data)

plt.plot(df_sysinfo['timestamp'], kalman_smoothed[:, 0], 'g-')

plt.legend(['Temperature', 'Loess Smoothing', 'Kalman Smoothing'])

plt.savefig('cpu.svg')
