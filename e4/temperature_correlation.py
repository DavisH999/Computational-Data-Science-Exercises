import pathlib
import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plot


def distance_between_two_positions(lat_1, lon_1, lat_2, lon_2):
    # This function is adapted from https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
    radius = 6371
    diff_lats = np.deg2rad(lat_2 - lat_1)
    diff_lons = np.deg2rad(lon_2 - lon_1)

    s1 = (np.power(np.sin(diff_lats / 2), 2) +
          np.cos(np.deg2rad(lat_1)) * np.cos(np.deg2rad(lat_2)) *
          np.sin(diff_lons / 2) * np.sin(diff_lons / 2))

    s2 = np.arctan2(np.sqrt(s1), np.sqrt(1 - s1)) * 2
    s3 = radius * s2
    return s3


def distance(df_a_city, df_stations):
    return distance_between_two_positions(df_a_city['latitude'], df_a_city['longitude'],
                                          df_stations['latitude'], df_stations['longitude'])


def best_tmax(df_a_city, df_stations):
    # print(df_a_city)
    df_distance = distance(df_a_city, df_stations)
    index_closest_distance = df_distance.idxmin()
    df_line = df_stations.iloc[index_closest_distance]
    return df_line['avg_tmax']


def main():
    stations_file = pathlib.Path(sys.argv[1])
    city_file = sys.argv[2]
    output_file = sys.argv[3]

    df_stations = pd.read_json(stations_file, lines=True)
    df_stations['avg_tmax'] = df_stations['avg_tmax'] / 10

    df_cities = pd.read_csv(city_file)
    df_cities = df_cities.dropna()
    df_cities['area'] = df_cities['area'] / pow(10, 6)
    df_cities = df_cities[df_cities['area'] <= pow(10, 4)].reset_index()
    df_cities['population_density'] = df_cities['population'] / df_cities['area']
    # best_tmax is function name, df_stations=df_station is parameters mapping, axis=1 is operating on row
    df_cities['closest_avg_temp'] = df_cities.apply(best_tmax, df_stations=df_stations, axis=1)

    plot.plot(df_cities['closest_avg_temp'], df_cities['population_density'], 'b.', alpha=0.7)
    plot.title('Temperature vs Population Density')
    plot.xlabel('Avg Max Temperature (\u00b0C)')
    plot.ylabel('Population Density (people/km\u00b2)')
    plot.savefig(output_file)


if __name__ == '__main__':
    main()
