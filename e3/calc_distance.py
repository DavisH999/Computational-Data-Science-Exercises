import sys
import numpy as np
import pandas as pd
from pykalman import KalmanFilter
import xml.dom.minidom as minidom


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation

    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.7f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.7f' % (pt['lon']))
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def distance_between_two_positions(lat_1, lon_1, lat_2, lon_2):
    # This function is adapted from https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
    radius = 6371000
    diff_lats = np.deg2rad(lat_2 - lat_1)
    diff_lons = np.deg2rad(lon_2 - lon_1)

    s1 = (np.power(np.sin(diff_lats / 2), 2) +
          np.cos(np.deg2rad(lat_1)) * np.cos(np.deg2rad(lat_2)) *
          np.sin(diff_lons / 2) * np.sin(diff_lons / 2))

    s2 = np.arctan2(np.sqrt(s1), np.sqrt(1 - s1)) * 2
    s3 = radius * s2
    return s3


def distance(df):
    # create a new copy of the df
    new_df = df.copy()
    new_df['shifted_lat'] = new_df['lat'].shift(periods=-1)
    new_df['shifted_lon'] = new_df['lon'].shift(periods=-1)
    new_df = new_df.dropna()
    ret = distance_between_two_positions(new_df['lat'], new_df['lon'], new_df['shifted_lat'], new_df['shifted_lon'])

    return ret.sum()


def main():
    # Read the XML
    input_gpx = sys.argv[1]
    input_csv = sys.argv[2]

    list_lat = []
    list_lon = []
    list_date = []
    doc_gpx = minidom.parse(input_gpx)
    list_trkpt = doc_gpx.getElementsByTagName('trkpt')

    for trkpt in list_trkpt:
        # get data
        list_lat.append(trkpt.getAttribute('lat'))
        list_lon.append(trkpt.getAttribute('lon'))
        list_date.append(trkpt.getElementsByTagName('time')[0].firstChild.data)

    # convert the datatype
    df_points = pd.DataFrame(
        {
            'lat': list_lat,
            'lon': list_lon,
            'datetime': list_date
        }
    )
    df_points['lat'] = df_points['lat'].astype(np.float64)
    df_points['lon'] = df_points['lon'].astype(np.float64)
    df_points['datetime'] = pd.to_datetime(df_points['datetime'], utc=True)

    # Read the CSV and Combine
    df_points = df_points.set_index('datetime')
    df_senor = pd.read_csv(input_csv, parse_dates=['datetime']).set_index('datetime')
    df_points['Bx'] = df_senor['Bx']
    df_points['By'] = df_senor['By']

    # Calculate Distances
    dist = distance(df_points)
    print(f'Unfiltered distance: {dist:.2f}')

    # Kalman Filtering
    df_kalman = df_points[['lat', 'lon', 'Bx', 'By']]
    initial_value_guess = df_kalman.iloc[0]
    observation_covariance = np.diag([5/pow(10, 5), 5/pow(10, 5), 5, 5]) ** 2
    transition_covariance = np.diag([0.00002, 0.00002, 1.5, 1.5]) ** 2
    transition_matrix = [[1, 0, 5 * pow(10, -7), 33 * pow(10, -7)],
                         [0, 1, -48 * pow(10, -7), 9 * pow(10, -7)],
                         [0, 0, 1, 0],
                         [0, 0, 0, 1]]

    np_pred_state, state_cov = KalmanFilter(
        initial_state_mean=initial_value_guess,
        observation_covariance=observation_covariance,
        transition_covariance=transition_covariance,
        transition_matrices=transition_matrix
    ).smooth(df_points)

    np_smoothed_points = np_pred_state  # pred_state is numpy not dataframe!
    """
    ONE WAY TO BUILD DF FROM NUMPY
    pd.DataFrame(
        {
            'lat': np_smoothed_points[:, 0],
            'lon': np_smoothed_points[:, 1],
            'Bx': np_smoothed_points[:, 2],
            'By': np_smoothed_points[:, 3],

        }
    )
    """
    df_smoothed_points = pd.DataFrame(np_smoothed_points, columns=['lat', 'lon', 'Bx', 'By'])
    smoothed_dist = distance(df_smoothed_points)
    print(f'Filtered distance: {smoothed_dist:.2f}')

    # Viewing Your Results
    output_gpx(df_smoothed_points, 'out.gpx')


if __name__ == '__main__':
    main()

