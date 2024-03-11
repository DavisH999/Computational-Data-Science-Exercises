import numpy as np
import pandas as pd
import os
import sys
import pathlib
import xml.dom.minidom as minidom


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation, parse
    xmlns = 'http://www.topografix.com/GPX/1/0'

    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.10f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.10f' % (pt['lon']))
        time = doc.createElement('time')
        time.appendChild(doc.createTextNode(pt['datetime'].strftime("%Y-%m-%dT%H:%M:%SZ")))
        trkpt.appendChild(time)
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    doc.documentElement.setAttribute('xmlns', xmlns)

    with open(output_filename, 'w') as fh:
        fh.write(doc.toprettyxml(indent='  '))


def get_data(input_gpx):
    list_lat = []
    list_lon = []
    list_ele = []
    list_time = []

    doc_gpx = minidom.parse(str(input_gpx))
    list_trkpt = doc_gpx.getElementsByTagName('trkpt')
    for trkpt in list_trkpt:
        list_lat.append(trkpt.getAttribute('lat'))
        list_lon.append(trkpt.getAttribute('lon'))
        list_ele.append(trkpt.getElementsByTagName('ele')[0].firstChild.data)
        list_time.append(trkpt.getElementsByTagName('time')[0].firstChild.data)

    df_gps = pd.DataFrame(
        {
            'lat': list_lat,
            'lon': list_lon,
            'ele': list_ele,
            'timestamp': list_time
        }
    )
    df_gps['lat'] = df_gps['lat'].astype(np.float64)
    df_gps['lon'] = df_gps['lon'].astype(np.float64)
    df_gps['ele'] = df_gps['ele'].astype(np.float64)
    df_gps['timestamp'] = pd.to_datetime(df_gps['timestamp'])

    return df_gps


def group_data(df):
    # round df['timestamp'] and group by
    # the timestamp will be changed to the closest time that is a multiple of 4 seconds.
    df['timestamp'] = df['timestamp'].round('4S')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.groupby(['timestamp']).mean().reset_index()
    return df


def main():
    # read data
    input_dir = pathlib.Path(sys.argv[1])
    output_dir = pathlib.Path(sys.argv[2])
    df_accl = pd.read_json(input_dir / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    df_gps = get_data(input_dir / 'gopro.gpx')
    df_phone = pd.read_csv(input_dir / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    # I SPENT 3 HRS TRYING TO FIND THE BUG AND FINALLY MOVED THE LINE TO THIS LOCATION.
    first_time = df_accl['timestamp'].min()
    # group accl
    df_accl = group_data(df_accl)
    df_gps = group_data(df_gps)

    list_tuples = []
    # first_time = df_accl['timestamp'].min()  # WRONG POSITION
    for offset in np.linspace(-5, 5, 101):
        # assume we had offset
        # because we are trying all offset, we use new copy of phone in order to avoid data contamination
        df_new_phone = df_phone.copy()
        df_new_phone = df_new_phone[['time', 'gFx']]  # only need two col
        df_new_phone['timestamp'] = first_time + pd.to_timedelta(df_phone['time'] + offset, unit='sec')
        df_new_phone['timestamp'] = pd.to_datetime(df_new_phone['timestamp'])  # convert to datetime
        df_new_phone['gFx'] = df_new_phone['gFx'].astype(np.float64)
        df_new_phone = group_data(df_new_phone)  # round 4
        # merge them
        df_accl['x'] = df_accl['x'].astype(np.float64)
        df_new_phone = df_new_phone.merge(df_accl, how='inner', on='timestamp')
        # np.correlate(df_new_phone['gFx'], df_new_phone['x']) will produce [-3.2]
        dot_product = np.dot(df_new_phone['gFx'], df_new_phone['x'])
        temp_tuple = (dot_product, offset)
        list_tuples.append(temp_tuple)

    np_tuples = np.array(list_tuples)
    # print(best_offset)
    index = np.argmax(np_tuples[:, 0])
    a_tuple = np_tuples[index]
    best_offset = a_tuple[1]
    # add best offset to data. so boot phone first and then GoPro.
    df_phone['timestamp'] = first_time + pd.to_timedelta(df_phone['time'] + best_offset, unit='sec')
    df_phone = group_data(df_phone)
    df_combined = df_phone.merge(df_accl, how='inner', on='timestamp').merge(df_gps, how='inner', on='timestamp')
    df_combined = df_combined.rename(columns={'timestamp': 'datetime'})
    # print and output the file
    print(f'Best time offset: {best_offset:.1f}')
    os.makedirs(output_dir, exist_ok=True)
    output_gpx(df_combined[['datetime', 'lat', 'lon']], output_dir / 'walk.gpx')
    df_combined[['datetime', 'Bx', 'By']].to_csv(output_dir / 'walk.csv', index=False)


main()
