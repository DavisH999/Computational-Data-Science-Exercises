import os
import pathlib
import sys
import numpy as np
import pandas as pd
import xml.dom.minidom as md


# Mohammad Parsaei - Assignment 4

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
    # TODO: you may use your code from exercise 3 here.

    """
        Adopted from https://www.youtube.com/watch?v=r6dyk68gymk
    """

    # reading the gpx file content
    gpxContent = []
    with open(input_gpx, 'r') as gpx_file:
        gpxContent.append(gpx_file.read())

    gpxContent = ''.join(gpxContent)
    # parsing the string read from gpx file
    xml_file = md.parseString(gpxContent)

    trkpt_elements = xml_file.getElementsByTagName('trkpt')
    # latitudes = longitude = times = list() Don't do that, then all the lists are pointing the same
    longitude = list()
    times = list()
    latitudes = list()
    for track_point in trkpt_elements:
        latitudes.append(float(track_point.getAttribute('lat')))
        longitude.append(float(track_point.getAttribute('lon')))
        times.append(track_point.getElementsByTagName('time')[0].firstChild.data)

    data = pd.DataFrame()
    data['lat'] = latitudes
    data['lon'] = longitude
    data['timestamp'] = times
    data['timestamp'] = pd.to_datetime(data['timestamp'], format='mixed', utc=True)
    data.set_index('timestamp')
    return data


def find_offset(phone_arg, accl_arg, first_time_arg):
    """
    takes original phone df and 4 second bin grouped accl df and the first time of accl
    Finds the best offsets,
    """
    highest_cross_correlation = None
    best_offset = None
    accl_clone = accl_arg.copy()
    for offset in np.linspace(-5.0, 5.0, 101):
        phone_arg['timestamp'] = first_time_arg + pd.to_timedelta(phone_arg['time'] + offset, unit='sec')
        phone_arg['timestamp'] = phone_arg['timestamp'].dt.round('4S')
        clone_df = phone_arg.groupby(['timestamp']).mean().reset_index()
        '''
        This code bellow was wrong because the I used the reset_index so 
        accl_clone['gFx'] = clone_df['gFx']
        clone_df = accl_clone[accl_clone['gFx'].notnull()]
        i = clone_df['x'].dot(clone_df['gFx'])'''

        merged = accl_clone.merge(clone_df, how='inner', on='timestamp')
        i = merged['x'].dot(merged['gFx'])
        if highest_cross_correlation is None or highest_cross_correlation < i:
            highest_cross_correlation = i
            best_offset = offset
    return best_offset


def main():
    input_directory = pathlib.Path(sys.argv[1])
    output_directory = pathlib.Path(sys.argv[2])

    accl = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    gps = get_data(input_directory / 'gopro.gpx')
    phone = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    # TODO: create "combined" as described in the exercise

    # Applying 4 second bin and grouping on gps and accl data frames
    first_time = accl['timestamp'].min()
    gps['timestamp'] = gps['timestamp'].dt.round('4S')
    accl['timestamp'] = accl['timestamp'].dt.round('4S')
    accl = accl.groupby(['timestamp']).mean().reset_index()
    gps = gps.groupby(['timestamp']).mean().reset_index()

    # Finding the best offset
    best_offset = find_offset(phone, accl, first_time)

    phone['timestamp'] = first_time + pd.to_timedelta(phone['time'] + best_offset, unit='sec')
    phone['timestamp'] = phone['timestamp'].dt.round('4S')
    phone = phone.groupby(['timestamp']).mean().reset_index()

    combined = accl.merge(phone, how='inner', on='timestamp')
    combined = combined.merge(gps, how='inner', on='timestamp')
    combined['datetime'] = combined['timestamp']
    # print(combined)
    print(f'Best time offset: {best_offset:.1f}')
    os.makedirs(output_directory, exist_ok=True)
    output_gpx(combined[['datetime', 'lat', 'lon']], output_directory / 'walk.gpx')
    combined[['datetime', 'Bx', 'By']].to_csv(output_directory / 'walk.csv', index=False)


main()
