#pip install requests
#pip install shapely
#pip install pyproj

import requests
import time
import csv
from shapely.geometry import Point
from shapely.ops import transform
from functools import partial
import pyproj
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = 'o8JZi3FYE62PoeraAJAPSiJef7EPsPOW' # Please change it to your API!
BASE_URL = 'https://api.tomtom.com'
VERSION_NUMBER = '1'
MAX_WORKERS = 50 # Adjust it to your API request limits

coordinates = [
    [24.48911, 54.36392], [24.48174, 54.36953], [24.43027, 54.40863],
    [24.34738, 54.50308], [24.32152, 54.63613], [24.21924, 55.73506],
    [24.22604, 55.76603], [23.65244, 53.70409], [23.83569, 52.81053],
    [23.09597, 53.60661], [24.09104, 52.75500], [23.75058, 53.74549],
    [24.03534, 53.88551], [24.40370, 54.51630], [24.42010, 54.57840],
    [24.28646, 54.58908], [24.46684, 55.34308], [24.25936, 55.70507],
    [24.16365, 55.70231], [23.53133, 55.48616]
]


def create_polygon(lat, lon, radius_km):
    proj_wgs84 = pyproj.CRS('EPSG:4326')
    proj_utm = pyproj.CRS(proj='utm', zone=40, ellps='WGS84')
    project_to_utm = partial(pyproj.Transformer.from_crs(proj_wgs84, proj_utm, always_xy=True).transform)
    point_utm = transform(project_to_utm, Point(lon, lat))
    buffer = point_utm.buffer(radius_km * 1000)
    project_to_wgs84 = partial(pyproj.Transformer.from_crs(proj_utm, proj_wgs84, always_xy=True).transform)
    buffer_wgs84 = transform(project_to_wgs84, buffer)
    coords = list(buffer_wgs84.exterior.coords)
    return coords


def create_job_request(lat, lon, date):
    polygon_coords = create_polygon(lat, lon, 1)
    multi_polygon = [[polygon_coords]]
    payload = {
        "jobName": f"Traffic Density Job at ({lat}, {lon}) for {date}",
        "distanceUnit": "KILOMETERS",
        "mapVersion": "2022.12",
        "acceptMode": "AUTO",
        "network": {
            "name": f"network_{lat}_{lon}",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": multi_polygon
            },
            "timeZoneId": "Asia/Dubai",
            "frcs": ["0", "1", "2", "3", "4", "5", "6", "7"],
            "probeSource": "ALL"
        },
        "dateRange": {
            "name": date,
            "from": date,
            "to": date
        },
        "timeSets": [
            {
                "name": "All day",
                "timeGroups": [
                    {
                        "days": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
                        "times": ["00:00-23:59"]
                    }
                ]
            }
        ]
    }
    response = requests.post(
        f'{BASE_URL}/traffic/trafficstats/trafficdensity/{VERSION_NUMBER}?key={API_KEY}',
        json=payload,
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code == 200 and 'jobId' in response.json():
        return response.json()['jobId']
    else:
        print(f"Failed to create job for ({lat}, {lon}) on {date}: {response.json()}")
        return None


def check_job_status(job_id):
    status_response = requests.get(
        f'{BASE_URL}/traffic/trafficstats/status/{VERSION_NUMBER}/{job_id}?key={API_KEY}'
    )
    return status_response.json()


def download_results(job_id, lat, lon, date):
    status_data = check_job_status(job_id)
    job_status = status_data['jobState']
    while job_status not in ['DONE', 'ERROR', 'REJECTED']:
        time.sleep(40)
        status_data = check_job_status(job_id)
        job_status = status_data['jobState']
        print(f'Job status for ({lat}, {lon}) on {date}: {job_status}')

    if job_status == 'DONE':
        result_urls = status_data.get('urls', [])
        if result_urls:
            json_result_url = result_urls[0]
            json_result = requests.get(json_result_url).json()
            return json_result
        else:
            print(f"No result URLs found for job {job_id} on {date}")
            return None
    else:
        print(f'Job for ({lat}, {lon}) on {date} ended with status: {job_status}')
        return None


def process_results(result, lat, lon, date, writer):
    if result:
        car_density = 0
        road_type_totals = {i: 0 for i in range(8)}
        count = 0
        for feature in result.get('features', []):
            properties = feature.get('properties', {})
            if 'segmentProbeCounts' in properties:
                for probe_count in properties['segmentProbeCounts']:
                    frc = properties.get('frc')
                    probe_count_value = probe_count.get('probeCount', 0)
                    if frc is not None:
                        road_type_totals[frc] += probe_count_value
                    car_density += probe_count_value
                    count += 1

        if count > 0:
            avg_density = car_density / count
            row = {
                'date': date,
                'longitude': lon,
                'latitude': lat,
                'car_Density': avg_density,
                'total_car_density': car_density
            }
            row.update({f'road_type_{i}': road_type_totals[i] for i in range(8)})
            writer.writerow(row)


def handle_request(lat, lon, date, writer):
    job_id = create_job_request(lat, lon, date)
    if job_id:
        result = download_results(job_id, lat, lon, date)
        if result:
            process_results(result, lat, lon, date, writer)


start_date = datetime.strptime("2022-10-14", "%Y-%m-%d")
end_date = datetime.strptime("2022-10-14", "%Y-%m-%d")
date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]

csv_file_path = 'traffic_density_data.csv'
with open(csv_file_path, mode='w', newline='') as csv_file:
    fieldnames = ['date', 'longitude', 'latitude', 'car_Density', 'total_car_density'] + [f'road_type_{i}' for i in
                                                                                          range(8)]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for lat, lon in coordinates:
            for date in date_generated:
                date_str = date.strftime("%Y-%m-%d")
                futures.append(executor.submit(handle_request, lat, lon, date_str, writer))

        for future in as_completed(futures):
            future.result()  # To catch exceptions if any

print(f'Results saved to {csv_file_path}')
