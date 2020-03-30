import requests
import csv
from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )


def create_csv_poi(neigbourhood_file, poi_file):
    # Define Foursquare Credentials
    CLIENT_ID = 'xxx'  # your Foursquare ID
    # your Foursquare Secret
    CLIENT_SECRET = 'xxx'
    VERSION = '20200301'
    LIMIT = 50
    RADIUS = 5000  # metros
    print('Your credentails:')
    print('CLIENT_ID: ' + CLIENT_ID)
    print('CLIENT_SECRET:' + CLIENT_SECRET)
    venues_list = []

    with open(neigbourhood_file, "r") as fin:
        with open(poi_file, "w") as fout:
            csv_in = csv.DictReader(fin, delimiter=';')
            csv_out = csv.writer(fout, delimiter=";")
            csv_out.writerow(['Neighbourhood',
                              'Neighbourhood Latitude',
                              'Neighbourhood Longitude',
                              'Venue',
                              'Venue Latitude',
                              'Venue Longitude',
                              'Venue Category'])

            for row in csv_in:
                name = row['neighbourhood']
                lon = row['longitude']
                lat = row['latitude']
                print(name)

                # create the API request URL
                url = 'https://api.foursquare.com/v2/venues/explore?&client_id={}&client_secret={}&v={}&ll={},{}&radius={}&limit={}'.format(
                    CLIENT_ID,
                    CLIENT_SECRET,
                    VERSION,
                    lat,
                    lon,
                    RADIUS,
                    LIMIT)

                # make the GET request
                results = requests.get(url).json()[
                    "response"]['groups'][0]['items']

                # return only relevant information for each nearby venue
                venues_list = ([(
                    name,
                    lat,
                    lon,
                    v['venue']['name'],
                    v['venue']['location']['lat'],
                    v['venue']['location']['lng'],
                    v['venue']['categories'][0]['name']) for v in results])

                csv_out.writerows(venues_list)


def execute():

    tmp_neighbourhood_file = "/tmp/neighbourhood.csv"
    tmp_poi_file = "/tmp/poi.csv"

    bucket_name = "kc-airbnb"
    neighbourhood_blob_name = "datasets/neighbourhood.csv"
    poi_blob_name = "datasets/poi.csv"

    download_blob(bucket_name, neighbourhood_blob_name, tmp_neighbourhood_file)
    create_csv_poi(tmp_neighbourhood_file, tmp_poi_file)
    upload_blob(bucket_name, tmp_poi_file, poi_blob_name)
