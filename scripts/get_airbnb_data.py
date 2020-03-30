import urllib.request
import csv
from google.cloud import storage

# ---------------------------------------------------------
# urllib3
# google-cloud-storage==1.26.0
# requests
# ---------------------------------------------------------


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


def remove_newlines(inputfile, outputfile):
    print(f"Processing {inputfile} ...")
    with open(inputfile, "r", newline="", encoding="utf8") as fin:
        with open(outputfile, "w", newline="", encoding="utf8") as fout:
            r = csv.reader(fin, delimiter=";", lineterminator="\r\n")
            w = csv.writer(fout, delimiter=";",
                           quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in r:
                row = [col.replace("\r\n", "**") for col in row]
                row = [col.replace("\n", "**") for col in row]
                # print(f"{row}")
                w.writerow(row)


def get_data():
    """
    Recoge los datos de de Airbnb publicados en public.opendatasoft.com
    Prepara el fichero para que pueda ser importado en Hive
        - Elimina los caracteres de final de linea que se encuentren en el propio texto.
    """

    bucket_name = "kc-airbnb"
    source_file_name = "/tmp/airbnb-listings-lite.csv"
    source_file_name_preproc = "/tmp/airbnb-listings-lite_preproc.csv"
    blob_name = "datasets/airbnb-listings-lite_preproc.csv"

    url = "https://public.opendatasoft.com/explore/dataset/airbnb-listings/download/?format=csv&disjunctive.host_verifications=true&disjunctive.amenities=true&disjunctive.features=true&q=madrid&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    urllib.request.urlretrieve(url, source_file_name)
    # with open(source_file_name, "w", encoding="utf8") as fout:
    # get request
    #print("Downloading Airbnb dataset ...")
    #response = urllib.request.urlopen(url)

    # print(response.read().decode('latin1'))

    # fout.write(response.read().decode('utf8'))

    remove_newlines(source_file_name, source_file_name_preproc)

    upload_blob(bucket_name, source_file_name_preproc, blob_name)
