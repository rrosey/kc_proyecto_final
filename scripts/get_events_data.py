from google.cloud import storage
import urllib.request


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


def get_data():
    """
    Recoge los datos de eventos publicados en datos.madrid.es
    Convierte el fichero a utf8
    """

    bucket_name = "kc-airbnb"
    source_file_name = "/tmp/events.csv"
    blob_name = "datasets/events.csv"

    url = 'https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.csv'
    with open(source_file_name, "w", encoding="utf8") as file:
        # get request
        print("Downloading event dataset ...")
        response = urllib.request.urlopen(url)
        # print(response.read().decode('latin1'))
        file.write(response.read().decode('latin1'))

    upload_blob(bucket_name, source_file_name, blob_name)
