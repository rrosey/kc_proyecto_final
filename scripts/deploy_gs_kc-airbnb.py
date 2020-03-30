from google.cloud import storage


def create_bucket(bucket_name):
    """Creates a new bucket."""

    storage_client = storage.Client()
    # Set properties on a plain resource object.
    bucket = storage_client.bucket(bucket_name)
    bucket.location = "europe-west1"
    bucket.storage_class = "STANDARD"
    bucket.create()

    print("Bucket {} created".format(bucket.name))


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))


def delete_folder(bucket_name, folder):
    """Deletes a folder from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder)
    for blob in blobs:
        blob.delete()

    print("Folder {} deleted.".format(folder))


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


def upload_datasets_test(bucket_name):

    # delete_folder(bucket_name, "datasets")

    # Para subirlo a una carpeta del bucket se lo indicaremos en el blob name.
    file_name = "datasets/airbnb-listings-lite_preproc.csv"
    blob_name = "datasets/airbnb-listings-lite_preproc.csv"
    upload_blob(bucket_name=bucket_name,
                source_file_name=file_name, destination_blob_name=blob_name)

    file_name = "datasets/events.csv"
    blob_name = "datasets/events.csv"
    upload_blob(bucket_name=bucket_name,
                source_file_name=file_name, destination_blob_name=blob_name)

    file_name = "datasets/poi.csv"
    blob_name = "datasets/poi.csv"
    upload_blob(bucket_name=bucket_name,
                source_file_name=file_name, destination_blob_name=blob_name)


def main():

    bucket_name = "kc-airbnb"
    # create_bucket(bucket_name)

    upload_datasets_test(bucket_name)

    file_name = "scripts/sql/load_data.sql"
    blob_name = "sql/load_data.sql"
    upload_blob(bucket_name=bucket_name,
                source_file_name=file_name, destination_blob_name=blob_name)

    file_name = "scripts/sql/compute_recommendations.sql"
    blob_name = "sql/compute_recommendations.sql"
    upload_blob(bucket_name=bucket_name,
                source_file_name=file_name, destination_blob_name=blob_name)

    file_name = "datasets/neighbourhood.csv"
    blob_name = "datasets/neighbourhood.csv"
    upload_blob(bucket_name=bucket_name,
                source_file_name=file_name, destination_blob_name=blob_name)

    print("Upload completo")


if __name__ == "__main__":
    main()
