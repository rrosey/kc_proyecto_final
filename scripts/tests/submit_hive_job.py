from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.gapic.transports import (
    job_controller_grpc_transport)
import requests
import json
from google.cloud import storage
import io
import csv

# --------------------------------------------------------------------------------------
# SLACK FUNCTIONS
# --------------------------------------------------------------------------------------


def get_slackblock_header(text):
    slackblock = {}
    slackblock["type"] = "section"
    slackblock["text"] = {
        "type": "mrkdwn", "text": text}
    return slackblock


def get_slackblock(title, text, reviews, listing_url, image_url=None):
    slackblock = {}
    slackblock["type"] = "section"
    slackblock["text"] = {
        "type": "mrkdwn", "text": f"*{title}*\n:star::star::star::star: {reviews} reviews\n {text}.\n<{listing_url}|Para más información>"}

    if image_url is not None:
        slackblock["accessory"] = {
            "type": "image", "image_url": image_url, "alt_text": "Sin imagen"}

    # print(slackblock)
    return slackblock


def get_slackblock_divider():
    slackblock = {}
    slackblock["type"] = "divider"
    return slackblock


def read_recommendations():
    """
    0-name
    1-summary
    2-num_poi
    3-num_events
    4-price
    5-number_of_reviews
    6-review_scores_value
    7-listing_url
    8-thumbnail_url
    9-longitude
    10-latitude

    """
    blocks = []
    blocks.append(get_slackblock_header(
        "Hola,\nLas recomendaciones de las casas de *Airbnb* con mejor oferta de ocio son:"))
    blocks.append(get_slackblock_divider())

    storage_client = storage.Client('big-data-keepcoding')
    # get bucket with name
    bucket = storage_client.get_bucket('kc-airbnb')
    # get bucket data as blob
    blob = bucket.get_blob('recommended_listings/000000_0')
    # convert to string
    fin = io.StringIO(blob.download_as_string().decode('utf-8'))

    r = csv.reader(fin, delimiter=";", lineterminator="")
    for row in r:
        #print(row[0], row[1], row[6], row[7])

        title = row[0]
        text = (row[1][:100] + '..') if len(row[1]) > 100 else row[1]
        reviews = row[5]
        listing_url = row[7]
        image_url = row[8]

        blocks.append(get_slackblock(
            title, text, reviews, listing_url, image_url))

    return blocks


def post_message_to_slack(text, blocks=None):
    slack_token = (
        "xoxp-982760551664-984970813334-972673509553-0ed398c91e4423ae67630ae482703081"
    )
    slack_channel = "#ocio"
    slack_icon_url = "https://a.slack-edge.com/production-standard-emoji-assets/10.2/google-large/1f514.png"
    slack_user_name = "Notification agent"

    return requests.post(
        "https://slack.com/api/chat.postMessage",
        {
            "token": slack_token,
            "channel": slack_channel,
            "text": text,
            "icon_url": slack_icon_url,
            "username": slack_user_name,
            "blocks": json.dumps(blocks) if blocks else None,
        },
    ).json()


def send_message():
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    blocks = read_recommendations()
    print(post_message_to_slack(None, blocks=blocks))

# --------------------------------------------------------------------------------------
# SUBMIT HIVE JOBS
# --------------------------------------------------------------------------------------


def submit_job(dataproc_job_client, project, region, cluster_name, bucket_name,
               hql_filename):
    """Submit the Pyspark job to the cluster (assumes `filename` was uploaded
    to `bucket_name."""
    # job_transport = (job_controller_grpc_transport.JobControllerGrpcTransport(
    #    address='{}-dataproc.googleapis.com:443'.format(region)))
    #dataproc_job_client = dataproc_v1.JobControllerClient(job_transport)

    job_details = {
        'placement': {
            'cluster_name': cluster_name
        },
        'hive_job': {
            'query_file_uri': 'gs://{}/{}'.format(bucket_name, hql_filename)
        }
    }

    result = dataproc_job_client.submit_job(
        project_id=project, region=region, job=job_details)
    job_id = result.reference.job_id
    print('Submitted job ID {}.'.format(job_id))
    return job_id


def wait_for_job(dataproc, project, region, job_id):
    """Wait for job to complete or error out."""
    print('Waiting for job to finish...')
    while True:
        job = dataproc.get_job(project, region, job_id)
        # Handle exceptions
        if job.status.State.Name(job.status.state) == 'ERROR':
            raise Exception(job.status.details)
        elif job.status.State.Name(job.status.state) == 'DONE':
            print('Job finished.')
            return job


def http_request(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    region = "europe-west1"
    project = "big-data-keepcoding"

    job_transport = (
        job_controller_grpc_transport.JobControllerGrpcTransport(
            address='{}-dataproc.googleapis.com:443'.format(region)))
    dataproc_job_client = dataproc_v1.JobControllerClient(job_transport)

    job_id = submit_job(dataproc_job_client, project, region, "kc-airbnb-cluster", "kc-airbnb",
                        "sql/load_data.sql")

    wait_for_job(dataproc_job_client, project, region, job_id)

    job_id = submit_job(dataproc_job_client, project, region, "kc-airbnb-cluster", "kc-airbnb",
                        "sql/compute_recommendations.sql")

    wait_for_job(dataproc_job_client, project, region, job_id)

    send_message()

    return 'OK'


http_request(None)
