from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name):
    """This sample walks a user through creating a Cloud Dataproc cluster
       using the Python client library.

       Args:
           project_id (string): Project to use for creating resources.
           region (string): Region where the resources should live.
           cluster_name (string): Name to use for creating a cluster.
    """

    # Create a client with the endpoint set to the desired cluster region.
    # dataproc.ClusterControllerClient.from_service_account_json()
    cluster_client = dataproc.ClusterControllerClient(
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-1"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-1"},
            "config_bucket": "kc-airbnb",
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(project_id, region, cluster)
    result = operation.result()

    # Output a success message.
    print("Cluster created successfully: {}".format(result.cluster_name))


create_cluster("big-data-keepcoding", "europe-west1", "kc-airbnb-cluster")
