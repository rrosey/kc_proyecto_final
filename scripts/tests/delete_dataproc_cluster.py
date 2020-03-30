from google.cloud import dataproc_v1 as dataproc


def delete_cluster(project, region, cluster):
    """Delete the cluster."""
    print("Tearing down cluster.")

    cluster_client = dataproc.ClusterControllerClient(
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    operation = cluster_client.delete_cluster(
        project_id=project, region=region, cluster_name=cluster
    )

    result = operation.result()

    # Output a success message.
    print("Cluster deleted successfully")

    return result


delete_cluster("big-data-keepcoding", "europe-west1", "kc-airbnb-cluster")
