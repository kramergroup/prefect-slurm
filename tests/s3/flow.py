import os

from prefect import flow, task
from prefect_aws.s3 import S3Bucket


@task(name="Download asset from bucket")
def download(bucketname, src):

    s3_bucket = S3Bucket.load(bucketname)
    s3_bucket.download_object_to_path(src, "data")


@task(name="Upload asset to bucket")
def upload(bucketname, dest):

    s3_bucket = S3Bucket.load(bucketname)
    s3_bucket.upload_from_path("data", dest)


@task(name="Clean-up")
def cleanup():
    os.remove("data")


@flow(log_prints=True)
def test_flow(bucketname, source_id, target_id):
    download(bucketname, source_id)
    upload(bucketname, target_id)
    cleanup()


if __name__ == "__main__":

    test_flow.serve(
        name="test-s3-flow-dev",
        tags=["test"],
        description="Test s3 buckets",
        parameters={},
    )
    # test_flow.from_source(
    #     source=str(Path(__file__).parent), entrypoint="test_flow.py:test_flow"
    # ).deploy(
    #     work_pool_name="hsuper-slurm-dev",
    #     name="test-flow",
    # )
