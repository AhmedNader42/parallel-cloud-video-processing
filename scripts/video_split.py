import boto3
import botocore
import os
from pathlib import Path
import argparse
import threading
import math

# Create S3 client
s3_client = boto3.client("s3")


def assign_worker_ranges_fair(no_of_workers, per_worker_bytes):
    worker_ranges = [{"start_bytes": 0, "end_bytes": per_worker_bytes}]
    for i in range(1, no_of_workers):
        worker_ranges.append(
            {
                "start_bytes": worker_ranges[i - 1]["end_bytes"],
                "end_bytes": (i + 1) * per_worker_bytes,
            }
        )

        print(
            "Worker "
            + str(i)
            + " assigned ranges "
            + str(worker_ranges[i - 1]["end_bytes"])
            + " and ending at "
            + str((i + 1) * per_worker_bytes)
        )

    print(worker_ranges)
    return worker_ranges


def get_video_chunk(range_start, range_end):
    try:
        print("bytes=" + str(math.floor(range_start)) + "-" + str(math.ceil(range_end)))
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=s3_key,
            Range="bytes="
            + str(math.floor(range_start))
            + "-"
            + str(math.ceil(range_end)),
        )
        write_chunk_to_disk(
            response=response, range_start=range_start, range_end=range_end
        )

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("The video does not exist in the specified S3 path")
        else:
            print(f"An error occurred: {str(e)}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return False


def transform_video_threaded(worker_ranges):
    threads = []

    for each in worker_ranges:
        t = threading.Thread(
            target=get_video_chunk,
            args=(each["start_bytes"], each["end_bytes"]),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
        print(t)


def write_chunk_to_disk(response, range_start, range_end):
    range_start = range_start // 1024
    range_end = range_end // 1024

    disk_filename = (
        local_path + str(int(range_start)) + "to" + str(int(range_end)) + ".mp4"
    )
    print("Writing file at specified path: " + disk_filename)
    print(response)

    # Write the initial chunk to temporary file
    with open(disk_filename, "wb") as f:
        f.write(response["Body"].read())

    # if ".mp4" in local_path:
    #     disk_filename = local_path
    # else:


def download_video_segment_from_s3(bucket_name, s3_key, local_path, no_of_workers=10):
    """
    Download a specific duration of a video file from S3 to a local path

    Parameters:
    bucket_name (str): Name of the S3 bucket
    s3_key (str): Path to the video file in S3
    local_path (str): Local path where the video should be saved

    Returns:
    bool: True if download was successful, False otherwise
    """
    # Create directory if it doesn't exist
    Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)

    file_size_response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
    file_size_bytes = file_size_response["ContentLength"]

    print("Total file size : " + str(file_size_bytes))
    print("Splitting the file among " + str(no_of_workers) + " workers")
    per_worker_bytes = file_size_bytes / no_of_workers

    print("Each worker will process " + str(per_worker_bytes) + " bytes of data")
    worker_ranges = assign_worker_ranges_fair(no_of_workers, per_worker_bytes)
    worker_ranges = [worker_ranges[1]]

    transform_video_threaded(worker_ranges=worker_ranges)
    # # Get object with range using get_object instead of download_file
    print("Downloading initial chunk for analysis...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "bucket_name", help="The S3 bucket name where the file is stored"
    )
    parser.add_argument(
        "s3_key",
        help="The S3 key for the file. Written as complete_filename.mp4",
    )
    parser.add_argument(
        "--local_path",
        help="Path where chunk file will be saved on local machine.",
        default="/tmp/",
    )
    args = parser.parse_args()
    bucket_name = args.bucket_name
    s3_key = args.s3_key
    local_path = args.local_path

    print(args.bucket_name)
    print(args.s3_key)
    print(args.local_path)

    success = download_video_segment_from_s3(bucket_name, s3_key, local_path)
