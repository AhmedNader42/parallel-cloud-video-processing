import boto3
import botocore
import os
from pathlib import Path
from moviepy.editor import VideoFileClip
import argparse


def download_video_segment_from_s3(
    bucket_name, s3_key, local_path, duration_seconds=10
):
    """
    Download a specific duration of a video file from S3 to a local path

    Parameters:
    bucket_name (str): Name of the S3 bucket
    s3_key (str): Path to the video file in S3
    local_path (str): Local path where the video should be saved
    duration_seconds (int): Duration in seconds to download (default: 10)

    Returns:
    bool: True if download was successful, False otherwise
    """
    # Create S3 client
    s3_client = boto3.client("s3")

    # Create directory if it doesn't exist
    Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)

    try:
        file_size_response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        print(file_size_response["ContentLength"])

        # Start with an initial chunk (5MB) to analyze video properties
        initial_bytes = 5 * 1024 * 1024  # 5MB

        # # Get object with range using get_object instead of download_file
        print("Downloading initial chunk for analysis...")
        response = s3_client.get_object(
            Bucket=bucket_name, Key=s3_key, Range=f"bytes=0-{initial_bytes-1}"
        )

        disk_filename = ""
        if ".mp4" in local_path:
            disk_filename = local_path
        else:
            disk_filename = local_path + "chunk.mp4"

        print(disk_filename)
        # Write the initial chunk to temporary file
        with open(disk_filename, "wb") as f:
            f.write(response["Body"].read())

        # # Analyze video properties
        # clip = VideoFileClip(temp_path)
        # bitrate = os.path.getsize(temp_path) / clip.duration  # bytes per second
        # clip.close()

        # # Calculate required bytes for desired duration
        # required_bytes = int(bitrate * duration_seconds)

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("The video does not exist in the specified S3 path")
        else:
            print(f"An error occurred: {str(e)}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return False


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
