import boto3
import botocore
import os
from pathlib import Path
from moviepy.editor import VideoFileClip

def download_video_segment_from_s3(bucket_name, s3_key, local_path, duration_seconds=10):
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
    s3_client = boto3.client('s3')

    # Create directory if it doesn't exist
    Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)

    try:
        # First, download a small portion to analyze the video
        temp_path = local_path + '.temp'

        # Start with an initial chunk (5MB) to analyze video properties
        initial_bytes = 5 * 1024 * 1024  # 5MB

        # Get object with range using get_object instead of download_file
        print("Downloading initial chunk for analysis...")
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=s3_key,
            Range=f'bytes=0-{initial_bytes-1}'
        )

        # Write the initial chunk to temporary file
        with open(temp_path, 'wb') as f:
            f.write(response['Body'].read())

        # Analyze video properties
        clip = VideoFileClip(temp_path)
        bitrate = os.path.getsize(temp_path) / clip.duration  # bytes per second
        clip.close()

        # Calculate required bytes for desired duration
        required_bytes = int(bitrate * duration_seconds)

        # Delete temporary file
        os.remove(temp_path)

        # Download the estimated bytes needed for the duration
        print(f"Downloading first {duration_seconds} seconds of {s3_key}...")
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=s3_key,
            Range=f'bytes=0-{required_bytes-1}'
        )

        # Write the video segment to file
        with open(local_path, 'wb') as f:
            f.write(response['Body'].read())

        # Trim the video to exact duration using moviepy
        if os.path.exists(local_path):
            video = VideoFileClip(local_path)
            video = video.subclip(0, min(duration_seconds, video.duration))
            final_path = local_path.rsplit('.', 1)[0] + '_trimmed.' + local_path.rsplit('.', 1)[1]
            video.write_videofile(final_path)
            video.close()

            # Replace original file with trimmed version
            os.remove(local_path)
            os.rename(final_path, local_path)

            print(f"Successfully downloaded and trimmed video to {local_path}")
            return True

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The video does not exist in the specified S3 path")
        else:
            print(f"An error occurred: {str(e)}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False

def estimate_video_size(local_path):
    """
    Get the duration and size of a local video file
    """
    clip = VideoFileClip(local_path)
    size = os.path.getsize(local_path)
    duration = clip.duration
    clip.close()
    return size, duration

bucket_name = "video-processing-testing"
s3_key = "Experience the Underwater World Through the Eyes of a Free Diver _ Short Film Showcase.mp4"
local_path = "/home/ahmed/workspace/video-streaming/divinig.mp4"

success = download_video_segment_from_s3(bucket_name, s3_key, local_path)
