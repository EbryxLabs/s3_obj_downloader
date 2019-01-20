# Description
	Script to easily download targeted objects from S3 bucket


# Use cases:
- You know the exact time or time range when the CT activity took place but since there are several hundred objects for that duration, you might be looking to download all those objects automatically instead of manually selecting and downloading each object
- Download S3 objects for a limited duration/timerange to investigate/audit CT activity


## **Requirements**:
- Role or User that has read access to S3 objects of relevant bucket
- requirements.txt
- Key/Name of the first_object where you might want to start downloading from. Default is from the first element of the bucket

## **Execution**:
- python3 s3_obj_downloader.py -h