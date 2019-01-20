import boto3
import pytz
import os
from datetime import datetime
from pprint import pprint
import time
import argparse


# Global Vars
args = ''
#############


def setup_argparse():
    global args
    parser = argparse.ArgumentParser(
        description='''Download S3 objects based on last modified time''',
        epilog="""All's well that ends well.""")
    parser.add_argument('-s', '--start_time', required=True, help='Time in UTC\nExample: 2018-10-31T00:00:00Z')
    parser.add_argument('-e', '--end_time', required=True, help='Time in UTC\nExample: 2018-10-31T00:00:00Z')
    parser.add_argument('-p', '--s3_bucket', required=True, help='my_bucket OR my_bucket')
    parser.add_argument('-f', '--s3_bucket_prefix', default='/', required=False, help='/folder1 OR /folder1/folder2/folder3')
    parser.add_argument('-d', '--downloads_folder', default='downloads-{0}'.format(int(time.time())), required=False, help='Folder where the downloaded objects shall be put into\nExample: /folder1 OR /folder1/folder2/folder3')
    parser.add_argument('-m', '--max_s3_objects_to_list', type=int, default=1000, required=False, help='10 OR 100 OR at max 1000')
    parser.add_argument('-k', '--first_key', default='', required=False, help='Start downloading objects after this object\nExample: folder/myfile.zip')
    parser.add_argument('-t', '--testing', type=int, default=1, required=False, help='1 if under testing so the objects will not be downloaded AND 0 if not testing so the objects will be downloaded')
    parser.add_argument('-l', '--log_file', required=False, default=str(__file__)+'.log', help='/path/to/log/file')

    args = parser.parse_args()

    print('Printing all args...')
    print(args.start_time)
    print(args.end_time)
    print(args.s3_bucket)
    print(args.s3_bucket_prefix)
    print(args.max_s3_objects_to_list)
    print(args.first_key)
    print(args.testing)
    print(args.log_file)
    print('All args printed')

    with open(args.log_file, 'w') as o: pass

    if args.testing == 0:
        if not os.path.exists(args.downloads_folder):
            os.makedirs(args.downloads_folder)

    print('Argparse setup complete')


def log_msg(msg):
    try:
        with open(args.log_file, 'a') as o:
            mStr =  '{0} ---- {1}\n'.format(datetime.now(), msg)
            print(mStr + '\n')
            o.write(mStr)
    except Exception as e:
        print('Unable to write msg to log file due to error "{0}"'.format(e))


def convert_to_s3_time(str_time):
    return pytz.UTC.localize(datetime.strptime(str_time, '%Y-%m-%dT%H:%M:%SZ'))


def if_object_be_downloaded(s3_start_time, s3_end_time, last_modified_time):
    return True if s3_end_time >= last_modified_time and last_modified_time >= s3_start_time else False


def get_file_name(item):
    return item[item.rfind('/')+1:]


def get_objects_with_lmt(client, s3_bucket, s3_bucket_prefix, start_time, end_time, downloads_folder, max_s3_objects_to_list, first_key):
    s3_start_time = ''
    s3_end_time = ''

    if type(start_time) == str:
        s3_start_time = convert_to_s3_time(start_time)
        # log_msg(s3_start_time)

    if type(s3_end_time) == str:
        s3_end_time = convert_to_s3_time(end_time)
        # log_msg(s3_end_time)




    # res = client.list_objects(
    #     Bucket=s3_bucket,
    #     # Delimiter='string',
    #     # EncodingType='url',
    #     # Marker='string',
    #     MaxKeys=2,
    #     Prefix=s3_bucket_prefix
    #     # RequestPayer='requester'
    # )

    # pprint(res)

    res = ''
    size_in_bytes = 0
    StartAfter = None
    while True:
        if 'ContinuationToken' in res:
            res = client.list_objects_v2(
                Bucket=s3_bucket,
                # Delimiter='string',
                # EncodingType='url',
                ContinuationToken=res['ContinuationToken'],
                MaxKeys=max_s3_objects_to_list,
                StartAfter=StartAfter,
                Prefix=s3_bucket_prefix
                # RequestPayer='requester'
            )
        else:
            res = client.list_objects_v2(
                Bucket=s3_bucket,
                # Delimiter='string',
                # EncodingType='url',
                # Marker='string',
                StartAfter=first_key,
                MaxKeys=max_s3_objects_to_list,
                Prefix=s3_bucket_prefix
                # RequestPayer='requester'
            )

        if 'Contents' in res and len(res['Contents']) > 0:
            for item in res['Contents']:
                log_msg('Item: ' + item['Key'])
                file_name = get_file_name(item['Key'])
                log_msg('File name: {0}'.format(file_name))
                log_msg('Last modified time: {0}'.format(item['LastModified']))
                log_msg('Size: {0} MB'.format(item['Size']/1000000))
                download_object = if_object_be_downloaded(s3_start_time, s3_end_time, item['LastModified'])
                log_msg('Object Download: {0}'.format(download_object))

                # if args.testing:
                #     input('Waiting for user input to initiate download...')

                if args.testing == 0 and download_object:
                    with open('{0}/{1}'.format(downloads_folder, file_name), 'wb') as data:
                        client.download_fileobj(s3_bucket, item['Key'], data)
                if download_object:
                    size_in_bytes += item['Size']

            StartAfter = res['Contents'][-1]['Key']

        if 'ContinuationToken' not in res:
            log_msg('No ContinuationToken found')
            break
        if args.testing == 1: break

    log_msg('~ Download size {0} MB'.format(size_in_bytes/1000000))
    log_msg('~ Download size {0} GB'.format(size_in_bytes/1000000000))
    log_msg('~ Download size {0} TB'.format(size_in_bytes/1000000000000))


def main():
    client = boto3.client('s3')
    get_objects_with_lmt(client, args.s3_bucket, args.s3_bucket_prefix, args.start_time, args.end_time, args.downloads_folder, args.max_s3_objects_to_list, args.first_key)


if __name__ == '__main__':
    setup_argparse()
    mTime = [datetime.now(), 0]
    log_msg('Start Time: {0}'.format(mTime[0]))

    main()

    mTime[1] = datetime.now()
    log_msg('End Time: {0}'.format(mTime[1]))
    log_msg('Start Time: {0}'.format(mTime[0]))
    log_msg('End Time: {0}'.format(mTime[1]))
    log_msg('Time Diff: {0}'.format(mTime[1] - mTime[0]))
else:
    main()