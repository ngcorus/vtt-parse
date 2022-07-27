import time
import pandas as pd
import boto3
import csv
import botocore
import webvtt
import configparser
import os

config = configparser.ConfigParser()
config.read(r'../config/config.ini')

# AWS Credentials
aws_access_key_id = config.get('aws', 'aws_access_key_id')
aws_secret_access_key = config.get('aws', 'aws_secret_access_key')
aws_session_token = config.get('aws', 'aws_session_token')
region_name = config.get('aws', 'region_name')

# S3 Bucket

bucket = "aiptaxonomy"
station_ = 'global'
prefix = f"final-rekognition/{station_}/"

# Connect to AWS s3 resource
s3 = boto3.client( 's3',
                    region_name = region_name,
                    aws_access_key_id = aws_access_key_id,
                    aws_secret_access_key = aws_secret_access_key,
                    aws_session_token = aws_session_token
)

# Grab guids from a file and append to tracker file
def setTracker(station_):
    print('getting guids...')
    trackerfile = '../data/tracker.csv'
    if os.path.exists(trackerfile):
        os.remove(trackerfile)
    get_station = pd.read_csv(f'../parsed-vtt/{station_}.csv')
    group_by_guid = get_station.groupby(['guid'], sort=False)
    for id, allcolumns in group_by_guid:
        with open(trackerfile, 'a') as f:
            f.write(f'{id}\n')

# Create list of tokens against which the current file is checked to identify if it has already been processed
def tokens():
    setTracker(station_)
    tracker = pd.read_csv('../data/tracker.csv', header=None)
    group_by_guid = tracker.groupby([0], sort=False)
    tokens = []
    for guid, allcolumns in group_by_guid:
        tokens.append(guid)
    return tokens

# Export to csv file
def savetoCSV(vttdata, guid, station_name):
    fields = ["guid", "start", "end", "text"]
    for i in range(len(vttdata)):
        write_csv_to = f'../parsed-vtt/{station_name}.csv'
        if not os.path.isfile(write_csv_to):
            with open(write_csv_to, 'w', encoding="utf-8", newline='') as csvfile:
                # creating a csv dict writer object
                writer = csv.DictWriter(csvfile, fieldnames=fields)
                writer.writeheader()
                writer.writerow(vttdata[i])
        with open(write_csv_to, 'a', encoding="utf-8", newline='') as csvfile:
            # creating a csv dict writer object
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writerow(vttdata[i])
    return

# Parse vtt file
vttdata = []
def parseVTT(file, guid):
    try:
        for caption in webvtt.read(file):
            vtt = {"guid" : guid, "start" : caption.start, "end" : caption.end, "text" : caption.text}
            vttdata.append(vtt)
    except Exception:
        return


# get token list - run this function if the ingest was previously interrupted
get_tokens = tokens()

paginator = s3.get_paginator('list_objects')
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for i in range(len(page['Contents'])):
        filename = page['Contents'][i]['Key']
        if filename.endswith('_WebVTT.vtt'):
            filename_split = filename.split('/')
            guid = filename_split[3]
            station_name = filename_split[1]
            if guid not in get_tokens:
                print(f"{filename} ...")
                try:
                    s3.download_file(bucket, filename, f'../data/{station_name}.vtt')
                    parseVTT(f'../data/{station_name}.vtt', guid)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
    savetoCSV(vttdata, guid, station_name)