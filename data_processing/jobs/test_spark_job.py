from pyspark.sql import SparkSession
import boto3
import os
import sys

def list_s3_files(bucket_name, output_file):
    spark = SparkSession.builder.appName("List S3 Files").getOrCreate()

    s3 = boto3.client('s3', 
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' in response:
        files = [content['Key'] for content in response['Contents']]
        if not files:
            files = ["none"]
    else:
        files = ["none"]

    with open('s3_files.txt', 'w') as f:
        for file in files:
            f.write(f"{file}\n")
    
    s3.upload_file('s3_files.txt', bucket_name, output_file)
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit test_spark_s3.py <bucket_name> <output_file>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    output_file = sys.argv[2]
    
    list_s3_files(bucket_name, output_file)