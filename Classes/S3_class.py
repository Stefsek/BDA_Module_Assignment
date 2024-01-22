import os
import pandas as pd
from io import BytesIO
import boto3
from tqdm import tqdm
import s3fs

class S3_Aws_Class:
    """
    A class to handle AWS S3 operations including uploading dataframes and reading data from S3.

    Attributes:
        service_name (str): The name of the AWS service (e.g., 's3').
        region_name (str): The AWS region name.
        aws_access_key_id (str): AWS access key ID.
        aws_secret_access_key (str): AWS secret access key.
    """

    def __init__(self, service_name, region_name, aws_access_key_id, aws_secret_access_key):
        """
        Initialize the S3_Aws_Class instance with AWS credentials.

        Args:
            service_name (str): The name of the AWS service (e.g., 's3').
            region_name (str): The AWS region name.
            aws_access_key_id (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
        """
        self.service_name = service_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # Initialize boto3 client and resource for AWS S3
        self.s3_client = boto3.client(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        self.s3_resource = boto3.resource(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

    def upload_dataframe_to_s3_parquet(self, df, bucket_name, object_name):
        """
        Uploads a DataFrame to AWS S3 in Parquet format.

        Args:
            df (pandas.DataFrame): The DataFrame to upload.
            bucket_name (str): The name of the S3 bucket.
            object_name (str): The S3 object name for the uploaded file.

        Returns:
            int: 1 if upload is successful, 0 otherwise.
        """
        # Save DataFrame to a Bytes buffer in Parquet format
        buffer = BytesIO()
        df.to_parquet(buffer)

        # Upload the buffer content to S3
        response = self.s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket_name, Key=object_name)

        # Check response status
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return 1
        else:
            return 0

    def read_upload_s3_from_folder(self, folder_path, bucket_name):
        """
        Reads CSV files from a folder and uploads them as Parquet files to AWS S3.

        Args:
            folder_path (str): Path to the folder containing CSV files.
            bucket_name (str): The name of the S3 bucket to upload to.
        """
        # List all CSV files in the folder
        files_in_folder = os.listdir(folder_path)
        csv_files = [file for file in files_in_folder if "csv" in file]

        # Prepare lists to hold DataFrames and their names
        dfs_list = []
        dfs_names = []

        # Read each CSV file into a DataFrame
        for csv_file in csv_files:
            file_name = csv_file.split(".")[0]
            df_name = f"{file_name}_df"
            globals()[df_name] = pd.read_csv(f"{folder_path}/{csv_file}", encoding='ISO-8859-1')
            dfs_list.append(globals()[df_name])
            dfs_names.append(file_name)

        # Upload each DataFrame to S3 as Parquet
        for i in tqdm(range(len(dfs_list)), desc='Uploading files to S3'):
            filename = dfs_names[i]
            status = self.upload_dataframe_to_s3_parquet(dfs_list[i], bucket_name, f"files/{filename}.parquet")
            if status == 1:
                print(f"{filename} uploaded successfully to {bucket_name}")
            else:
                print("Error")

    def get_data_from_s3(self, files_path, bucket_name):
        """
        Retrieves data from AWS S3 and loads it into Pandas DataFrames.

        Args:
            files_path (str): The path within the S3 bucket to retrieve files from.
            bucket_name (str): The name of the S3 bucket.

        Returns:
            tuple: A tuple containing two lists, one of DataFrames and another of their names.
        """
        # Initialize S3 file system
        fs = s3fs.S3FileSystem(anon=False, key=self.aws_access_key_id, secret=self.aws_secret_access_key)
        bucket = self.s3_resource.Bucket(bucket_name)

        # Prepare lists to hold DataFrames and their names
        dfs_list = []
        dfs_names = []

        # Get the list of Parquet objects in the specified path
        objects = [obj for obj in bucket.objects.filter(Prefix=files_path) if obj.key.endswith('.parquet')]

        # Download and read each Parquet file into a DataFrame
        for obj in tqdm(objects, desc=f"Downloading files from {bucket_name}/{files_path}"):
            s3_path = f"s3://{bucket_name}/{obj.key}"
            file_name = obj.key.split('/')[-1].split('.')[0]

            df = pd.read_parquet(s3_path, engine='pyarrow', filesystem=fs)
            dfs_list.append(df)
            dfs_names.append(file_name)

        print("All files downloaded")
        return dfs_list, dfs_names