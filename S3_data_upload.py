from Classes.S3_class import S3_Aws_Class

# Initialize the S3_Aws_Class with AWS credentials and region information
initialize = S3_Aws_Class(service_name='',
                          region_name='',
                          aws_access_key_id='',
                          aws_secret_access_key='')

# Using the initialized S3_Aws_Class instance to read and upload data
# from the specified folder path to the given S3 bucket.

initialize.read_upload_s3_from_folder(folder_path="Data/DataSet_final", bucket_name="")



