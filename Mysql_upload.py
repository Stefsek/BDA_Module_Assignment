from Classes.Mysql_class import Mysql_Class
from Classes.S3_class import S3_Aws_Class

# Initialize the S3_Aws_Class with AWS credentials and region information.
# This instance will be used to interact with AWS S3 services.
s3_init = S3_Aws_Class(service_name='',
                       region_name='',
                       aws_access_key_id='',
                       aws_secret_access_key='')

# Initialize the Mysql_Class with MySQL credentials.
# This instance will be used for database operations in MySQL.
mysql_init = Mysql_Class(user='',
                         host='',
                         password='')

# Retrieve data from the specified S3 bucket and path.
# This function will return a list of dataframes and their corresponding names.
dataframes_list, dataframes_names = s3_init.get_data_from_s3(files_path="",
                                                             bucket_name='')

# Upload the retrieved dataframes to the specified MySQL database.
# Each dataframe in the list will be uploaded with its corresponding name.
mysql_init.Upload_tables(db_name='',
                         dfs_list=dataframes_list,
                         dfs_names=dataframes_names)