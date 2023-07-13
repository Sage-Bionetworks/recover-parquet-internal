################################
## Required Parameters
################################

INGRESS_BUCKET = 'recover-ingestion'
# S3 bucket whose objects are being copied

PRE_ETL_BUCKET = 'recover-input-data'
# S3 bucket to where objects are being copied into

AWS_DOWNLOAD_LOCATION = './temp_aws/main/'
# Local location where Ingress bucket files are synced to

# FILE_LIST_OUTPUT = 's3files.txt' 
# # file name of the file where the aws ls output is saved

SYNAPSE_PARENT_ID = 'syn51714264'
# Synapse location where the S3 bucket objects are listed

SYNAPSE_FILEVIEW_ID = 'syn51406705'
# Synapse ID of the file view containing list of all currently indexed S3 objects in Synapse

PARQUET_BUCKET <- 'recover-processed-data'
# S3 bucket containing the post-ETL parquet datasets

PARQUET_FILEVIEW_ID <- 'syn52047601'
# synID of the file view containing a list of all currently indexed S3 objects from the parquet bucket folder in Synapse

AWS_PARQUET_DOWNLOAD_LOCATION <- './temp_aws_parquet'
# Local location where parquet bucket files are synced to
