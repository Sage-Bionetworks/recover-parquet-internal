# Parameters for sts_synindex.R process

PARQUET_BUCKET <- 'recover-processed-data'
# S3 bucket containing the post-ETL parquet datasets

AWS_DOWNLOAD_LOCATION <- './temp_aws_parquet/'
# Local location where parquet bucket files are synced to

SYNAPSE_PARENT_ID = 'syn51406699'
# Synapse location where the S3 bucket objects are listed

SYNAPSE_FILEVIEW_ID <- 'syn52047601'
# synID of the file view containing a list of all currently indexed S3 objects from the parquet bucket folder in Synapse
