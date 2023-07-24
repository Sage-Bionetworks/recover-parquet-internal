# Parameters for sts_synindex.R process

# S3 bucket containing the post-ETL parquet datasets
PARQUET_BUCKET <- 'recover-processed-data'

PARQUET_BUCKET_BASE_KEY <- 'main/parquet/'

PARQUET_FOLDER <- 'syn51406699'

# Local location where parquet bucket files are synced to
AWS_PARQUET_DOWNLOAD_LOCATION <- './temp_aws_parquet'

# Synapse location where the S3 bucket objects are listed
SYNAPSE_PARENT_ID <- 'syn51406699'

# synID of the file view containing a list of all currently indexed S3 objects from the parquet bucket folder in Synapse
SYNAPSE_FILEVIEW_ID <- 'syn52133651'
