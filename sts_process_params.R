# Parameters for sts_synindex.R process

# S3 bucket containing the post-ETL parquet datasets
PARQUET_BUCKET <- 'recover-processed-data'

PARQUET_BUCKET_BASE_KEY <- 'main/parquet/'

PARQUET_FOLDER <- 'syn51406699'

AWS_PARQUET_DOWNLOAD_LOCATION <- './temp_aws_parquet'

# Local location where parquet bucket files are synced to
AWS_DOWNLOAD_LOCATION <- './temp_aws_parquet/'

PARQUET_FILTERED_LOCATION <- './parquet_filtered'

datasets_to_filter <- c("./temp_aws_parquet/dataset_enrolledparticipants", 
                        "./temp_aws_parquet/dataset_enrolledparticipants_customfields_symptoms", 
                        "./temp_aws_parquet/dataset_enrolledparticipants_customfields_treatments", 
                        "./temp_aws_parquet/dataset_healthkitv2heartbeat", 
                        "./temp_aws_parquet/dataset_healthkitv2samples", 
                        "./temp_aws_parquet/dataset_healthkitv2workouts", 
                        "./temp_aws_parquet/dataset_symptomlog")

cols_to_drop <- list(c("EmailAddress", "DateOfBirth", "CustomFields_DeviceOrderInfo", "FirstName", "LastName", "PostalCode", "MiddleName"),
                     c("name"),
                     c("name"),
                     c("Source_Name"),
                     c("Source_Name", "Device_Name"),
                     c("Source_Name", "Metadata_HKWorkoutBrandName", "Metadata_Coach", "Metadata_trackerMetadata", "Metadata_SWMetadataKeyCustomWorkoutTitle", "Metadata_location"),
                     c("Value_notes", "Properties"))

PARQUET_FINAL_LOCATION <- './parquet_filtered_final'

# Synapse location where the S3 bucket objects are listed
SYNAPSE_PARENT_ID <- 'syn51406699'

# synID of the file view containing a list of all currently indexed S3 objects from the parquet bucket folder in Synapse
SYNAPSE_FILEVIEW_ID <- 'syn52133651'
