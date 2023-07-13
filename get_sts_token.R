library('aws.s3')
library('synapser')
synapser::synLogin(authToken=Sys.getenv("SYNAPSE_AUTH_TOKEN"))

folder_and_storage_location <- synapser::synCreateS3StorageLocation(parent='syn51849609',
                                                                    folder_name='ce-input-data-dev',
                                                                    folder=NULL,
                                                                    bucket_name='recover-dev-input-data',
                                                                    base_key='main',
                                                                    sts_enabled=TRUE)

folder <- folder_and_storage_location[[1]]
storage_location <- folder_and_storage_location[[2]]

sts_token <- synapser::synGetStsStorageToken(entity = folder$properties$id, # MHP Input Data folder
                                             permission = 'read_only',
                                             output_format = 'json')

# configure the environment with AWS token
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)

# credentials <- paste0(
#   "\n[profile sts-token]\n",
#   "aws_access_key_id=", sts_token$accessKeyId, "\n",
#   "aws_secret_access_key=", sts_token$secretAccessKey, "\n",
#   "aws_session_token=", sts_token$sessionToken, "\n")
# 
# aws_config_file <- file.path(path.expand('~'), '.aws/config')
# 
# cat(credentials, file = aws_config_file, append = TRUE, sep = '\n')

## list objects in bucket/ list buckets
bucket_list <- aws.s3::get_bucket_df('recover-dev-input-data', prefix = 'main')
