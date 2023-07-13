##############
# Sync the input and pre-ETL buckets .This is to make a copy
# of the data provided to us by Care Evolution(CE) in the input bucket.
##############
## Required functions and parameters
source('~/recover-s3-synindex/awscli_utils.R')
source('~/recover-s3-synindex/params.R')
library('synapser')

#############
# Sync S3 ingress bucket to Local EC2 (First Sync )
# using prod-creds (with 'read' only permissions) for accessing the ingress bucket 
#############
# aws profile used is command line or programmatic access creds from jumpcloud under
# org-sagebase-identitycentral > S3ExternalCollab
# these creds are stored in config file under ~/.aws/config 
s3SyncToLocal(source_bucket = paste0('s3://', INGRESS_BUCKET,'/'),
              local_destination = AWS_DOWNLOAD_LOCATION,
              aws_profile = 's3-external-collab') 

#############
# Rename folders with '\' to have '_' for eg., 'adults\v1' to 'adults_v1'
#############
# We will work with the locally replicated structure 
# all folders inside the AWS_DOWNLOAD_LOCATION, i.e all folders at AWS_DOWNLOAD_LOCATION/
localDirs <- list.dirs(path = AWS_DOWNLOAD_LOCATION, full.names = FALSE, recursive = FALSE) 

# The folders are named as 'adults\v1', 'pregnant\v1' and 'pediatric\v1'. We want to remove the
# '\' and replace it with '_'
for(dir_ in localDirs){
  dir_newName <- stringr::str_replace_all(dir_, "\\\\",'_')
  # rename each folder
  file.rename(paste0(AWS_DOWNLOAD_LOCATION,"/",dir_), paste0(AWS_DOWNLOAD_LOCATION,"/",dir_newName))
}

# Get newly renamed folders
localDirs <- list.dirs(path = AWS_DOWNLOAD_LOCATION, full.names = FALSE, recursive = FALSE) 


#############
# Sync Local EC2 (from ingress bucket) to pre_etl bucket (Second Sync )
#############
synapser::synLogin(daemon_acc, daemon_acc_password) # login into Synapse
# synapser::synLogin()

sts_token <- synapser::synGetStsStorageToken(entity = 'syn51714264', # sts enabled destination folder
                                             permission = 'read_write',  
                                             output_format = 'json')

# configure the environment with AWS token (this is the aws_profile named 'env-var')
Sys.setenv('AWS_ACCESS_KEY_ID'=sts_token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=sts_token$secretAccessKey,
           'AWS_SESSION_TOKEN'=sts_token$sessionToken)


s3SyncFromLocal(local_source = AWS_DOWNLOAD_LOCATION,
                destination_bucket = paste0('s3://', PRE_ETL_BUCKET,'/main'),
                aws_profile = 'env-var')

## check if we have download access to PRE_ETL_BUCKET
# s3SyncToLocal(source_bucket = paste0('s3://', PRE_ETL_BUCKET,'/main'),aws_profile = 'env-var',local_destination = './temp_input_data1')

## list objects from PRE_ETL_BUCKET
# s3lsBucketObjects(source_bucket =  paste0('s3://', PRE_ETL_BUCKET,'/main'),
#                   aws_profile = 'env-var')

#############
# Get bucket params and file list
#############
## The folders we want to show in synapse
## This would depend on the how the data is organized in the main S3 INGRESS bucket
FOLDERS_TO_SYNC_SYNAPSE <- c('adults_v1',
                             'pregnant_v1',
                             'pediatric_v1') 

# When we create dataFileHandleId in synapse it will create some folders in the source S3 bucket
# we don't need those
## From the local AWS location remove all folders that are not the ones selected above
dirs_to_delete <- dplyr::setdiff(localDirs, c(FOLDERS_TO_SYNC_SYNAPSE, "")) # "" is the local directory, a result of list.dirs(.. full.names=FALSE)

## delete the unwanted directories before creating a upload manifest
for(dir_ in dirs_to_delete){
  unlink(paste0(AWS_DOWNLOAD_LOCATION,"/",dir_), recursive = TRUE)
}
