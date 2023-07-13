##########
## AWS CLI helper functions
## Uses synapse STS token to access AWS token
##########

####
# Sync between two buckets - source and destination
####
s3SyncBuckets <- function(source_bucket, destination_bucket, aws_profile='service-catalog'){
  command_in <- glue::glue('aws --profile {aws_profile} s3 sync {source_bucket} {destination_bucket}')
  print(paste0('RUNNING: ',command_in))
  system(command_in)
}

####
# Sync to local location from a source bucket
####
s3SyncToLocal <- function(source_bucket, local_destination = './temp_aws', aws_profile='service-catalog'){
  command_in <- glue::glue('aws --profile {aws_profile} s3 sync {source_bucket} {local_destination}')
  if(aws_profile == 'env-var'){
    command_in <- glue::glue('aws s3 sync {source_bucket} {local_destination}')
  }
  
  print(paste0('RUNNING: ',command_in))
  system(command_in)
}

####
# Sync from local location to a destination bucket
####
s3SyncFromLocal <- function(local_source = './temp_aws', destination_bucket, aws_profile='service-catalog'){
  command_in <- glue::glue('aws --profile {aws_profile} s3 sync {local_source} {destination_bucket}')
  if(aws_profile == 'env-var'){
    command_in <- glue::glue('aws s3 sync {local_source} {destination_bucket} --acl bucket-owner-full-control')
  } # need to add acl permisisons as bucket-owner-full-control to upload/put objects in the bucket
  
  print(paste0('RUNNING: ',command_in))
  system(command_in)
}

####
# Get a list of all files in a bucket, Stores to a txt file in the local working directory
####
s3lsBucketObjects <- function(source_bucket, output_file='s3files.txt', aws_profile='service-catalog'){
  command_in <- glue::glue('aws --profile {aws_profile} s3 ls {source_bucket} --recursive --output text > {output_file}')
  if(aws_profile == 'env-var'){
    command_in <- glue::glue('aws s3 ls {source_bucket} --recursive --output text > {output_file}')
  }

  print(paste0('RUNNING: ',command_in))
  system(command_in)
}
