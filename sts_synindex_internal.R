library(synapser)
library(arrow)
library(dplyr)
library(synapserutils)
library(rjson)

synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))
source('~/recover-parquet-internal/sts_params_internal.R')

#### Get STS token for bucket in order to sync to local dir ####

# Get STS credentials
token <- synapser::synGetStsStorageToken(
  entity = PARQUET_FOLDER,
  permission = "read_only",
  output_format = "json")

if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
  base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
} else {
  base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
}

# configure the environment with AWS token
Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
           'AWS_SESSION_TOKEN'=token$sessionToken)

#### Sync bucket to local dir####
unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)

#### Index S3 Objects in Synapse ####
existing_dirs <- synGetChildren(PARQUET_FOLDER) %>% as.list()

if(length(existing_dirs)>0) {
  for (i in seq_along(existing_dirs)) {
    synDelete(existing_dirs[[i]]$id)
  }
}

# Generate manifest of existing files
SYNAPSE_AUTH_TOKEN <- Sys.getenv('SYNAPSE_AUTH_TOKEN')
manifest_cmd <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {SYNAPSE_PARENT_ID} --manifest ./current_manifest.tsv {AWS_PARQUET_DOWNLOAD_LOCATION}')
system(manifest_cmd)

## Get a list of all files to upload and their synapse locations(parentId) 
STR_LEN_AWS_PARQUET_DOWNLOAD_LOCATION <- stringr::str_length(AWS_PARQUET_DOWNLOAD_LOCATION)

## All files present locally from manifest
synapse_manifest <- read.csv('./current_manifest.tsv', sep = '\t', stringsAsFactors = F) %>% 
  dplyr::filter(path != paste0(AWS_PARQUET_DOWNLOAD_LOCATION,'/owner.txt')) %>%  # need not create a dataFileHandleId for owner.txt
  dplyr::rowwise() %>% 
  dplyr::mutate(file_key = stringr::str_sub(string = path, start = STR_LEN_AWS_PARQUET_DOWNLOAD_LOCATION+2)) %>% # location of file from home folder of S3 bucket
  dplyr::mutate(s3_file_key = paste0('main/parquet/', file_key)) %>% # the namespace for files in the S3 bucket is S3::bucket/main/
  dplyr::mutate(md5_hash = as.character(tools::md5sum(path))) %>% 
  dplyr::ungroup()

## All currently indexed files in Synapse
synapse_fileview <- synapser::synTableQuery(paste0('SELECT * FROM ', SYNAPSE_FILEVIEW_ID))$filepath %>% read.csv()

## find those files that are not in the fileview - files that need to be indexed
if (nrow(synapse_fileview)>0) {
  synapse_manifest_to_upload <- 
    synapse_manifest %>% 
    dplyr::anti_join(
      synapse_fileview %>% 
        dplyr::select(parent = parentId,
                      s3_file_key = dataFileKey,
                      md5_hash = dataFileMD5Hex))
} else {
  synapse_manifest_to_upload <- synapse_manifest
}

## Index in Synapse
## For each file index it in Synapse given a parent synapse folder
if(nrow(synapse_manifest_to_upload) > 0){ # there are some files to upload
  for(file_number in seq(nrow(synapse_manifest_to_upload))){
    
    # file and related synapse parent id 
    file_= synapse_manifest_to_upload$path[file_number]
    parent_id = synapse_manifest_to_upload$parent[file_number]
    s3_file_key = synapse_manifest_to_upload$s3_file_key[file_number]
    # this would be the location of the file in the S3 bucket, in the local it is at {AWS_PARQUET_DOWNLOAD_LOCATION}/
    
    absolute_file_path <- tools::file_path_as_absolute(file_) # local absolute path
    
    temp_syn_obj <- synapser::synCreateExternalS3FileHandle(
      bucket_name = PARQUET_BUCKET,
      s3_file_key = s3_file_key, #
      file_path = absolute_file_path,
      parent = parent_id
    )
    
    # synapse does not accept ':' (colon) in filenames, so replacing it with '_colon_'
    new_fileName <- stringr::str_replace_all(temp_syn_obj$fileName, ':', '_colon_')
    
    f <- File(dataFileHandleId=temp_syn_obj$id,
              parentId=parent_id,
              name = new_fileName) ## set the new file name
    
    f <- synStore(f)
    
  }
}
