library(magrittr)

# Functions ---------------------------------------------------------------

#' Replace equal sign with underscore
#'
#' This function renames a directory path by replacing equal signs with underscores.
#' If a replacement is performed, it logs the change.
#'
#' @param directory_path The path of the directory to rename.
#'
#' @examples
#' replace_equal_with_underscore("path_with=equals")
#' 
replace_equal_with_underscore <- function(directory_path) {
  new_directory_path <- gsub("=", "_", directory_path)
  if (directory_path != new_directory_path) {
    file.rename(directory_path, new_directory_path)
    return(cat("Renamed:", directory_path, "to", new_directory_path, "\n"))
  }
}


# Setup -------------------------------------------------------------------

synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))
source('~/recover-parquet-internal/sts_params_internal.R')


# Get STS credentials for processed data bucket ---------------------------

token <- synapser::synGetStsStorageToken(
  entity = PARQUET_FOLDER,
  permission = "read_only",
  output_format = "json")

if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
  base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
} else {
  base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
}


# Configure the environment with AWS token --------------------------------

Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
           'AWS_SESSION_TOKEN'=token$sessionToken)


# Sync bucket to local dir ------------------------------------------------

unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)


# Modify cohort identifier in dir name ------------------------------------

junk <- invisible(lapply(list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION), replace_equal_with_underscore))


# Generate manifest of existing files -------------------------------------

SYNAPSE_AUTH_TOKEN <- Sys.getenv('SYNAPSE_AUTH_TOKEN')
manifest_cmd <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {SYNAPSE_PARENT_ID} --manifest ./current_manifest.tsv {AWS_PARQUET_DOWNLOAD_LOCATION}')
system(manifest_cmd)


# Index files in Synapse --------------------------------------------------

# Get a list of all files to upload and their synapse locations (parentId)
STR_LEN_AWS_PARQUET_DOWNLOAD_LOCATION <- stringr::str_length(AWS_PARQUET_DOWNLOAD_LOCATION)

# List all local files present (from manifest)
synapse_manifest <- 
  read.csv('./current_manifest.tsv', sep = '\t', stringsAsFactors = F) %>%
  dplyr::filter(!grepl('owner.txt', path)) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(file_key = stringr::str_sub(string = path, start = STR_LEN_AWS_PARQUET_DOWNLOAD_LOCATION+2)) %>%
  dplyr::mutate(s3_file_key = paste0(PARQUET_BUCKET_BASE_KEY, file_key)) %>%
  dplyr::mutate(md5_hash = as.character(tools::md5sum(path))) %>%
  dplyr::ungroup() %>% 
  dplyr::mutate(file_key = gsub("cohort_", "cohort=", file_key),
                s3_file_key = gsub("cohort_", "cohort=", s3_file_key))

# List all files currently indexed in Synapse
synapse_fileview <- 
  synapser::synTableQuery(paste0('SELECT * FROM ', SYNAPSE_FILEVIEW_ID))$filepath %>% 
  read.csv()
synapse_fileview <- 
  synapser::synTableQuery(paste0('SELECT * FROM ', SYNAPSE_FILEVIEW_ID))$filepath %>% 
  read.csv()

# Find the files in the manifest that are not yet indexed in Synapse
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

# Get script details for SynStore() provenance
latest_commit <- gh::gh("/repos/:owner/:repo/commits/main", owner = "Sage-Bionetworks", repo = "recover-parquet-internal")
latest_commit_tree_url <- latest_commit$html_url %>% stringr::str_replace("commit", "tree")

# Index each file in Synapse
if(nrow(synapse_manifest_to_upload) > 0){
  for(file_number in seq_len(nrow(synapse_manifest_to_upload))){
    tmp <- 
      synapse_manifest_to_upload[file_number, c("path", "parent", "s3_file_key")]
    
    absolute_file_path <- 
      tools::file_path_as_absolute(tmp$path)
    
    temp_syn_obj <- 
      synapser::synCreateExternalS3FileHandle(
        bucket_name = PARQUET_BUCKET,
        s3_file_key = tmp$s3_file_key,
        file_path = absolute_file_path,
        parent = tmp$parent)
    
    new_fileName <- stringr::str_replace_all(temp_syn_obj$fileName, ':', '_colon_')
    
    f <- 
      synapser::File(dataFileHandleId = temp_syn_obj$id,
                     parentId = tmp$parent,
                     name = new_fileName)
    
    f <- 
      synapser::synStore(f, 
                         activityName = "Indexing", 
                         activityDescription = "Indexing internal parquet datasets",
                         used = PARQUET_FOLDER, 
                         executed = latest_commit_tree_url)
  }
}
