token <- synapser::synGetStsStorageToken(
  entity = 'syn52224315',
  permission = "read_only",
  output_format = "json")

s3 <- arrow::S3FileSystem$create(
  access_key = token$accessKeyId,
  secret_key = token$secretAccessKey,
  session_token = token$sessionToken,
  region="us-east-1")

base_s3_uri <- paste0(token$bucket, "/", token$baseKey)

parquet_datasets <- s3$GetFileInfo(
  arrow::FileSelector$create(paste0(base_s3_uri), recursive=T))


e <- function() {
  i <- 0
  for (dataset in parquet_datasets) {
    if (grepl('.*dataset.*/.*', dataset$path, perl = T, ignore.case = T)) {
      i <- i+1
      print(paste0(i, ': ', dataset$path))
    }
  }
}
try(e())

metadata <- arrow::open_dataset(
  s3$path(paste0(base_s3_uri, "dataset_enrolledparticipants/")))

metadata_df <- dplyr::collect(metadata)
