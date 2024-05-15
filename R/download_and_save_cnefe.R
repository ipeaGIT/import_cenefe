## debug
# link <- tar_read(get_files_links)[4]
# lapply(X= tar_read(get_files_links) , FUN = download_and_save_cnefe)

#> erro no link 2022 para  TO  !!!!!!

download_and_save_cnefe <- function(link){


  # detect year
  year <- detect_year_from_string(link)

  # detecta uf
  uf <- sub(".*_([A-Z]{2})\\.zip", "\\1", link)
  message(uf)

  message(paste0("\nProcessing UF: ", uf, '\n'))

  # create dir if it has not been created already
  dest_dir <- paste0('./data_raw/',year,'/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }

  # Download zipped files
  dest_dir_uf <- paste0(dest_dir, uf)
  dir.create(path = dest_dir_uf, recursive = TRUE, showWarnings = FALSE)

  tempf <- paste0(dest_dir_uf, '/', basename(link))
  httr::GET(url = link,
            httr::progress(),
            httr::write_disk(tempf, overwrite = T),
            config = httr::config(ssl_verifypeer = FALSE)
  )

  ## Unzip original data
  temp_dir <- tempdir()
  unzip(zipfile = tempf, exdir = temp_dir, overwrite = TRUE)

  files <- list.files(temp_dir, pattern = '*.csv', full.names = T)
  files <- files[grepl(substr(basename(link),1,2),files)]

  # Read file
  temp_dt <- data.table::fread(files)
  data.table::setDT(temp_dt)

  if (year == 2022){
  names(temp_dt) <- c('code_state','code_muni', 'code_especie','lat', 'lon','nv_geo_coord')
  cols <- c('code_state','code_muni', 'code_especie','nv_geo_coord', 'lat', 'lon')
  temp_dt <- temp_dt[, ..cols]
  }

  # save in .csv
  # create dir if it has not been created already
  dest_dir <- paste0('./data/', year, '/csv/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  fwrite(temp_dt, file = paste0(dest_dir,gsub(".zip",".csv",basename(link))), row.names = F)

  # save in .parquet
  # create dir if it has not been created already
  dest_dir <- paste0('./data/', year, '/parquet/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  arrow::write_parquet(temp_dt, paste0(dest_dir,gsub(".zip",".parquet",basename(link))))

  }


