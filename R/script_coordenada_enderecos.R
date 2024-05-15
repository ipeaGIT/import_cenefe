
####### Load Support packages
library(rvest)
library(future)
library(furrr)


######
ftp     <- "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Coordenadas_enderecos/UF/"
links <-  RCurl::getURL(ftp) %>% read_html() %>% html_nodes(xpath = '//td/a[@href]') %>% html_attr('href')
links <- links[-1]
links <- paste0(ftp,links)

###### download raw data --------------------------------

download_adress <- function(links){

  uf <- sub(".*_([A-Z]{2})\\.zip", "\\1", links)

  message(paste0("\nDownloading UF: ", uf, '\n'))

  # create dir if it has not been created already
  dest_dir <- paste0('./coordenadas_enderecos/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }

  # Download zipped files
  temp_dir <- tempdir()
  temp_dir <- paste0(temp_dir, '/coordenadas_enderecos/', uf)
  dir.create(path = temp_dir, recursive = TRUE, showWarnings = FALSE)

  tempf <- paste0(temp_dir, '/', basename(links))
  httr::GET(url = links,
            httr::progress(),
            httr::write_disk(tempf, overwrite = T),
            config = httr::config(ssl_verifypeer = FALSE)
  )

  ## Unzip original data
  unzip(zipfile = tempf, exdir = temp_dir, overwrite = TRUE)

  files <- list.files(temp_dir, pattern = '*.csv', full.names = T)
  files <- files[grepl(substr(basename(links),1,2),files)]

  # Read file
  temp_dt <- data.table::fread(files)
  data.table::setDT(temp_dt)
  names(temp_dt) <- c('code_state','code_muni', 'code_especie','lat', 'lon','nv_geo_coord')
  cols <- c('code_state','code_muni', 'code_especie','nv_geo_coord', 'lat', 'lon')
  temp_dt <- temp_dt[, ..cols]


  # save in .csv
  # create dir if it has not been created already
  dest_dir <- paste0('./coordenadas_enderecos/csv/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  write.csv2(temp_dt, file = paste0(dest_dir,gsub(".zip",".csv",basename(links))), row.names = F)

  # save in .parquet
  # create dir if it has not been created already
  dest_dir <- paste0('./coordenadas_enderecos/parquet/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  arrow::write_parquet(temp_dt, paste0(dest_dir,gsub(".zip",".parquet",basename(links))))

  # save in .rds
  # create dir if it has not been created already
  dest_dir <- paste0('./coordenadas_enderecos/rds/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  saveRDS(temp_dt, file = paste0(dest_dir,gsub(".zip",".rds",basename(links))), compress = TRUE)

}

# apply function in parallel
future::plan(multisession)
future_map(.x = links,.f=download_adress)

rm(list= ls())
gc(reset = T)
