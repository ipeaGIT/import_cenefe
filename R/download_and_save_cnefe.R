## debug
# link <- tar_read(get_files_links)[4]
# lapply(X= tar_read(get_files_links) , FUN = download_and_save_cnefe)

#> erro no link 2022 para  TO  !!!!!!

download_and_save_cnefe <- function(link){

  if (grepl("Arquivos_CNEFE",link)){
    file <- "arquivos"
  } else {
    file <- "coordenadas"
  }


  # detect year
  year <- detect_year_from_string(link)

  # detecta uf
  uf <- sub(".*_([A-Z]{2})\\.zip$|.*/([^/]+)/[^/]+\\.zip$", "\\1\\2", link)
  #message(uf)

  message(paste0("\nProcessing UF: ", uf," (",file,")", '\n'))

  # create dir if it has not been created already
  dest_dir <- paste0('./data_raw/',year,"/",file,'/')
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





  if (year == 2010) {

    library(readr)
    files <- list.files(temp_dir, pattern = '*.txt', full.names = T)
    files <- files[grepl(substr(basename(link),1,2),files)]

    # Definindo as posições iniciais e finais com base na imagem fornecida
    positions_start <- c(
      1, 3, 8, 10, 12, 16, 17, 37, 67, 127, 135, 142, 162, 172, 192,
      202, 222, 232, 252, 262, 282, 292, 312, 322, 337, 352, 412,
      472, 474, 514, 515, 545, 548, 551
    )

    positions_end <- c(
      2, 7, 9, 11, 15, 16, 36, 66, 126, 134, 141, 161, 171, 191, 201,
      221, 231, 251, 261, 281, 291, 311, 321, 336, 351, 411, 471,
      473, 513, 514, 544, 547, 550, 558
    )

    # Nomeando as colunas
    column_names <- c(
      'code_state', 'code_muni', 'code_district', 'code_subdistrict',
      'code_sector', 'situacao_setor', 'nom_tipo_seglogr', 'nom_titulo_seglogr',
      'Nome_logradouro', 'num_andress', 'Modificador_numero',
      'nom_comp_elem1','val_comp_elem1','nom_comp_elem2','val_comp_elem2',
      'nom_comp_elem3','val_comp_elem3','nom_comp_elem4','val_comp_elem4',
      'nom_comp_elem5','val_comp_elem5','nom_comp_elem6','val_comp_elem6',
      'lat', 'lon', 'Localidade', 'Nulo', 'Especie_endereco',
      'identificacao_estabelecimento', 'indicador_endereco',
      'identificacao_domicilio_coletivo', 'num_quadra', 'num_face', 'cep'
    )

    # Criando o objeto pos com readr::fwf_positions
    pos <- readr::fwf_positions(
      start = positions_start,
      end = positions_end,
      col_names = column_names
    )

    # Lendo o arquivo com read_fwf do pacote readr e especificando a codificação
    temp_dt <- readr::read_fwf(files, col_positions = pos, locale = readr::locale(encoding = "ISO-8859-1"))


  }

  if (year == 2022) {

    files <- list.files(temp_dir, pattern = '*.csv', full.names = T)
    files <- files[grepl(substr(basename(link),1,2),files)]

    if (grepl("Arquivos_CNEFE",link)) {
      files <- files[grepl(uf,files)]

      # Read file
      temp_dt <- data.table::fread(files)

      names(temp_dt) <- c('code_address','code_state','code_muni','code_district','code_sub_district',
                          'code_sector','num_quadra','num_face','cep','desc_localidade','nom_tipo_seglogr',
                          'nom_titulo_seglogr','nom_seglogr','num_andress','dsc_modificador',
                          'nom_comp_elem1','val_comp_elem1','nom_comp_elem2','val_comp_elem2',
                          'nom_comp_elem3','val_comp_elem3','nom_comp_elem4','val_comp_elem4',
                          'nom_comp_elem5','val_comp_elem5','lat', 'lon','nv_geo_coord',
                          'cod_especie','dsc_estabelecimento','cod_indicador_estab_endereco',
                          'cod_indicador_const_endereco','cod_indicador_finalidade_const','cod_tipo_especi')

      } else {
      files <- files[!grepl(uf,files)]
      # Read file
      temp_dt <- data.table::fread(files)

      # melhor usar dplyr rename para checar nomes de origem !!!!!
      names(temp_dt) <- c('code_state','code_muni', 'code_especie','lat', 'lon','nv_geo_coord')
      cols <- c('code_state','code_muni', 'code_especie','nv_geo_coord', 'lat', 'lon')
      temp_dt <- temp_dt[, ..cols]
    }


  }

  # save in .csv
  # create dir if it has not been created already
  dest_dir <- paste0('./data/', year,"/",file, '/csv/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  fwrite(temp_dt, file = paste0(dest_dir,gsub(".zip",".csv",basename(link))), row.names = F)

  # save in .parquet
  # create dir if it has not been created already
  dest_dir <- paste0('./data/',year,"/",file, '/parquet/')
  if (isFALSE(dir.exists(dest_dir))) { dir.create(dest_dir,
                                                  recursive = T,
                                                  showWarnings = FALSE) }
  arrow::write_parquet(temp_dt, paste0(dest_dir,gsub(".zip",".parquet",basename(link))))

}


