# Load packages required to define the pipeline:
suppressPackageStartupMessages({
  library(targets)
  library(rvest)
  library(arrow)
  library(data.table)
  library(RCurl)
  library(dplyr)
})



# Run the R scripts in the R/ folder with your custom functions:
source("R/list_files_in_url.R")
source("R/download_and_save_cnefe.R")
source("R/utils.R")


# Sequencia de tarefas
list(
  # Anos em que a base está disponível
  tar_target(
    year,
    c(2022) # 2010
  )

  # Gerar URLs dos arquivos
  , tar_target(
    name = get_files_links,
    command = list_zipfiles_in_url(year),
    pattern = map(year)  # Isso garante que cada ano seja tratado individualmente
  )


  # Baixar arquivos para pasta data_raw e salvar em .csv e .parquet na pasta data
  , tar_target(
    name = download_and_save,
    command = download_and_save_cnefe(get_files_links),
    pattern = cross(year, get_files_links)  # Isso garante que cada link seja tratado individualmente
  )
)
