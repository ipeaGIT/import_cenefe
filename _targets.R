# Load packages required to define the pipeline:
suppressPackageStartupMessages({
  library(targets)
  library(rvest)
  library(future)
  library(furrr)
  library(arrow)
  library(data.table)
  library(RCurl)
  library(dplyr)
})

# Set target options:
tar_option_set(
  packages = c("tibble") # packages that your targets need to run
  # format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # For distributed computing in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller with 2 workers which will run as local R processes:
  #
  #   controller = crew::crew_controller_local(workers = 2)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package. The following
  # example is a controller for Sun Grid Engine (SGE).
  #
  #   controller = crew.cluster::crew_controller_sge(
  #     workers = 50,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.0".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
)

# tar_make_clustermq() is an older (pre-{crew}) way to do distributed computing
# in {targets}, and its configuration for your machine is below.
options(clustermq.scheduler = "multiprocess")

# tar_make_future() is an older (pre-{crew}) way to do distributed computing
# in {targets}, and its configuration for your machine is below.
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Run the R scripts in the R/ folder with your custom functions:
source("R/list_files_in_url.R")
source("R/download_and_save_cnefe.R")
source("R/utils.R")


# Sequencia de tarefas
list(
  # Anos em que a base está disponível
  tar_target(
    year,
    c( 2022) # 2010
  ),

  # Gerar URLs dos arquivos
  tar_target(
    name = get_files_links,
    command = list_zipfiles_in_url(year),
    pattern = map(year)  # Isso garante que cada ano seja tratado individualmente
  ),

  # Baixar arquivos para pasta data_raw e salvar em .csv e .parquet na pasta data
  tar_target(
    name = download_and_save,
    command = download_and_save_cnefe(get_files_links),
    pattern = map(get_files_links)  # Isso garante que cada link seja tratado individualmente
  )
)
