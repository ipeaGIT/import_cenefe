
# function do detect the string of zipped files in url

list_zipfiles_in_url <- function(year) {

  if (year == 2022) {
    ftp <- "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Coordenadas_enderecos/UF/"
  }

  if (year == 2010) {
    ftp <- "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2010/"
  }


  # harvest links

  links <- RCurl::getURL(ftp) |>
            rvest::read_html() |>
            rvest::html_nodes(xpath = '//td/a[@href]') |>
            rvest::html_attr('href')

  links <- links[links %like% 'zip']

  links <- paste0(ftp, links)

  return(links)
}
