# este passo serve para fazer a instalacao dos pacotes necessarios para
# executar o resto dos scripts

# opcional: caso esteja usando linux, para baixar pacotes pre-compilados
options(repos = c(PkgMgr = "https://packagemanager.rstudio.com/all/__linux__/jammy/latest"))

# instala e carrega os pacotes necessarios
instalar_carregar_pacotes <- function(pkg) {
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}

# precisei consultar as seguintes perguntas no stackoverflow:
# https://stackoverflow.com/questions/18023300/is-rgraphviz-no-longer-available-for-r
# https://stackoverflow.com/questions/25114771/glpk-no-such-file-or-directory-error-when-trying-to-install-r-package

# lista de pacotes necessarios
pacotes <- c(
  "arrow",
  "grid",
  "tidyverse",
  "sparklyr",
  "arules",
  #"BiocManager",
  "arulesViz",
  "digest",
  "RJDBC",
  "viridis",
  "shadowtext",
  "httpuv",
  "formattable"
)

# instalar e carregar os pacotes
instalar_carregar_pacotes(pacotes)

# precisei usar esse uma vez para instalar pacotes com dependencia do arulesViz
#BiocManager::install("Rgraphviz")
#BiocManager::install("graph")
