
# este passo serve para fazer a instalacao dos pacotes necessarios para
# executar o resto dos scripts

# opcional: caso esteja usando linux, para baixar pacotes pre-compilados
if (Sys.info()["sysname"] == "Linux") {
  options(repos = c(PkgMgr = "https://packagemanager.rstudio.com/all/__linux__/jammy/latest"))
}

# instala e carrega os pacotes necessarios
instalar_carregar_pacotes <- function(pkg) {
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}

# lista de pacotes necessarios
pacotes <- c(
  "arrow",
  "tidyverse",
  "sparklyr",
  "arules",
  "viridis",
  "shadowtext",
  "httpuv",
  "formattable"
)

# instalar e carregar os pacotes
instalar_carregar_pacotes(pacotes)

