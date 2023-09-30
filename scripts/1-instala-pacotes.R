
# este passo serve para fazer a instalacao dos pacotes necessarios para
# executar o resto dos scripts

# opcional: caso esteja usando linux, para baixar pacotes pre-compilados
if (Sys.info()["sysname"] == "Linux") {
  # a versao mais atual no momento da distro ubuntu eh a jammy 22.04,
  # pra outras versoes do linux deve ter outros diretorios
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
  "formattable",
  "extrafont"
)

# instalar e carregar os pacotes
instalar_carregar_pacotes(pacotes)

