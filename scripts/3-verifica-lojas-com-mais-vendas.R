# este passo serve para verificar quais sao as lojas com mais transacoes da
# nossa extracao, e salva em um CSV. para isso, vamos usar o Apache Spark para
# fazer uma leitura rapida em memoria do arquivo parquet da extracao

readRenviron(".Renviron")

nome_arquivo_transacoes <-
  Sys.getenv("NOME_ARQUIVO_TRANSACOES", unset = "transacoes")

# 1 - conecta com o spark
# 2 - le o arquivo parquet das transacoes
# 3 - le o arquivo parquet dos produtos (apenas para observar a quantidade)
# 4 - realiza algumas operacoes para conhecer nossa base de dados
# 5 - faz o agrupamento usando as funcoes do dplyr
# 6 - coleta os dados para um dataframe no R
# 7 - salva o dataframe para um CSV

################################################################################
# CRIA CONEXAO COM O APACHE SPARK
################################################################################

# desconecta alguma conexão ativa com o spark
spark_disconnect_all()

# conecta com o spark
sc <- spark_connect(master = "local", spark_home = "/mnt/spark")

# le transacoes
system.time(
  transacoes_parquet <- spark_read_parquet(
    sc,
    name = "transacoes_parquet",
    path = paste("./datasets/", nome_arquivo_transacoes, "_parquet", sep =
                   "")
  )
)

# le produtos
system.time(
  produtos_parquet <- spark_read_parquet(sc,
                                         name = "produtos_parquet",
                                         path = "./datasets/produtos_parquet")
)

################################################################################
# CONHECENDO NOSSOS BANCOS DE DADOS
################################################################################

# conta quantidade de linhas de produtos
sdf_nrow(produtos_parquet)

# verifica nossos produtos
glimpse(produtos_parquet)

# conta quantidade de linhas de transacoes (OBS: nao sao transacoes unicas)
sdf_nrow(transacoes_parquet)

# verifica nossas transacoes
glimpse(transacoes_parquet)

# ve se ha valores nulos
transacoes_parquet %>%
  summarise_all(~ sum(as.integer(is.na(.))))

# conta a quantidade de valores faltantes no dataframe
sapply(transacoes_parquet, function(x)
  sum(is.na(x)))

# quantas transacoes existem
transacoes_parquet %>% summarize(redes_distintas = n_distinct(ID_Transacao_Rede))

# quantas bandeiras de rede existem
transacoes_parquet %>% summarize(redes_distintas = n_distinct(Rede))

# quantas farmacias existem
transacoes_parquet %>% summarize(lojas_distintas = n_distinct(CNPJ))

# quantos produtos existem (no banco de dados de transacoes)
transacoes_parquet %>% summarize(produtos_distintos = n_distinct(EAN))

# quantos clientes existem
transacoes_parquet %>% summarize(clientes_distintos = n_distinct(Cpf))

# quantos produtos existem (no banco de dados de produtos)
produtos_parquet %>% summarize(produtos_distintos = n_distinct(EAN))

################################################################################
# FAZ AGRUPAMENTO DE LOJAS COM MAIS VENDAS
################################################################################

# agrupa cnpjs, conta a quantidade unica de transações, e ve as transacoes
# de farmacias com mais de 20000 transacoes
transacoes_agrupadas <-
  transacoes_parquet %>% select(Rede, ID_Transacao_Rede, CNPJ) %>%
  group_by(CNPJ, Rede) %>%
  summarise(total_transacoes = n_distinct(ID_Transacao_Rede)) %>%
  filter(total_transacoes >= 20000) %>%
  arrange(desc(total_transacoes))

# verifica o agrupamento
glimpse(transacoes_agrupadas)

# conta a quantidade de linhas do agrupamento
sdf_nrow(transacoes_agrupadas)

# transfere para um dataframe no R
system.time(transacoes_loja_contagem_R <-
              collect(transacoes_agrupadas))

# cria diretorio de resultados
dir.create(file.path("./resultados"))

# cria arquivo CSV com o resultado
write.csv(
  transacoes_agrupadas,
  paste(
    "./resultados/",
    nome_arquivo_transacoes,
    "_por_loja.csv",
    sep = ""
  ),
  row.names = TRUE
)
