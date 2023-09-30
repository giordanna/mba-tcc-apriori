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
# [1] 345404

# verifica nossos produtos
glimpse(produtos_parquet)

# conta quantidade de linhas de transacoes (OBS: nao sao transacoes unicas)
sdf_nrow(transacoes_parquet)
# [1] 49272047

# verifica nossas transacoes
glimpse(transacoes_parquet)

# ve se ha valores nulos
transacoes_parquet %>%
  summarise_all(~ sum(as.integer(is.na(.))))

# conta a quantidade de valores faltantes no dataframe
sapply(transacoes_parquet, function(x)
  sum(is.na(x)))

# cria coluna nova: ID_Transacao_Rede + Cpf + CNPJ = id_transacao
transacoes_parquet <-
  transacoes_parquet %>% mutate(id_transacao = paste(ID_Transacao_Rede,
                                                     "_",
                                                     Cpf,
                                                     "_",
                                                     CNPJ,
                                                     sep = ""))

# quantas transacoes, redes, lojas, eans, e cpfs existem
transacoes_parquet %>% summarize(
  transacoes_distintas = n_distinct(id_transacao),
  redes_distintas = n_distinct(Rede),
  lojas_distintas = n_distinct(CNPJ),
  produtos_distintos = n_distinct(EAN),
  clientes_distintos = n_distinct(Cpf)
)

# Source: spark<?> [?? x 5]
# transacoes_distintas redes_distintas lojas_distintas produtos_distintos clientes_distintos
# <int>           <int>           <int>              <int>              <int>
#   1             20939183              79            5554             223617            7511207

# quantos produtos existem (no banco de dados de produtos)
produtos_parquet %>% summarize(produtos_distintos = n_distinct(EAN))

# Source: spark<?> [?? x 1]
# produtos_distintos
# <int>
#   1             345404

################################################################################
# FAZ AGRUPAMENTO DE LOJAS COM MAIS VENDAS
################################################################################

# agrupa cnpjs, conta a quantidade unica de transações, e ve as transacoes
# de farmacias com mais de 20000 transacoes
transacoes_agrupadas <-
  transacoes_parquet %>% select(Rede, CNPJ, id_transacao) %>%
  group_by(CNPJ, Rede) %>%
  summarise(total_transacoes = n_distinct(id_transacao)) %>%
  filter(total_transacoes >= 20000) %>%
  arrange(desc(total_transacoes))

# agrupa cnpjs, conta a quantidade unica de transações, e ve as transacoes
# de farmacias com mais 1000 transacoes e de menos de 20000 transacoes
transacoes_agrupadas_menos_transacoes <-
  transacoes_parquet %>% select(Rede, CNPJ, id_transacao) %>%
  group_by(CNPJ, Rede) %>%
  summarise(total_transacoes = n_distinct(id_transacao)) %>%
  filter(1000 < total_transacoes & total_transacoes < 20000) %>%
  arrange(total_transacoes)

# verifica o agrupamento
glimpse(transacoes_agrupadas)

# verifica o agrupamento de lojas com menos transacoes
glimpse(transacoes_agrupadas_menos_transacoes)


# conta a quantidade de linhas do agrupamento
sdf_nrow(transacoes_agrupadas)
# [1] 156

# conta a quantidade de linhas do agrupamento com menos transacoes
sdf_nrow(transacoes_agrupadas_menos_transacoes)
# [1] 2893

# transfere para um dataframe no R
system.time(transacoes_loja_contagem_R <-
              collect(transacoes_agrupadas))
# usuário   sistema decorrido 
# 0.998     0.127    50.651

# transfere para um dataframe no R
system.time(transacoes_loja_contagem_R_menos_transacoes <-
              collect(transacoes_agrupadas_menos_transacoes))
# usuário   sistema decorrido 
# 1.493     0.161    63.127 

# cria diretorio de resultados
dir.create(file.path("./resultados"))

# cria arquivo CSV com o resultado
write.csv(
  transacoes_loja_contagem_R,
  paste(
    "./resultados/",
    nome_arquivo_transacoes,
    "_por_loja.csv",
    sep = ""
  ),
  row.names = TRUE
)

# cria arquivo CSV com o resultado com menos transacoes
write.csv(
  transacoes_loja_contagem_R_menos_transacoes,
  paste(
    "./resultados/",
    nome_arquivo_transacoes,
    "_por_loja_menos_transacoes.csv",
    sep = ""
  ),
  row.names = TRUE
)

# vamos extrair tbm os possiveis valores distintos da tabela
# produtos, para saber o que usar para classificar os produtos
# de maneira mais generica
lista_coluna_produtos = c(
  "Setor_NEC_Aberto",
  "Molecula",
  "Classe_4",
  "Classe_3",
  "Classe_2",
  "Classe_1",
  "NEC_4",
  "NEC_3",
  "NEC_2",
  "NEC_1"
)

for (i in lista_coluna_produtos) {
  classificacao_coluna <- i
  
  # pega ocorrencias distintas e ordena em ordem alfabetica
  classificacoes_distintas_produtos <- produtos_parquet %>%
    select(all_of(c(classificacao_coluna))) %>% distinct %>% arrange()
  
  # transfere para um dataframe no R
  system.time(produtos_distintos_R <-
                collect(classificacoes_distintas_produtos))
  
  # cria arquivo CSV com o resultado
  write.csv(
    produtos_distintos_R,
    paste(
      "./resultados/",
      "classificacoes_coluna=",
      classificacao_coluna ,
      ".csv",
      sep = ""
    ),
    row.names = TRUE
  )
}
