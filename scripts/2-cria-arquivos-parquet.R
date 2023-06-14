# este passo serve para criar os arquivos parquet, caso esteja com um arquivo
# CSV de milhoes de linhas

# 1 - cria o schema do banco
# 2 - le o arquivo CSV usando o pacote arrow
# 3 - cria o arquivo parquet com ou sem particoes

# o pacote arrow consegue ler e escrever de forma mais eficiente e rapida

# precisei consultar a seguinte pergunta no stackoverflow:
# https://stackoverflow.com/questions/66710855/create-parquet-file-directory-from-csv-file-in-r

################################################################################
# CRIA ARQUIVO PARQUET DE TRANSACOES
################################################################################

# cria schema de transacoes
schema_transacoes <- arrow::schema(
  Mes = int32(),
  Ano = int32(),
  ID_Transacao_Rede = float32(),
  Data_Transacao = string(),
  Rede = string(),
  CNPJ = string(),
  Cpf = string(),
  Sexo = string(),
  Faixa_Etaria_Idade = string(),
  EAN = string(),
  Quantidade = float32(),
  Valor_Total = float32(),
  Valor_Bruto = float32(),
  Valor_Desc = float32(),
  Valor_Liq = float32(),
  Perc_Desc = float32()
)

# le CSV de transacoes
csv_stream_transacoes <- open_dataset(
  "./datasets/transacoes.csv",
  format = "csv",
  schema = schema_transacoes,
  skip_rows = 1,
  delimiter = ";"
)

# cria arquivo parquet (versao particionada)
write_dataset(
  csv_stream_transacoes,
  "./datasets/transacoes_parquet/",
  format = "parquet",
  partitioning = c("Rede"),
  max_rows_per_file = 1000000L,
  hive_style = TRUE,
  existing_data_behavior = "overwrite"
)

################################################################################
# CRIA ARQUIVO PARQUET DE PRODUTOS
################################################################################

# cria schema de produtos
schema_produtos <- arrow::schema(
  EAN = string(),
  Produto = string(),
  Marca = string(),
  Familia = string(),
  Setor_NEC_Aberto = string(),
  Molecula = string(),
  Classe_4 = string(),
  Classe_3 = string(),
  Classe_2 = string(),
  Classe_1 = string(),
  NEC_4 = string(),
  NEC_3 = string(),
  NEC_2 = string(),
  NEC_1 = string(),
  Forma_3 = string(),
  Forma_2 = string(),
  Forma_1 = string()
)

# le CSV de produtos
csv_stream_produtos <- open_dataset(
  "./datasets/produtos.csv",
  format = "csv",
  schema = schema_produtos,
  skip_rows = 1,
  delimiter = ";"
)

# cria arquivo parquet de produtos (sem particionamento)
write_dataset(
  csv_stream_produtos,
  "./datasets/produtos_parquet/",
  format = "parquet",
  max_rows_per_file = 1000000L,
  hive_style = TRUE,
  existing_data_behavior = "overwrite"
)
