# este passo serve para coletar as transacoes de uma loja escolhida, aplicar os
# ajustes necessarios para utilizar o algoritmo apriori de forma mais
# automatizada possivel

readRenviron(".Renviron")

# escolhe a rede e o cnpj a partir das variaveis de ambiente
rede <- Sys.getenv("REDE")
cnpj <- Sys.getenv("CNPJ")
nome_arquivo_transacoes <-
  Sys.getenv("NOME_ARQUIVO_TRANSACOES", unset = "transacoes")

# escolhe a coluna que sera usada para classificar o produto das transacoes
coluna_classificacao <-
  Sys.getenv("COLUNA_CLASSIFICACAO", unset = "NEC_2")
filtroProdutoOTC <-
  Sys.getenv("FILTRO_PRODUTO_OTC", unset = "98A - NOT OTC")

# define valores do apriori
support <- Sys.getenv("APRIORI_SUPPORT", unset =  "0.001")
confidence <- Sys.getenv("APRIORI_CONFIDENCE", unset = "0.4")
minlen <- Sys.getenv("APRIORI_MINLEN", unset = "2")
maxlen <- Sys.getenv("APRIORI_MAXLEN", unset = "2")

support = as.double(support)
confidence = as.double(confidence)
minlen = as.integer(minlen)
maxlen = as.integer(maxlen)

# define quando deve mudar a posicao da porcentagem no grafico de barras,
# pro questoes esteticas
limite_posicao_porcentagem <- 6.5

#  1 - conecta com o spark
#  2 - le o arquivo parquet das transacoes
#  3 - le o arquivo parquet dos produtos
#  4 - filtra o banco de transacoes para pegar apenas da loja selecionada e com
#      faixa etaria valida
#  5 - filtra o banco de produtos para pegar apenas produtos OTC
#  6 - junta as duas tabelas com um inner join pela chave EAN
#  7 - coleta os dados para um dataframe no R
#  8 - transforma os IDs de transacao e categorias do produto em factor
#  9 - itera pelos meses das transacoes
# 10 - cria graficos de frequencia das transacoes por faixa etaria e sexo
# 11 - cria segmentos por faixa etaria e sexo
# 12 - itera nesses segmentos, filtra transacoes e aplica o apriori. salva os
#      resultados

# (pule este passo caso tenha executado ja o arquivo
# 3-verifica-lojas-com-mais-vendas.R e nao fechou a conexao com o spark ainda)

################################################################################
# CRIA CONEXAO COM O APACHE SPARK
################################################################################

# desconecta alguma conexão ativa com o spark
spark_disconnect_all()

# conecta com o spark
sc <- spark_connect(master = "local", spark_home = "/mnt/spark")

# lendo transacoes de arquivo parquet
system.time(
  transacoes_parquet <- spark_read_parquet(sc,
                                           name = "transacoes_parquet",
                                           path =  encodeURI(
                                             paste(
                                               "./datasets/",
                                               nome_arquivo_transacoes,
                                               "_parquet/Rede=",
                                               rede,
                                               sep = ""
                                             )
                                           ))
)

# le produtos em arquivo parquet
system.time(
  produtos_parquet <- spark_read_parquet(sc,
                                         name = "produtos_parquet",
                                         path = "./datasets/produtos_parquet")
)

################################################################################
# DATA WRANGLING
################################################################################

# desativa graficos do R (todos vao ser salvos no computador)
graphics.off()

# seleciona apenas o que vai ser usado
# combina duas colunas: ID_Transacao_Rede e Cpf, para gerar um ID unico que
# identifica a transacao por cliente em uma nova coluna: id_transacao
transacoes_parquet_filtradas <-
  transacoes_parquet %>% mutate(id_transacao = paste(ID_Transacao_Rede,
                                                     "_",
                                                     Cpf,
                                                     sep = "")) %>%
  filter(CNPJ == cnpj,  Faixa_Etaria_Idade != "NÃƒO DEFINIDA") %>%
  select(Ano, Mes, Sexo, Faixa_Etaria_Idade, EAN, id_transacao)


glimpse(produtos_parquet)

# aqui como a coluna vai ser dinamico entao o filtro para filtrar produtos Rx
# deve ser dinamico tbm
produtos_parquet_filtrados <-
  produtos_parquet %>% select(EAN, !!as.name(coluna_classificacao)) %>%
  filter(!!as.name(coluna_classificacao) != filtroProdutoOTC)

# inner join
transacoes_com_produtos_parquet <-
  inner_join(transacoes_parquet_filtradas,
             produtos_parquet_filtrados,
             by = c("EAN"))

# transfere para um dataframe no R
system.time(transacoes_R <-
              collect(transacoes_com_produtos_parquet))

# transforma colunas em factor
colunas_factor <-
  c("id_transacao",
    coluna_classificacao)

transacoes_R[colunas_factor] <-
  lapply(transacoes_R[colunas_factor], factor)

################################################################################
# CRIACAO DE SEGMENTOS DE MES
################################################################################

# conta a quantidade de transacoes distintas
qtd_transacoes_unicas <- n_distinct(transacoes_R$id_transacao)

# ve os meses da extracao
tipos_meses <-
  transacoes_R %>% select(Mes, id_transacao) %>%
  group_by(Mes) %>%
  summarise(
    total_transacoes = n_distinct(id_transacao),
    porcentagem_double = 100 * n_distinct(id_transacao) / qtd_transacoes_unicas,
    porcentagem = percent(n_distinct(id_transacao) / qtd_transacoes_unicas)
  ) %>%
  arrange(Mes)

# garante que eh do tipo dataframe (poderia estar como tibble)
tipos_meses <- as.data.frame(tipos_meses)

# cria dataframe de segmentos de meses
segmentos_mes <-
  data.frame(Mes = NA)

# cria array de segmentos de meses para iterar
for (i in 1:nrow(tipos_meses)) {
  segmentos_mes[nrow(segmentos_mes) + 1,] <-
    c(tipos_meses[i, 1])
}

# cria diretorio de resultados
dir.create(file.path("./resultados"))

################################################################################
# LOOP DE SEGMENTOS DE MES
################################################################################
for (i in 1:nrow(segmentos_mes)) {
  transacoes_mes = transacoes_R
  
  # cria diretorio de resultados por cnpj
  diretorio_resultados = paste("./resultados/Coluna=",
                               coluna_classificacao ,
                               ";CNPJ=",
                               cnpj,
                               "/",
                               sep = "")
  
  # se mes for diferente de NA, cria diretorio de mês
  if (!is.na(segmentos_mes[i, 1])) {
    diretorio_resultados = paste(
      "./resultados/Coluna=",
      coluna_classificacao ,
      ";Mes=",
      segmentos_mes[i, 1],
      ";CNPJ=",
      cnpj,
      "/",
      sep = ""
    )
    
    # adiciona filtro de mes
    transacoes_mes <-
      transacoes_mes %>% filter(Mes == segmentos_mes[i, 1])
  }
  
  # conta quantidade de transacoes unicas pra cada mes
  qtd_transacoes_unicas_mes <-
    n_distinct(transacoes_mes$id_transacao)
  
  # cria diretorios de frequencias e CSVs dentro da pasta de resultados
  dir.create(file.path(diretorio_resultados))
  dir.create(file.path(diretorio_resultados, 'frequencias'))
  dir.create(file.path(diretorio_resultados, 'csvs'))
  
  # ve faixa etaria e a frequencia
  tipos_faixa_etaria <-
    transacoes_mes %>%
    group_by(Faixa_Etaria_Idade) %>%
    summarise(
      total_transacoes = n_distinct(id_transacao),
      porcentagem_double = 100 * n_distinct(id_transacao) / qtd_transacoes_unicas,
      porcentagem = percent(n_distinct(id_transacao) / qtd_transacoes_unicas_mes)
    ) %>%
    arrange(desc(total_transacoes))
  
  # ve sexo e frequencia
  tipos_sexo <-
    transacoes_mes %>% select(Sexo, id_transacao) %>%
    group_by(Sexo) %>%
    summarise(
      total_transacoes = n_distinct(id_transacao),
      porcentagem_double = 100 * n_distinct(id_transacao) / qtd_transacoes_unicas,
      porcentagem =  percent(n_distinct(id_transacao) / qtd_transacoes_unicas_mes)
    ) %>%
    arrange(desc(total_transacoes))
  
  ##############################################################################
  # GRAFICO PLOT POR FAIXA ETARIA
  ##############################################################################
  
  plot_tipos_faixa_etaria <- tipos_faixa_etaria %>%
    ggplot(aes(x = Faixa_Etaria_Idade, y = total_transacoes)) +
    geom_bar(stat = "identity", aes(fill = Faixa_Etaria_Idade)) +
    scale_fill_viridis_d(direction = -1) +
    labs(
      fill = "Faixa Etária",
      y = paste("Transações (total=",
                qtd_transacoes_unicas_mes,
                ")",
                sep = ""),
      x = "Faixa Etária"
    ) +
    geom_shadowtext(
      aes(label = paste(
        total_transacoes, " (", porcentagem, ")", sep = ""
      )),
      size = 8,
      hjust = if_else(
        tipos_faixa_etaria$porcentagem_double <= limite_posicao_porcentagem,
        -0.1,
        1.1
      )
    ) +
    coord_flip() +
    theme(
      text = element_text(size = 24),
      legend.position = "none",
      plot.margin = unit(c(1, 1, 1, 1), "cm")
    )
  png(
    file = paste(diretorio_resultados,
                 "Faixa_Etaria_Idade.png",
                 sep = ""),
    width = 1200,
    height = 600,
  )
  print(plot_tipos_faixa_etaria)
  dev.off()
  
  ##############################################################################
  # GRAFICO PLOT POR SEXO
  ##############################################################################
  
  plot_tipos_sexo <- tipos_sexo %>%
    ggplot(aes(x = Sexo, y = total_transacoes)) +
    geom_bar(stat = "identity", aes(fill = Sexo)) +
    scale_fill_viridis_d(direction = -1) +
    labs(
      fill = "Sexo",
      y = paste("Transações (total=",
                qtd_transacoes_unicas_mes,
                ")",
                sep = ""),
      x = "Sexo"
    ) +
    geom_shadowtext(
      aes(label = paste(
        total_transacoes, " (", porcentagem, ")", sep = ""
      )),
      size = 12,
      hjust = if_else(
        tipos_sexo$porcentagem_double <= limite_posicao_porcentagem,
        -0.1,
        1.1
      )
    ) +
    coord_flip() +
    theme(
      text = element_text(size = 32),
      legend.position = "none",
      plot.margin = unit(c(1, 1, 1, 1), "cm")
    )
  png(
    file = paste(diretorio_resultados,
                 "Sexo.png",
                 sep = ""),
    width = 1200,
    height = 600
  )
  print(plot_tipos_sexo)
  dev.off()
  
  ##############################################################################
  # CRIA LISTA DE SEGMENTOS DE PUBLICO ALVO E SEXO
  ##############################################################################
  
  publicos_alvo <-
    data.frame(Sexo = NA, Faixa_Etaria_Idade = NA)
  
  # converte de novo pra dataframe (tava como tibble)
  tipos_faixa_etaria <- as.data.frame(tipos_faixa_etaria)
  tipos_sexo <- as.data.frame(tipos_sexo)
  
  tipos_faixa_etaria[4, 1]
  
  # itera pelas faixas etarias
  for (j in 1:nrow(tipos_faixa_etaria)) {
    publicos_alvo[nrow(publicos_alvo) + 1,] <-
      c(NA, tipos_faixa_etaria[j, 1])
  }
  
  # itera pelos sexos
  for (j in 1:nrow(tipos_sexo)) {
    publicos_alvo[nrow(publicos_alvo) + 1,] <- c(tipos_sexo[j, 1], NA)
    
    for (k in 1:nrow(tipos_faixa_etaria)) {
      publicos_alvo[nrow(publicos_alvo) + 1,] <-
        c(tipos_sexo[j, 1], tipos_faixa_etaria[k, 1])
    }
  }
  
  # cria array vazio das regras para salvar e consultar depois do loop
  todas_regras <- list()
  
  ##############################################################################
  # LOOP POR SEGMENTO DE FAIXA ETARIA
  ##############################################################################
  for (j in 1:nrow(publicos_alvo)) {
    # envolve em um try catch
    try({
      transacoes_publico_alvo <- transacoes_mes
      sexo_atual <- "Todos"
      faixa_etaria_atual <- "Todas"
      
      # adiciona filtro de sexo caso seja diferente de NA
      if (!is.na(publicos_alvo[j, 1])) {
        sexo_atual <- publicos_alvo[j, 1]
        
        transacoes_publico_alvo <-
          transacoes_publico_alvo %>% filter(Sexo == publicos_alvo[j, 1])
      }
      
      # adiciona filtro de faixa etaria caso seja diferente de NA
      if (!is.na(publicos_alvo[j, 2])) {
        faixa_etaria_atual <- publicos_alvo[j, 2]
        
        transacoes_publico_alvo <-
          transacoes_publico_alvo %>% filter(Faixa_Etaria_Idade == publicos_alvo[j, 2])
      }
      
      # conta a quantidade de transacoes do segmento (faixa etaria + sexo)
      qtd_transacoes_unicas_segmento <-
        n_distinct(transacoes_publico_alvo$id_transacao)
      
      ##########################################################################
      # APRIORI
      ##########################################################################
      
      # escreve uma tabela temporaria
      write.table(transacoes_publico_alvo,
                  file = tmp <- file(),
                  row.names = FALSE)
      
      # le dataframe para transactions
      transacoes <- read.transactions(
        tmp,
        format = "single",
        header = TRUE,
        cols = c("id_transacao", coluna_classificacao)
      )
      
      # fecha tabela temporaria
      close(tmp)
      
      # roda o apriori
      rules <-
        apriori(
          transacoes,
          parameter = list(
            support = support,
            confidence = confidence,
            minlen = minlen,
            maxlen = maxlen
          )
        )
      
      # ordena pelo lift
      rules <-
        sort(rules,
             decreasing = TRUE,
             na.last = NA,
             by = "lift")
      
      # salva resultado na lista
      todas_regras[[j]] <- list(
        Transacoes = transacoes,
        Sexo = sexo_atual,
        Faixa_Etaria_Idade = faixa_etaria_atual,
        Regras = rules
      )
      
      # escreve resultado em csv
      write(
        rules,
        file = paste(
          diretorio_resultados,
          "csvs/Sexo=",
          sexo_atual,
          "_Faixa_Etaria_Idade=",
          faixa_etaria_atual ,
          ".csv",
          sep = ""
        ),
        sep = ";",
        quote = TRUE,
        row.names = TRUE
      )
      
      ##########################################################################
      # GRAFICO PLOT ITENS MAIS FREQUENTES
      ##########################################################################
      
      itens_mais_frequentes <-
        transacoes_publico_alvo %>% select(!!as.name(coluna_classificacao),
                                           id_transacao) %>%
        group_by(!!as.name(coluna_classificacao)) %>%
        summarise(
          total_transacoes = n_distinct(id_transacao),
          porcentagem_double = 100 * n_distinct(id_transacao) / qtd_transacoes_unicas_segmento,
          porcentagem =  percent(
            n_distinct(id_transacao) / qtd_transacoes_unicas_segmento
          )
        ) %>%
        slice_max(total_transacoes, n = 10)
      
      plot_itens_frequentes <- itens_mais_frequentes %>%
        ggplot(aes(
          x = reorder(!!as.name(coluna_classificacao), -total_transacoes),
          y = total_transacoes
        )) +
        geom_bar(stat = "identity", aes(fill = total_transacoes)) +
        scale_fill_viridis_c() +
        labs(
          fill = "Produto",
          y = paste(
            "Transações (total=",
            qtd_transacoes_unicas_segmento,
            ")",
            sep = ""
          ),
          x = "Produto"
        ) +
        geom_shadowtext(
          aes(label = paste(
            total_transacoes, " (", porcentagem, ")", sep = ""
          )),
          size = 8,
          hjust = if_else(
            itens_mais_frequentes$porcentagem_double <= limite_posicao_porcentagem,
            -0.1,
            1.1
          )
        ) +
        coord_flip() +
        theme(
          text = element_text(size = 24),
          legend.position = "none",
          plot.margin = unit(c(1, 1, 1, 1), "cm")
        ) +
        ggtitle(
          paste(
            "Segmento: sexo=",
            sexo_atual,
            " e faixa etária=",
            faixa_etaria_atual,
            sep = ""
          )
        )
      
      png(
        file = paste(
          diretorio_resultados,
          "frequencias/Sexo=",
          sexo_atual,
          "_Faixa_Etaria_Idade=",
          faixa_etaria_atual,
          ".png",
          sep = ""
        ),
        width = 1200,
        height = 600
      )
      print(plot_itens_frequentes)
      dev.off()
      
    })
  }
  
  # percorre todas as regras e gera um data frame contendo todas as regras
  todas_regras_dataframe <- data.frame(
    sexo = character(0),
    faixa_etaria = character(0),
    lhs = character(0),
    rhs = character(0),
    support = numeric(0),
    confidence = numeric(0),
    coverage = numeric(0),
    lift = numeric(0),
    count = numeric(0)
  )
  
  # junta todas as regras
  for (regra in todas_regras) {
    todas_regras_dataframe <- todas_regras_dataframe %>%
      bind_rows(
        data.frame(
          sexo = regra$Sexo,
          faixa_etaria =  regra$Faixa_Etaria_Idade,
          lhs = labels(lhs(regra$Regras)),
          rhs = labels(rhs(regra$Regras)),
          regra$Regras@quality
        )
      )
  }
  
  # filtra apenas as que tiveram lift maior que dois e ordena
  todas_regras_dataframe <-
    todas_regras_dataframe %>% filter(lift >= 2) %>%
    arrange(desc(lift))
  
  # salva regras em um CSV
  write.csv(
    todas_regras_dataframe,
    paste(
      diretorio_resultados,
      "csvs/Todas_Regras_Consideraveis.csv",
      sep = ""
    ),
    sep = ";",
    quote = TRUE,
    row.names = TRUE
  )
}
