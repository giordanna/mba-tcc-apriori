# TCC MBA - Data Science & Analytics

Projeto feito em R, implementa o algoritmo apriori em uma extração de transação de compras.

O projeto cria os arquivos parquet a partir do CSV das transações de compra, separados pela coluna `Rede`. Depois, gera segmentos por mês da transação, faixa etária e sexo. Por fim, itera por estes segmentos e aplicao apriori para cada um, e salva os resultados na pasta `resultados`.

## Pré-requisitos

1. É necessário ter a linguagem R e o R Project instalados;
2. Também é preciso ter o Apache Spark instalado. Lembre-se de configurar corretamente o `spark_home` com o diretório correto da instalação na sua máquina (no meu caso no Ubuntu foi instalado em `/mnt/spark`);
3. Em `datasets` é necessário ter o arquivo `transacoes.csv` e `produtos.csv` para gerar os arquivos Parquet. Veja os arquivos de exemplo;
4. É preciso ter um arquivo de variável de ambiente com o nome `.REnviron`. Veja o arquivo de exemplo.

## Como usar

1. Abra o arquivo `mba-tcc-apriori.Rproj`;
2. Rode os arquivos na pasta `script` em ordem crescente.