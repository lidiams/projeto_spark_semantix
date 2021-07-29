## Projeto Final de Spark

## **Campanha Nacional de Vacinação contra Covid-19**



### Nível Básico:

Dados: https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar

Referência das Visualizações:

- Site: https://covid.saude.gov.br/
- Guia do Site: Painel Geral



### 1. Enviar os dados para o hdfs

 No terminal Ubuntu, criar o diretório em que os arquivos serão baixados e realizar o download e descompactação do arquivo:

```
~/spark$ cd input

~/spark/input$ sudo curl -O https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar

~/spark/input$ unrar x 04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar 
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe1_1.PNG)

Para enviar os dados para o hdfs, acessar o container namenode:

```
$ docker exec -it namenode bash
root@namenode:/# hdfs dfs -mkdir -p /user/aluno/lidia/projeto_spark
root@namenode:/# hdfs dfs -put /input/*.csv /user/aluno/lidia/projeto_spark
```

Verificando:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe1_2.PNG)



##### Obs: A partir do exercício 2 até o exercício 8, as resoluções também estão disponíveis no arquivo [projeto_spark_basico.ipynb](https://github.com/lidiams/projeto_spark_semantix/blob/main/projeto_spark_basico.ipynb)



### 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.

Acesse o Jupyter notebook pelo endereço http://localhost:8889/

```
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

Leitura dos arquivos:

```
covid_br = spark.read.csv("/user/aluno/lidia/projeto_spark/*.csv", sep = ";", header = "true")
```

Criação das partições, por município:

```
covid_br.write.mode("overwrite").partitionBy("municipio").saveAsTable("covid_br_municipio")
```

Visualização no hdfs:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe2_1.PNG)

### 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe3.PNG)



#### Visualização 1

```
recuperados = covid_br.withColumn("casos_recuperados", col("Recuperadosnovos").cast(IntegerType()))\
    .withColumn("recuperados_em_acompanhamento", col("emAcompanhamentoNovos").cast(IntegerType()))
```

```
recuperados_visualizacao = recuperados.groupBy("regiao", "data")\
    .agg(sum("casos_recuperados").alias("casos_recuperados"),\
    sum("recuperados_em_acompanhamento").alias("em_acompanhamento"))\
    .where(col("regiao") == "Brasil").where(col("data") == "2021-07-06")
```

```
recuperados_visualizacao.select(col("casos_recuperados"), col("em_acompanhamento")).show()
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe3_1.PNG)



#### Visualização 2

```
confirmados = covid_br.withColumn("casos_acumulados", col("casosAcumulado").cast(IntegerType()))\
    .withColumn("casos_novos", col("casosNovos").cast(IntegerType()))\
    .withColumn("incidencia", col("casosAcumulado")/(col("populacaoTCU2019")/100000).cast(FloatType()))
```

```
confirmados_visualizacao = confirmados.groupBy("regiao", "data")\
    .agg(format_number(sum("casos_acumulados"), 0).alias("casos_acumulados")\
    , format_number(sum("casos_novos"), 0).alias("casos_novos")\
    , format_number(sum("incidencia"), 1).alias("incidencia"))\
    .where(col("regiao") == "Brasil").where(col("data") == "2021-07-06")
```

```
print("CASOS CONFIRMADOS")
confirmados_visualizacao.select(col("casos_acumulados"), col("casos_novos"), col("incidencia")).show()
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe3_2.PNG)

#### Visualização 3

```
obitos = covid_br.withColumn("obitos_acumulados", col("obitosAcumulado"))\
    .withColumn("obitos_novos", col("obitosNovos"))\
    .withColumn("letalidade", ((col("obitos_acumulados")/col("casosAcumulado"))*100))\
    .withColumn("mortalidade", col("obitos_acumulados")/(col("populacaoTCU2019")/100000))
```

```
obitos_visualizacao = obitos.groupBy("regiao", "data")\
    .agg(sum("obitos_acumulados").cast(IntegerType()).alias("obitos_acumulados")\
    , sum("obitos_novos").cast(IntegerType()).alias("obitos_novos")\
    , sum("letalidade").cast(FloatType()).alias("letalidade")\
    , sum("mortalidade").cast(FloatType()).alias("mortalidade"))\
    .where(col("data") == "2021-07-06")
```

```
print("ÓBITOS CONFIRMADOS")
obitos_visualizacao.select(format_number(col("obitos_acumulados"), 0).alias("obitos_acumulados")\
    , format_number(col("obitos_novos"), 0).alias("obitos_novos")\
    , format_number(col("letalidade"), 1).alias("letalidade")\
    , format_number(col("mortalidade"), 1).alias("mortalidade"))\
    .where(col("regiao") == "Brasil").show()
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe3_3.PNG)



### 4. Salvar a primeira visualização como tabela Hive

```
recuperados_visualizacao.write.mode("overwrite").format("orc").saveAsTable("recuperados_covid")
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe4.PNG)



### 5. Salvar a segunda visualização com formato parquet e compressão snappy

```
confirmados_visualizacao.write.parquet("/user/aluno/lidia/projeto_spark/confirmados_covid", compression="snappy")
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe5.PNG)



### 6. Salvar a terceira visualização em um tópico no Kafka

```
obitos_visualizacao.selectExpr("CAST(regiao AS STRING) AS key", "to_json(struct(*)) AS value")\
    .write\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("topic","obitos_vi") \
    .save()
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe6_1.PNG)



### 7. Criar a visualização pelo Spark com os dados enviados para o HDFS:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe7.PNG)

```
sintese_casos = covid_br.withColumn("casos_acumulados", col("casosAcumulado").cast(IntegerType()))\
    .withColumn("obitos_acumulados", col("obitosAcumulado").cast(IntegerType()))\
    .withColumn("populacao", col("populacaoTCU2019").cast(IntegerType()))\
```

```
sintese_visualizacao = sintese_casos.groupBy("data", "regiao", "codmun")\
    .agg(format_number(sum("casos_acumulados"), 0).alias("casos")\
    , format_number(sum("obitos_acumulados"), 0).alias("obitos")\
    , format_number(sum("populacao"), 0).alias("populacao")\
    , format_number((sum("casos_acumulados")/(sum("populacao") / 100000)), 1).alias("incidencia")\
    , format_number((sum("obitos_acumulados")/(sum("populacao") / 100000)), 1).alias("mortalidade"))\
    .where(col("codmun").isNull()).where(col("data") == "2021-07-06")\
```

```
print("Síntese de casos, óbitos, incidência e mortalidade")
sintese_visualizacao.select(col("regiao"), col("casos"), col("obitos"), col("incidencia"), col("mortalidade"), col("data")).show()
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe7_1.PNG)



### 8. Salvar a visualização do exercício 6 em um tópico no Elastic

```
obitos_elastic = obitos_visualizacao.where(col("regiao") == "Brasil")
```

```
from elasticsearch import Elasticsearch
```

```
es = Elasticsearch("host.docker.internal:9200")
```

```
obitos_elastic.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "host.docker.internal")\
    .option("es.port", "9200")\
    .option("es.resource", "obitos_elastic")\
    .option("es.nodes.wan.only", "true")\
    .save()
```

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe8.PNG)



### 9. Criar um dashboard no Elastic para visualização dos novos dados enviados

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe9.PNG)

