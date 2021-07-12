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



### 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.

Acesse o Jupyter notebook pelo endereço http://localhost:8889/

```
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

Leitura dos arquivos:

```
covid_br = spark.read.csv("/user/aluno/lidia/projeto_spark/*.csv", sep = ";", header = "true").show(5)
```

Criação das partições, por município:

```
covid_br.write.mode("overwrite").partitionBy("municipio").saveAsTable("covid_br_municipio")
```

Visualização no hdfs:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe2_1.PNG)

### 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe3.PNG)

### 4. Salvar a primeira visualização como tabela Hive





### 5. Salvar a segunda visualização com formato parquet e compressão snappy





### 6. Salvar a terceira visualização em um tópico no Kafka





### 7. Criar a visualização pelo Spark com os dados enviados para o HDFS:

![](https://github.com/lidiams/projeto_spark_semantix/blob/main/images/exe7.PNG)





### 8. Salvar a visualização do exercício 6 em um tópico no Elastic





### 9. Criar um dashboard no Elastic para visualização dos novos dados enviados