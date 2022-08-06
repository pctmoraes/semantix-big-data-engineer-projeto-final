# semantix-big-data-engineer-projeto-final
Projeto final para o curso Big Data Engineer oferecido pela Semantix

---

## Sobre o curso
Instituição: [Semantix Academy](https://www.semantix.ai/)  
Curso: Big Data Engineer  
Professor: Rodrigo Rebouças  
Turma: 05-22  

Módulos:  
- Big Data Foundations (Semana 1, 2 e 3)
- MongoDB - Básico (Semana 4)
- Redis – Básico (Semana 5)
- Apache Kafka – Básico (Semana 6)
- Elasticsearch I (Semana 7 e 8)
- Spark - Big Data Processing (Semana 9, 10 e 11)

---

## Descrição do projeto  

<br>

O **Projeto final** contido neste repositório integra ferramentas e habilidades adquiridas durante o curso de Big Data Engineer, nele foi utilizado dados da [campanha de vacição do COVID-19](https://covid.saude.gov.br/) para realizar os 9 exercícios do projeto, o fluxo se deu através da **(1)** ingestão dos dados para o [HDFS](https://hadoop.apache.org/), **(2)** leitura dos dados com [PySpark](https://www.databricks.com/glossary/pyspark) usando [Jupyter](https://jupyter.org/), **(3)** criação de [DataFrames](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html), aplicando as transformações necessárias, **(4)** escrita das tabelas no [Hive](https://hive.apache.org/), no [Kafka](https://kafka.apache.org/) e no [Elastic](https://www.elastic.co/pt/) e criação de dashboards com o [Kibana](https://www.elastic.co/pt/kibana/).


---

## • HANDS ON •

<br>

## Projeto final de Spark - nível básico

<br>

Campanha Nacional de Vacinação contra Covid-19

**Tabela de conceitos** retirado do site [coronavirus.sc.gov.br](https://www.coronavirus.sc.gov.br/sobre-os-dados/)  

- **Casos confirmados**: Cada caso confirmado é um paciente que teve a infecção por coronavírus confirmada por exame laboratorial (teste rápido sorológico ou RT-PCR) ou por vínculo epidemiológico. Os gráficos que exibem a evolução deste número ao longo do tempo levam em consideração a data de início dos sintomas, permitindo identificar com maior precisão o momento da infecção pelo vírus SARS-CoV-2. Quando a data de início dos sintomas não está disponível, a referência utilizada é a data de coleta do exame. Os casos que evoluem a óbito e os recuperados estão incluídos neste número.

- **Óbitos**: Pacientes que faleceram em decorrência da Covid-19, com confirmação laboratorial ou por critério clínico epidemiológico. Os gráficos que exibem a evolução deste número ao longo do tempo levam em consideração a data do óbito.

- **Recuperados**: Estimativa realizada com base no tempo decorrido a partir do início dos sintomas e a evolução de cada caso. Entram no cálculo os pacientes que tiveram início de sintomas há pelo menos 14 dias, não evoluíram a óbito e não se encontram em internação hospitalar. Uma limitação a ser considerada nessa estimativa é o fato de ser possível a existência de pacientes que, mesmo se enquadrando nos critérios, ainda estejam em acompanhamento, assim como há a possibilidade de se atingir a recuperação antes do período de 14 dias.

- **Letalidade**: Taxa percentual de casos confirmados que evoluíram a óbito. Método de cálculo: (número de óbitos x 100) / número de casos confirmados.

- **Incidência**: Quantidade proporcional de casos confirmados para cada 1 milhão de habitantes. A população considerada no cálculo é a estimativa populacional para 2019 do Instituto Brasileiro de Geografia e Estatística (IBGE), salvo quando especificada referência distinta. Método de cálculo: (casos confirmados * 1.000.000) / população.

- **Mortalidade**: Quantidade proporcional de óbitos para cada 1 milhão habitantes, seguindo a mesma metodologia de cálculo da incidência de casos confirmados. Método de cálculo: (óbitos * 1.000.000) / população.

<br>

### Exercício 1 - Enviar os dados para o hdfs

Resolução:

1. abri um terminal linux
2. acessei o diretório com o comando
```
cd treinamentos/spark
```
3. baixei os dados sobre a campanha de vacinação do covid-19 com o comando
```
sudo curl -O https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar
```
4. instalei o programa para fazer a extração dos dados que estavam compactados com o comando
```
sudo apt install unrar
```
5. extraí os dados com o comando
```
unrar x 04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar
```
6. movi os arquivos extraídos para o diretório /input com o comando
```
sudo mv *.csv /home/pmoraes/treinamentos/spark/input
```
7. inicializei o docker com o comando 
```
docker-compose start
```
8. acessei o container namenode com o comando
```
docker exec -it namenode bash
```
9. criei uma estrutura de diretórios para receber os arquivos do linux com o comando
```
hdfs dfs -mkdir -p /user/paula/spark/projeto_final
```
10. transferi os arquivos do linux para o hadoop com o comando
```
hdfs dfs -put /input/*.csv /user/paula/spark/projeto_final
```

### Exercício 2 - Otimizar todos os dados do HDFS para uma tabela Hive particionada por municipio

Resolução:
1. importei as bibliotecas e criei a sessão do spark


```python
import pyspark as spark
from pyspark.sql.functions import *

spark = SparkSession\
.builder\
.appName('Projeto final de Spark - Campanha Nacional de Vacinação contra Covid-19')\
.config('spark.some.config.option', 'some-value')\
.enableHiveSupport()\
.getOrCreate()
```

2. criei o dataframe através da leitura dos arquivos CSV no diretório do hdfs


```python
df = spark.read.csv('hdfs://namenode/user/paula/spark/projeto_final/', sep=";",header=True, inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)
```

3. imprimi o schema do dataframe


```python
df.printSchema()
```

    root
     |-- regiao: string (nullable = true)
     |-- estado: string (nullable = true)
     |-- municipio: string (nullable = true)
     |-- coduf: integer (nullable = true)
     |-- codmun: integer (nullable = true)
     |-- codRegiaoSaude: integer (nullable = true)
     |-- nomeRegiaoSaude: string (nullable = true)
     |-- data: timestamp (nullable = true)
     |-- semanaEpi: integer (nullable = true)
     |-- populacaoTCU2019: integer (nullable = true)
     |-- casosAcumulado: decimal(10,0) (nullable = true)
     |-- casosNovos: integer (nullable = true)
     |-- obitosAcumulado: integer (nullable = true)
     |-- obitosNovos: integer (nullable = true)
     |-- Recuperadosnovos: integer (nullable = true)
     |-- emAcompanhamentoNovos: integer (nullable = true)
     |-- interior/metropolitana: integer (nullable = true)
    


4. transformei o campo data no formato de tempo unix e visualizei de forma vertical os 6 primeiros registros, de modo a se ter uma melhor visualização da porção do dataframe


```python
df_to_unix = df.withColumn('data', from_unixtime(unix_timestamp(df.data), 'yyyy-MM-dd'))
df_to_unix.show(6, vertical=True)
```

    -RECORD 0----------------------------
     regiao                 | Brasil     
     estado                 | null       
     municipio              | null       
     coduf                  | 76         
     codmun                 | null       
     codRegiaoSaude         | null       
     nomeRegiaoSaude        | null       
     data                   | 2020-02-25 
     semanaEpi              | 9          
     populacaoTCU2019       | 210147125  
     casosAcumulado         | 0          
     casosNovos             | 0          
     obitosAcumulado        | 0          
     obitosNovos            | 0          
     Recuperadosnovos       | null       
     emAcompanhamentoNovos  | null       
     interior/metropolitana | null       
    -RECORD 1----------------------------
     regiao                 | Brasil     
     estado                 | null       
     municipio              | null       
     coduf                  | 76         
     codmun                 | null       
     codRegiaoSaude         | null       
     nomeRegiaoSaude        | null       
     data                   | 2020-02-26 
     semanaEpi              | 9          
     populacaoTCU2019       | 210147125  
     casosAcumulado         | 1          
     casosNovos             | 1          
     obitosAcumulado        | 0          
     obitosNovos            | 0          
     Recuperadosnovos       | null       
     emAcompanhamentoNovos  | null       
     interior/metropolitana | null       
    -RECORD 2----------------------------
     regiao                 | Brasil     
     estado                 | null       
     municipio              | null       
     coduf                  | 76         
     codmun                 | null       
     codRegiaoSaude         | null       
     nomeRegiaoSaude        | null       
     data                   | 2020-02-27 
     semanaEpi              | 9          
     populacaoTCU2019       | 210147125  
     casosAcumulado         | 1          
     casosNovos             | 0          
     obitosAcumulado        | 0          
     obitosNovos            | 0          
     Recuperadosnovos       | null       
     emAcompanhamentoNovos  | null       
     interior/metropolitana | null       
    -RECORD 3----------------------------
     regiao                 | Brasil     
     estado                 | null       
     municipio              | null       
     coduf                  | 76         
     codmun                 | null       
     codRegiaoSaude         | null       
     nomeRegiaoSaude        | null       
     data                   | 2020-02-28 
     semanaEpi              | 9          
     populacaoTCU2019       | 210147125  
     casosAcumulado         | 1          
     casosNovos             | 0          
     obitosAcumulado        | 0          
     obitosNovos            | 0          
     Recuperadosnovos       | null       
     emAcompanhamentoNovos  | null       
     interior/metropolitana | null       
    -RECORD 4----------------------------
     regiao                 | Brasil     
     estado                 | null       
     municipio              | null       
     coduf                  | 76         
     codmun                 | null       
     codRegiaoSaude         | null       
     nomeRegiaoSaude        | null       
     data                   | 2020-02-29 
     semanaEpi              | 9          
     populacaoTCU2019       | 210147125  
     casosAcumulado         | 2          
     casosNovos             | 1          
     obitosAcumulado        | 0          
     obitosNovos            | 0          
     Recuperadosnovos       | null       
     emAcompanhamentoNovos  | null       
     interior/metropolitana | null       
    -RECORD 5----------------------------
     regiao                 | Brasil     
     estado                 | null       
     municipio              | null       
     coduf                  | 76         
     codmun                 | null       
     codRegiaoSaude         | null       
     nomeRegiaoSaude        | null       
     data                   | 2020-03-01 
     semanaEpi              | 10         
     populacaoTCU2019       | 210147125  
     casosAcumulado         | 2          
     casosNovos             | 0          
     obitosAcumulado        | 0          
     obitosNovos            | 0          
     Recuperadosnovos       | null       
     emAcompanhamentoNovos  | null       
     interior/metropolitana | null       
    only showing top 6 rows
    


5. criei a base da dados chamada covid e salvei o dataframe no hdfs particionado pelo municipio


```python
spark.sql("create database covid")
df_to_unix.write.mode('overwrite').partitionBy('municipio').format('csv').saveAsTable('covid.municipio', path='hdfs://namenode:8020/user/hive/warehouse/covid/')
```

6. executei o comando para, no diretório do hdfs, listar os arquivos mostrando os 10 primeiros e os 10 últimos


```python
!hdfs dfs -ls /user/hive/warehouse/covid | head -10
```

    Found 5299 items
    -rw-r--r--   2 root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/_SUCCESS
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abadia de Goiás
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abadia dos Dourados
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abadiânia
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abaetetuba
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abaeté
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abaiara
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abaré
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Abatiá



```python
!hdfs dfs -ls /user/hive/warehouse/covid | tail -10
```

    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Águas de São Pedro
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Águia Branca
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Álvares Florence
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Álvares Machado
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Álvaro de Carvalho
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Áurea
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Ângulo
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Érico Cardoso
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Óbidos
    drwxr-xr-x   - root supergroup          0 2022-08-06 16:42 /user/hive/warehouse/covid/municipio=Óleo


7. criei uma cópia do dataframe para realizar os próximos exercícios


```python
_df = df_to_unix
```

8. visualisei as bases de dados e usando a base covid visualizei as tabelas


```python
spark.sql("show databases").show()
```

    +------------+
    |databaseName|
    +------------+
    |       covid|
    |     default|
    +------------+
    



```python
spark.sql("use covid")
spark.sql("show tables").show()
```

    +--------+---------+-----------+
    |database|tableName|isTemporary|
    +--------+---------+-----------+
    |   covid|municipio|      false|
    +--------+---------+-----------+
    


### Exercício 3 - Criar as 3 visualizações pelo Spark com os dados enviados para o HDFS

#### Visualização 1 - Casos recuperados e Em acompanhamento


```python
# 1.1 Casos recuperados
df_casos_recuperados = spark.sql("select Recuperadosnovos as casos_recuperados from municipio order by 1 desc limit 1")
df_casos_recuperados.show()
```

    +-----------------+
    |casos_recuperados|
    +-----------------+
    |         17262646|
    +-----------------+
    



```python
# 1.2 Em acompanhamento
df_em_acompanhamento = spark.sql("select emAcompanhamentoNovos as em_acompanhamento from municipio order by 1 desc limit 1")
df_em_acompanhamento.show()
```

    +-----------------+
    |em_acompanhamento|
    +-----------------+
    |          1317658|
    +-----------------+
    


#### Visualização 2 - Casos confirmados


```python
# 2.1 Acumulado
df_acumulado = spark.sql("select casosAcumulado as acumulado from municipio order by 1 desc limit 1")
df_acumulado.show()
```

    +---------+
    |acumulado|
    +---------+
    | 18855015|
    +---------+
    



```python
# 2.2 Casos novos
df_casos_novos = spark.sql("select casosNovos as casos_novos from municipio order by 1 desc limit 1")
df_casos_novos.show()
```

    +-----------+
    |casos_novos|
    +-----------+
    |     115228|
    +-----------+
    



```python
# 2.3 Incidência
df_incidencia = spark.sql("select cast(((casosAcumulado*100000)/populacaoTCU2019) as decimal(5,1)) as incidencia from municipio order by 1 desc limit 1")
df_incidencia.show()
```

    +----------+
    |incidencia|
    +----------+
    |    9999.8|
    +----------+
    


#### Visualização 3 - Óbitos confirmados


```python
# 3.1 Óbitos acumulados
df_obitos_acumulados = spark.sql("select obitosAcumulado as obitos_acumulados from municipio order by 1 desc limit 1")
df_obitos_acumulados.show()
```

    +-----------------+
    |obitos_acumulados|
    +-----------------+
    |           526892|
    +-----------------+
    



```python
# 3.2 Casos de óbitos novos 
df_obitos_novos = spark.sql("select obitosNovos as obitos_novos from municipio order by 1 desc limit 1")
df_obitos_novos.show()
```

    +------------+
    |obitos_novos|
    +------------+
    |        4249|
    +------------+
    



```python
# 3.3 Letalidade
df_letalidade = spark.sql("select cast(((obitosAcumulado * 100) / casosAcumulado) as decimal(5,1)) as letalidade from municipio order by 1 desc limit 1")
df_letalidade.show()
```

    +----------+
    |letalidade|
    +----------+
    |    1500.0|
    +----------+
    



```python
# 3.4 Mortalidade
df_mortalidade = spark.sql("select cast(((obitosAcumulado * 100000) / populacaoTCU2019) as decimal(4,1)) as mortalidade from municipio order by 1 desc limit 1")
df_mortalidade.show()
```

    +-----------+
    |mortalidade|
    +-----------+
    |      999.1|
    +-----------+
    


### Exercício 4 - Salvar a primeira visualização como tabela Hive


```python
df_casos_recuperados.write.format('csv').saveAsTable('recuperados')
```


```python
df_em_acompanhamento.write.format('csv').saveAsTable('acompanhamento')
```

1. visualização das tabelas salvas na base de dados covid


```python
spark.sql('show tables').show()
```

    +--------+--------------+-----------+
    |database|     tableName|isTemporary|
    +--------+--------------+-----------+
    |   covid|acompanhamento|      false|
    |   covid|     municipio|      false|
    |   covid|   recuperados|      false|
    +--------+--------------+-----------+
    


### Exercício 5 - Salvar a segunda visualização com formato parquet e compressão snappy


```python
df_acumulado.write.option('compression', 'snappy').parquet('hdfs://namenode/user/paula/spark/projeto_final/exercicios/casos_acumulados')
```


```python
df_casos_novos.write.option('compression', 'snappy').parquet('hdfs://namenode/user/paula/spark/projeto_final/exercicios/casos_novos')
```


```python
df_incidencia.write.option('compression', 'snappy').parquet('hdfs://namenode/user/paula/spark/projeto_final/exercicios/incidencia')
```


```python
!hdfs dfs -ls /user/paula/spark/projeto_final/exercicios
```

    Found 3 items
    drwxr-xr-x   - root supergroup          0 2022-08-06 18:07 /user/paula/spark/projeto_final/exercicios/casos_acumulados
    drwxr-xr-x   - root supergroup          0 2022-08-06 18:08 /user/paula/spark/projeto_final/exercicios/casos_novos
    drwxr-xr-x   - root supergroup          0 2022-08-06 18:09 /user/paula/spark/projeto_final/exercicios/incidencia


### Exercício 6 - Salvar a terceira visualização em um tópico no Kafka


```python
df_obitos_acumulados.selectExpr("to_json(struct(*)) AS value").write.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('topic', 'obitos_acumulados').save()
```


```python
topico_obitos_acumulados = spark.read.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('subscribe','obitos_acumulados').load()

conteudo_obitos_acumulados = topico_obitos_acumulados.select(col('value').cast('string'))
conteudo_obitos_acumulados.show(truncate = False)
```

    +----------------------------+
    |value                       |
    +----------------------------+
    |{"obitos_acumulados":526892}|
    +----------------------------+
    



```python
df_obitos_novos.selectExpr("to_json(struct(*)) AS value").write.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('topic', 'obitos_novos').save()
```


```python
topico_obitos_novos = spark.read.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('subscribe','obitos_novos').load()

conteudo_obitos_novos = topico_obitos_novos.select(col('value').cast('string'))
conteudo_obitos_novos.show(truncate = False)
```

    +---------------------+
    |value                |
    +---------------------+
    |{"obitos_novos":4249}|
    +---------------------+
    



```python
df_letalidade.selectExpr("to_json(struct(*)) AS value").write.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('topic', 'letalidade').save()
```


```python
topico_letalidade = spark.read.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('subscribe','letalidade').load()

conteudo_letalidade = topico_letalidade.select(col('value').cast('string'))
conteudo_letalidade.show(truncate = False)
```

    +---------------------+
    |value                |
    +---------------------+
    |{"letalidade":1500.0}|
    +---------------------+
    



```python
df_mortalidade.selectExpr("to_json(struct(*)) AS value").write.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('topic', 'mortalidade').save()
```


```python
topico_mortalidade = spark.read.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('subscribe','mortalidade').load()

conteudo_mortalidade = topico_mortalidade.select(col('value').cast('string'))
conteudo_mortalidade.show(truncate = False)
```

    +---------------------+
    |value                |
    +---------------------+
    |{"mortalidade":999.1}|
    +---------------------+
    


### Exercício 7 - Criar a visualização pelo Spark com os dados enviados para o HDFS


```python
__df = _df.groupBy(['regiao', 'estado']).agg({'casosAcumulado':'max', 'obitosAcumulado':'max', 'populacaoTCU2019':'max'})
```


```python
__df.show(10)
```

    +--------+------+---------------------+-------------------+--------------------+
    |  regiao|estado|max(populacaoTCU2019)|max(casosAcumulado)|max(obitosAcumulado)|
    +--------+------+---------------------+-------------------+--------------------+
    |   Norte|    TO|              1572866|             200243|                3266|
    |   Norte|    AC|               881935|              85997|                1760|
    |   Norte|    PA|              8602865|             557708|               15624|
    |Nordeste|    MA|              7075181|             322052|                9190|
    |     Sul|    RS|             11377239|            1235914|               31867|
    | Sudeste|    SP|             45919049|            3809222|              130389|
    |Nordeste|    PI|              3273227|             299084|                6662|
    |   Norte|    AP|               845731|             118066|                1857|
    | Sudeste|    MG|             21168791|            1836198|               47148|
    |     Sul|    PR|             11433957|            1308643|               31692|
    +--------+------+---------------------+-------------------+--------------------+
    only showing top 10 rows
    



```python
df_ajuste_nome_dos_campos = __df.withColumnRenamed('max(populacaoTCU2019)','populacao').withColumnRenamed('max(casosAcumulado)', 'casos_acumulados').withColumnRenamed('max(obitosAcumulado)','obitos_acumulados')
```


```python
df_ajuste_nome_dos_campos.show(10)
```

    +--------+------+---------+----------------+-----------------+
    |  regiao|estado|populacao|casos_acumulados|obitos_acumulados|
    +--------+------+---------+----------------+-----------------+
    |   Norte|    TO|  1572866|          200243|             3266|
    |   Norte|    AC|   881935|           85997|             1760|
    |   Norte|    PA|  8602865|          557708|            15624|
    |Nordeste|    MA|  7075181|          322052|             9190|
    |     Sul|    RS| 11377239|         1235914|            31867|
    | Sudeste|    SP| 45919049|         3809222|           130389|
    |Nordeste|    PI|  3273227|          299084|             6662|
    |   Norte|    AP|   845731|          118066|             1857|
    | Sudeste|    MG| 21168791|         1836198|            47148|
    |     Sul|    PR| 11433957|         1308643|            31692|
    +--------+------+---------+----------------+-----------------+
    only showing top 10 rows
    



```python
df_inclusao_de_incidencia_e_mortalidade = (df_ajuste_nome_dos_campos.withColumn('incidencia', round(df_ajuste_nome_dos_campos['casos_acumulados']/df_ajuste_nome_dos_campos['populacao']*100000,1)).withColumn('mortalidade', round(df_ajuste_nome_dos_campos['obitos_acumulados']/df_ajuste_nome_dos_campos['populacao']*100000,1)))
```


```python
df_inclusao_de_incidencia_e_mortalidade.show(10)
```

    +--------+------+---------+----------------+-----------------+----------+-----------+
    |  regiao|estado|populacao|casos_acumulados|obitos_acumulados|incidencia|mortalidade|
    +--------+------+---------+----------------+-----------------+----------+-----------+
    |   Norte|    TO|  1572866|          200243|             3266|   12731.1|      207.6|
    |   Norte|    AC|   881935|           85997|             1760|    9750.9|      199.6|
    |   Norte|    PA|  8602865|          557708|            15624|    6482.8|      181.6|
    |Nordeste|    MA|  7075181|          322052|             9190|    4551.9|      129.9|
    |     Sul|    RS| 11377239|         1235914|            31867|   10863.0|      280.1|
    | Sudeste|    SP| 45919049|         3809222|           130389|    8295.5|      284.0|
    |Nordeste|    PI|  3273227|          299084|             6662|    9137.3|      203.5|
    |   Norte|    AP|   845731|          118066|             1857|   13960.2|      219.6|
    | Sudeste|    MG| 21168791|         1836198|            47148|    8674.1|      222.7|
    |     Sul|    PR| 11433957|         1308643|            31692|   11445.2|      277.2|
    +--------+------+---------+----------------+-----------------+----------+-----------+
    only showing top 10 rows
    


### Exercício 8 - Salvar a visualização do exercício 6 em um tópico no Elastic

Resolução:

1. criei uma cópia do dataframe e salvei no HDFS em formato csv


```python
df_final = df_inclusao_de_incidencia_e_mortalidade
df_final.write.format("csv").save('hdfs://namenode/user/paula/spark/projeto_final/output/covid.csv')
```

2. copiei o arquivo do HDFS para o linux com o comando
```
hdfs dfs -get /user/paula/spark/projeto_final/output/covid.csv /treinamentos/spark/data
```
3. copiei o arquivo salvo no linux para a minha máquina local (windows)
4. acessei o diretório do elasticsearch e inicializei o container com o comando
```
docker-compose start
```
5. após inicializar os containers, acessei o elastic no meu navegador através do localhost:5601
6. importei o csv através do caminho 
```
home > upload files > import csv
```
![import_data](https://github.com/pctmoraes/semantix-big-data-engineer-projeto-final/blob/main/elastic_import.jpg)

### Exercício 9 - Criar um dashboard no Elastic para visualização dos novos dados enviados

<br>

Resolução:

1. acessei o menu "Visualize" do Kibana e fiz alguns testes de visualizações, no entanto, encontrei dificuldades em criar visualizações interessantes, abaixo estão algumas que montei a partir dos testes:  

<br>

![modo_discover](https://github.com/pctmoraes/semantix-big-data-engineer-projeto-final/blob/main/elastic_discover.jpg)  
![modo_tabela](https://github.com/pctmoraes/semantix-big-data-engineer-projeto-final/blob/main/elastic_vis_tabela.jpg)  
![modo_barra](https://github.com/pctmoraes/semantix-big-data-engineer-projeto-final/blob/main/elastic_vis_barra.jpg)  