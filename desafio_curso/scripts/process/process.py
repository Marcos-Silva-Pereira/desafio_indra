from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.functions import asc,desc
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("select * from desafio_curso.tbl_clientes")
df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")
df_endereco = spark.sql("select * from desafio_curso.tbl_endereco")
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")
df_vendas = spark.sql("select * from desafio_curso.tbl_vendas")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional
# Tratando df_endereco
# retirando a primeira linha, pois repete o nome das colunas
df_endereco = df_endereco.where(df_endereco.city != 'City')

# mudando o tipo das colunas
li = ["address_number", "zip_code"]
for x in li:
    df_endereco = df_endereco.withColumn(x, df_endereco[x].cast(IntegerType()))

# substituindo valores nulos e vazios por 0, e strings vazias por 'Não informado'
for z in df_endereco.dtypes:
    if re.search("int", z[1]) :
        df_endereco = df_endereco.withColumn(z[0], when(df_endereco[z[0]].isNull(), 0)\
                                            .otherwise(df_endereco[z[0]]))
    if re.search("double", z[1]):
        df_endereco = df_endereco.withColumn(z[0], when(df_endereco[z[0]].isNull(), 0.0)\
                                            .otherwise(df_endereco[z[0]]))
    else:
        df_endereco = df_endereco.withColumn(z[0], trim(df_endereco[z[0]]))
        df_endereco = df_endereco.withColumn(z[0], when(df_endereco[z[0]] == '', 'Não informado')\
                                            .when(df_endereco[z[0]].isNull(), 'Não informado')  
                                            .otherwise(df_endereco[z[0]]))

# Tratando df_divisao
# retirando a primeira linha, pois repete o nome das colunas
df_divisao = df_divisao.where(df_divisao.division != 'Division')

# mudando o tipo das colunas
li = ["division"]
for x in li:
    df_divisao = df_divisao.withColumn(x, df_divisao[x].cast(IntegerType()))

# substituindo valores nulos e vazios por 0, e strings vazias por 'Não informado'
for z in df_divisao.dtypes:
    if re.search("int", z[1]) :
        df_divisao = df_divisao.withColumn(z[0], when(df_divisao[z[0]].isNull(), 0)\
                                            .otherwise(df_divisao[z[0]]))
    if re.search("double", z[1]):
        df_divisao = df_divisao.withColumn(z[0], when(df_divisao[z[0]].isNull(), 0.0)\
                                            .otherwise(df_divisao[z[0]]))
    else:
        df_divisao = df_divisao.withColumn(z[0], trim(df_divisao[z[0]]))
        df_divisao = df_divisao.withColumn(z[0], when(df_divisao[z[0]] == '', 'Não informado')\
                                            .when(df_divisao[z[0]].isNull(), 'Não informado')  
                                            .otherwise(df_divisao[z[0]]))

# Tratando df_clientes
# retirando a primeira linha, pois repete o nome das colunas
df_clientes = df_clientes.where(df_clientes.address_number != 'Address Number')

# mudando o tipo das colunas
li = ["address_number", "business_unit", "customer_key", "division", "region_code"]
for x in li:
    df_clientes = df_clientes.withColumn(x, df_clientes[x].cast(IntegerType()))

# substituindo valores nulos e vazios por 0, e strings vazias por 'Não informado'
for z in df_clientes.dtypes:
    if re.search("int", z[1]) :
        df_clientes = df_clientes.withColumn(z[0], when(df_clientes[z[0]].isNull(), 0)\
                                            .otherwise(df_clientes[z[0]]))
    if re.search("double", z[1]):
        df_clientes = df_clientes.withColumn(z[0], when(df_clientes[z[0]].isNull(), 0.0)\
                                            .otherwise(df_clientes[z[0]]))
    else:
        df_clientes = df_clientes.withColumn(z[0], trim(df_clientes[z[0]]))
        df_clientes = df_clientes.withColumn(z[0], when(df_clientes[z[0]] == '', 'Não informado')\
                                            .when(df_clientes[z[0]].isNull(), 'Não informado')  
                                            .otherwise(df_clientes[z[0]]))

# Tratando df_regiao
# retirando a primeira linha, pois repete o nome das colunas
df_regiao = df_regiao.where(df_regiao.region_code != 'Region Code')

# mudando o tipo das colunas
li = ["region_code"]
for x in li:
    df_regiao = df_regiao.withColumn(x, df_regiao[x].cast(IntegerType()))

# substituindo valores nulos e vazios por 0, e strings vazias por 'Não informado'
for z in df_regiao.dtypes:
    if re.search("int", z[1]) :
        df_regiao = df_regiao.withColumn(z[0], when(df_regiao[z[0]].isNull(), 0)\
                                            .otherwise(df_regiao[z[0]]))
    if re.search("double", z[1]):
        df_regiao = df_regiao.withColumn(z[0], when(df_regiao[z[0]].isNull(), 0.0)\
                                            .otherwise(df_regiao[z[0]]))
    else:
        df_regiao = df_regiao.withColumn(z[0], trim(df_regiao[z[0]]))
        df_regiao = df_regiao.withColumn(z[0], when(df_regiao[z[0]] == '', 'Não informado')\
                                            .when(df_regiao[z[0]].isNull(), 'Não informado')  
                                            .otherwise(df_regiao[z[0]]))

# Tratando df_vendas
# retirando a primeira linha, pois repete o nome das colunas
df_vendas = df_vendas.where(df_vendas.actual_delivery_date != 'Actual Delivery Date')

# mudando o tipo das colunas
# inteiros
li = ["customer_key", "invoice_number", "item_number", "line_number", "order_number", "sales_quantity", "sales_rep"]
for x in li:
    df_vendas = df_vendas.withColumn(x, df_vendas[x].cast(IntegerType()))
    
# double
lf = ["discount_amount", "list_price", "sales_amount", "sales_amount_based_on_list_price", "sales_cost_amount", "sales_margin_amount", "sales_price"]
for y in lf:
    df_vendas = df_vendas.withColumn(y, df_vendas[y].cast(DoubleType()))

# substituindo valores nulos e vazios por 0, e strings vazias por 'Não informado'
for z in df_vendas.dtypes:
    if re.search("int", z[1]) :
        df_vendas = df_vendas.withColumn(z[0], when(df_vendas[z[0]].isNull(), 0)\
                                            .otherwise(df_vendas[z[0]]))
    if re.search("double", z[1]):
        df_vendas = df_vendas.withColumn(z[0], when(df_vendas[z[0]].isNull(), 0.0)\
                                            .otherwise(df_vendas[z[0]]))
    else:
        df_vendas = df_vendas.withColumn(z[0], trim(df_vendas[z[0]]))
        df_vendas = df_vendas.withColumn(z[0], when(df_vendas[z[0]] == '', 'Não informado')\
                                            .when(df_vendas[z[0]].isNull(), 'Não informado')  
                                            .otherwise(df_vendas[z[0]]))


# Juntando a tabela cliente e vendas, selecionando colunas especificas
c = df_clientes.select(df_clientes.customer_key, df_clientes.division, df_clientes.address_number, df_clientes.region_code, df_clientes.customer, df_clientes.phone)
v = df_vendas.select(df_vendas.customer_key, df_vendas.sales_price, df_vendas.item, df_vendas.sales_amount, df_vendas.sales_quantity, df_vendas.invoice_date)

df1 = c.join(v, c.customer_key == v.customer_key, "inner")
df1 = df1.drop(v.customer_key)

# Criando as colunas de dia, mes e ano, baseado na data
df1 = df1.select('*', substring('invoice_date', 1,2).alias('day'), substring('invoice_date', 4,2).alias('month'), substring('invoice_date', 7,4).alias('year'))

df1 = df1.withColumn('trimester',
                when((col("month") == "01") | (col("month") == "02") | (col("month") == "03"), "1 trimester")
                .when((col("month") == "04") | (col("month") == "05") | (col("month") == "06"), "2 trimester")
                .when((col("month") == "07") | (col("month") == "08") | (col("month") == "09"), "3 trimester")
                .otherwise("4 trimester"))

# unindo com a tabela endereco
e = df_endereco.select(df_endereco.address_number, df_endereco.country, df_endereco.state, df_endereco.city)

df2 = df1.join(e, df1.address_number == e.address_number, "left")
df2 = df2.drop(e.address_number)

# unindo com a tabela divisao
df3 = df2.join(df_divisao, df2.division == df_divisao.division, "left")
df3 = df3.drop(df_divisao.division)

# unindo com a tabela regiao
df4 = df3.join(df_regiao, df3.region_code == df_regiao.region_code, "left")
df4 = df4.drop(df_regiao.region_code)

# Tratar a stage
df4 = df4.drop('customer_key', 'division', 'address_number', 'region_code')

# criando o fato
ft_vendas = []

#criando as dimensões
dim_clientes = []
dim_tempo = []
dim_localidade = []

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_indra/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_indra/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')