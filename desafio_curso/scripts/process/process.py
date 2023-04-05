from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES")
df_divisao = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_DIVISAO")
df_endereco = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_ENDERECO")
df_regiao = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_REGIAO")
df_vendas = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_VENDAS")

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