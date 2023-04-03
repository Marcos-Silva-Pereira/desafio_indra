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