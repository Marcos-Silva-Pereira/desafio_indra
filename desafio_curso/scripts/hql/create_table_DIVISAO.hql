CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_DIVISAO ( 
division string,
division_name string
)
COMMENT 'TBL_DIVISAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1");