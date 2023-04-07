CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.tbl_divisao ( 
division string,
division_name string
)
COMMENT 'tbl_divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1");