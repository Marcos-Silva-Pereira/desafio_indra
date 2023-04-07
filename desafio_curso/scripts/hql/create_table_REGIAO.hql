CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.tbl_regiao ( 
region_code string,
region_name string
)
COMMENT 'tbl_regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");