CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_REGIAO ( 
region_code string,
region_name string
)
COMMENT 'TBL_REGIAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");