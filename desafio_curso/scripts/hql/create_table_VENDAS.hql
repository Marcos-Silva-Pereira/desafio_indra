CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.tbl_vendas ( 
actual_delivery_date string,
customer_Key string,
date_Key string,
discount_amount string,
invoice_date string,
invoice_number string,
item_class string,
item_number string,
item string,
line_number string,
list_price string,
order_number string,
promised_delivery_date string,
sales_amount string,
sales_amount_based_on_list_price string,
sales_cost_amount string,
sales_margin_amount string,
sales_price string,
sales_quantity string,
sales_rep string,
u_m string
)
COMMENT 'tbl_vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");