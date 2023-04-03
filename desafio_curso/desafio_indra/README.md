<<<<<<< HEAD DESAFIO BIG DATA/BI

📌 ESCOPO DO DESAFIO Neste desafio serão feitas as ingestões dos dados que estão na pasta /raw onde vamos ter alguns arquivos .csv de um banco relacional de vendas.

VENDAS.CSV
CLIENTES.CSV
ENDERECO.CSV
REGIAO.CSV
DIVISAO.CSV
Seu trabalho como engenheiro de dados/arquiteto de BI é prover dados em uma pasta desafio_curso/gold em .csv para ser consumido por um relatório em PowerBI que deverá ser construído dentro da pasta 'app' (já tem o template).

📑 ETAPAS Etapa 1 - Enviar os arquivos para o HDFS - nesta etapa lembre de criar um shell script para fazer o trabalho repetitivo (não é obrigatório)

Etapa 2 - Criar o banco DEASFIO_CURSO e dentro tabelas no Hive usando o HQL e executando um script shell dentro do hive server na pasta scripts/pre_process.

- DESAFIO_CURSO (nome do banco)
    - TBL_VENDAS
    - TBL_CLIENTES
    - TBL_ENDERECO
    - TBL_REGIAO
    - TBL_DIVISAO
Etapa 3 - Processar os dados no Spark Efetuando suas devidas transformações criando os arquivos com a modelagem de BI. OBS. o desenvolvimento pode ser feito no jupyter porem no final o codigo deve estar no arquivo desafio_curso/scripts/process/process.py

Etapa 4 - Gravar as informações em tabelas dimensionais em formato cvs delimitado por ';'.

    - FT_VENDAS
    - DIM_CLIENTES
    - DIM_TEMPO
    - DIM_LOCALIDADE
Etapa 5 - Exportar os dados para a pasta desafio_curso/gold

Etapa 6 - Criar e editar o PowerBI com os dados que você trabalhou.

No PowerBI criar gráficos de vendas. Etapa 7 - Criar uma documentação com os testes e etapas do projeto.

REGRAS Campos strings vazios deverão ser preenchidos com 'Não informado'. Campos decimais ou inteiros nulos ou vazios, deversão ser preenchidos por 0. Atentem-se a modelagem de dados da tabela FATO e Dimensão. Na tabela FATO, pelo menos a métrica valor de venda é um requisito obrigatório. Nas dimensões deverá conter valores únicos, não deverá conter valores repetidos.

INSTRUÇÕES vocês deveram me entregar o projeto no github e por email (zip)

nome do email: DESAFIO MINSAIT BI/BIGDATA (Aluno) dentro deste email o zip e o link para o github onde estará o projeto. prazo: ate 08/04/2023 nesse caso não poderei aceitar atrasos na entrega.