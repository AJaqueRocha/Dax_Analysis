-- Databricks notebook source
select * from tbvendas

-- COMMAND ----------

select * from tbpotencial

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #Listar diretórios no DBFS (File System) ls é listar
-- MAGIC 
-- MAGIC dbutils.fs.ls("/FileStore/tables/dados_ebac")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #Criar Dataframe da tabela de vendas
-- MAGIC 
-- MAGIC df_vendas = (spark.read
-- MAGIC     .format("csv")
-- MAGIC     .option("header", "true")
-- MAGIC     .option("inferSchema", "true")
-- MAGIC     .load("/FileStore/tables/dados_ebac/Vendas.csv")
-- MAGIC )
-- MAGIC 
-- MAGIC display(df_vendas)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #Criar Dataframe da tabela de potencial
-- MAGIC 
-- MAGIC df_potencial = (spark.read
-- MAGIC     .format("csv")
-- MAGIC     .option("header", "true")
-- MAGIC     .option("inferSchema", "true")
-- MAGIC     .load("/FileStore/tables/dados_ebac/Vendas_Potencial.csv")
-- MAGIC )
-- MAGIC 
-- MAGIC display(df_potencial)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df_vendas.show()

-- COMMAND ----------

select count(*) from tbvendas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(df_vendas.count())

-- COMMAND ----------

select categoria, count(1) from tbvendas group by categoria

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(df_vendas.groupby("categoria").count())

-- COMMAND ----------

-- Precisamos apagar os arquivos que aparecerem como resultado, pois a partir do clone, eles ficaram em duplicidade e, por isso, há erro na criação das tabelas devido à reativação do cluster

%python

dbutils.fs.ls("/user/hive/warehouse")


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Se houver na criação das tabelas devido à reativação do cluster, executar este comando
-- MAGIC 
-- MAGIC dbutils.fs.rm("/user/hive/warehouse/tbvendas_final", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Após, tentar criar a tabela outra vez no delta lake
-- MAGIC 
-- MAGIC dbutils.fs.rm("/user/hive/warehouse/tbpotencial_final", recurse=True)

-- COMMAND ----------

-- Criar tabelas finais no delta lake
-- Delta Lake é uma camada de armazenamento que oferece confiabilidade, segurança e desempenho do seu data lake
-- Se houver erro neste comando depois de realizar o clone do cluster, execute os comandos acima para limpar o DBFS

create table tbvendas_final (Client_ID int, Categoria string, Subcategoria string, Produto string, Ano int, Mes int, Cidade string, Valor double, Volume double);

Create table tbpotencial_final (Client_ID int, Ano int, Area_Comercial double, Area_Hibrida double, Area_Residencial double, Area_Industrial double, ValorPotencial double);

-- COMMAND ----------

select * from tbvendas_final

-- COMMAND ----------

-- Inserindo dados na tabela de vendas no Delta Lake

insert into tbvendas_final (Client_ID, Categoria, Subcategoria, Produto, Ano, Mes, Cidade, Valor, Volume)
select replace(Client_ID, 'Client #', '') , Categoria,Subcategoria,Produto,Year,Month,Cidade,Valor,Volume from tbvendas where year in ('2020', '2021', '2022')

-- COMMAND ----------

-- Para verificar o erro:

select distinct year from tbvendas

-- Verificamos que o ano 2021-B não é int, conforme cmd 13. Por isso, colocarmos o where para determinar

-- COMMAND ----------

select * from tbpotencial_final

-- COMMAND ----------

-- Inserindo dados na tabela final Potencial

insert into tbpotencial_final (Client_ID, Ano, Area_Comercial, Area_Hibrida, Area_Residencial, Area_Industrial, ValorPotencial)
select replace(Client_ID, 'Client #', ''), Year, Area_Comercial, Area_Hibrida, Area_Residencial, Area_Industrial, BRL_Potencial from tbpotencial

-- COMMAND ----------

--- ### Consistência ### - SQL
-- Verificando se todos os campos estão preenchidos

Select *
from tbvendas_final
where isnull('Client_ID') = true
or isnull('Categoria') = true
or isnull('Subcategoria') = true
or isnull('Produto') = true
or isnull('Ano') = true
or isnull('Mes') = true
or isnull('Cidade') = true
or isnull('Valor') = true
or isnull('Volume') = true

-- COMMAND ----------

-- Apaga os registros nulos

delete from tbvendas_final
where isnull('Client_ID') = true
or isnull('Categoria') = true
or isnull('Subcategoria') = true
or isnull('Produto') = true
or isnull('Ano') = true
or isnull('Mes') = true
or isnull('Cidade') = true
or isnull('Valor') = true
or isnull('Volume') = true

-- COMMAND ----------

Select * 
from tbpotencial_final
where isnull('Client_ID') = true
or isnull('Ano') = true
or isnull('Area_Comercial') = true
or isnull('Area_Hibrida') = true
or isnull('Area_Residencial') = true
or isnull('Area_Industrial') = true
or isnull('ValorPotencial') = true

-- COMMAND ----------

-- Apaga os registros nulos

delete from tbpotencial_final
where isnull('Client_ID') = true
or isnull('Ano') = true
or isnull('Area_Comercial') = true
or isnull('Area_Hibrida') = true
or isnull('Area_Residencial') = true
or isnull('Area_Industrial') = true
or isnull('ValorPotencial') = true

-- COMMAND ----------

-- Checagem de negócio (via doc Case)
-- Indica as subcategorias do produto. São 10 ao todo. Cada subcategoria pertence apenas a 1 categoria.

select distinct subcategoria from tbvendas_final order by 1;

-- COMMAND ----------

-- Eliminar Sub-Categoria 99

delete from tbvendas_final where subcategoria = 'Sub-Categoria 99'

-- COMMAND ----------

-- Checar:cCada subcategoria pertence apenas a 1 categoria.

select count(distinct Categoria) as Qtdade, subcategoria from tbvendas_final group by subcategoria having count(distinct Categoria) > 1

-- COMMAND ----------

-- Verifica a distribuição das categorias com mais de uma subcategoria

select distinct Categoria, Subcategoria from tbvendas_final where Subcategoria in ('Sub-Categoria 8', 'Sub-Categoria 7') order by 2

-- COMMAND ----------

-- Verifica a distribuição das categorias com mais de uma subcategoria fazendo contagem

select distinct Categoria, Subcategoria, count(1) from tbvendas_final where Categoria in ('XTZ250', 'XT660', 'CB750') group by Categoria, Subcategoria order by 1

-- COMMAND ----------

-- Poderia desconsiderar os registros XT660 e Subcategoria 8 com apenas 22 registros
-- e desconsiderar os registros XTZ250 e Subcategoria 7 com apenas 8 registros

delete from tbvendas_final where categoria = 'XT660' and subcategoria = 'Sub-Categoria 8';
delete from tbvendas_final where categoria = 'XTZ250' and subcategoria = 'Sub-Categoria 7';

-- COMMAND ----------

select * from tbvendas_final

-- COMMAND ----------

select * from tbpotencial_final

-- COMMAND ----------


