-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC # Para saber o total de clientes, utilizarei a síntese do python sqlContext.sql dentro do SQL
-- MAGIC 
-- MAGIC sqlContext.sql("select Count(distinct client_id) as Qdade from tbvendas_final ").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Para saber o total vendido
-- MAGIC # Usarei o cmd cast(sum(valor) as decimal (18,2) para converter o valor para duas casas decimais após a vírgula
-- MAGIC 
-- MAGIC sqlContext.sql("select cast(sum(valor) as decimal (18,2)) as ValorTotal from tbvendas_final ").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Arquivo de vendas
-- MAGIC 
-- MAGIC sqlContext.sql("select count(distinct cidade) as Cidade, count(distinct categoria) as Categoria, count(distinct Subcategoria) as Subcategoria, count(distinct Produto) as Produto from tbvendas_final ").show()

-- COMMAND ----------

-- Arquivo potencial
-- executar apenas em uma query

select Count(distinct client_id) as Qtdade, sum(ValorPotencial) as Valor, sum(area_comercial) as Comercial, sum(area_hibrida) as hibrida, sum(area_residencial) as residencial, sum(area_industrial) as industrial from tbpotencial_final

-- COMMAND ----------

-- Período

select distinct ano, mes from tbvendas_final order by 1, 2

-- COMMAND ----------

-- Para extrair os dados do Databricks para serem carregados no SQL

select Client_ID, Categoria, Subcategoria, Produto, Ano, Mes, Cidade, cast(Valor as decimal(18,7)) as Valor, Volume from tbvendas_final

-- COMMAND ----------

select Client_ID, Ano,
  cast(Area_Comercial as decimal(18,2)) as Area_Comercial,
  cast(Area_Hibrida as decimal(18,2)) as Area_Hibrida,
  cast(Area_Residencial as decimal(18,2)) as Area_Residencial,
  cast(Area_Industrial as decimal(18,2)) as Area_Industrial,
  cast(ValorPotencial as decimal(18,7)) as ValorPotencial
from tbpotencial_final

-- COMMAND ----------


