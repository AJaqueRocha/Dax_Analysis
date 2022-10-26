
-- Oportunidades de crescimento para clientes
--Dei um apelido para a tabela. No caso, "a"
-- Utilizo into para jogar os dados para uma tabela temporária #temp1 para depois, plotar no slide
select     a.Ano,
           a.CodCliente,
		   sum(distinct b.valorpotencial) as ValorPotencial,
		   sum(a.valor) as ValorVendas 
into	   #temp1
from       tbVendas_Final a                                  
inner join tbPotencial_Final b
On         a.CodCliente = b.CodCliente
and        a.Ano = b.Ano
group by   a.Ano, a.CodCliente

select * from #temp1



-- Formatar a tabela para melhor visualização
select Ano,
	   format(sum(ValorPotencial), '###,##0.00','pt-br') as ValorPotencial,
	   format(sum(ValorVendas), '###,##0.00','pt-br') as ValorVendas,
	   format(sum(ValorPotencial) - sum(ValorVendas), '###,##0.00','pt-br') as Oportunidade,
	   abs(((sum(ValorVendas)/sum(ValorPotencial))*100)-100) [Oportunidade%]
from     #temp1
group by Ano
order by Ano


-- Para preencher a tabela em relação à oportunidade e ao que foi alcançado
-- O cmd abs deixa os dados como número absoluto, sem o sinal de negativo
-- O cmd round é para determinar as casas decimais, no caso, será 1 casa decimal
select   Ano,
	     round(abs(((sum(ValorVendas)/sum(ValorPotencial))*100)-100),1) as [Oportunidade%],
	     round(abs(abs(((sum(ValorVendas)/sum(ValorPotencial))*100)-100)-100),1) as [Alcançado%]
from     #temp1
group by Ano
order by Ano



-- Entendendo a perda de clientes
-- Slide 3
-- Cmd pivot passa os dados de linhas para colunas
-- Como temos os dados de 2022 apens até o mês de agosto, vamos comparar os dados dos outros meses até agosto também
select *
from   (
	   select Ano,
			  Categoria,
			  Valor
	   from   tbvendas_Final
	   where mes <= 8
	   ) t
pivot (sum(Valor) for Categoria in ([X], [XTZ250], [XT660],[CB750])) p
order by ano


-- Quantidade de clientes
select        Ano,
			  count (distinct codcliente) as [#Clientes]
from   tbvendas_Final
where mes <= 8
group by Ano
order by ano


-- Conquistar novos clientes
-- Slide 4
-- Iremos trabalhar com dados de clientes que têm potencial de vendas porém, não realizaram nenhuma compra no ano. Quero entender porquê não vendi nada para ele
-- Quero asber do ValorVendasPotencial, quanto ele representa percentualmente
-- Para isso, iremos jogas essas informações em uma nova tabela temporária #tmp_nc
-- O cmd where not exists se refere aos clientes que não transacionaram no ano
-- Drop table #tmp_nc é para apagar a tabela temporária
Drop table #tmp_nc

select   Ano,
         sum(Area_Comercial) as Area_Comercial,
	     sum(Area_Hibrida) as Area_Hibrida,
	     sum(Area_Residencial) as Area_Residencial,
	     sum(Area_Industrial) as Area_Industrial,

	     sum(Area_Comercial)+
	     sum(Area_Hibrida)+
	     sum(Area_Residencial)+
	     sum(Area_Industrial) as Area_Total,

	     sum(ValorPotencial) as ValorPotencial
into     #tmp_nc
from     tbPotencial_final a
where    not exists (select 1
         from tbVendas_Final b
	     where a.CodCliente = b.CodCliente
	     and   a.Ano = b.Ano)
group by Ano


-- Agora precisamos transformar em percentual os dados que estão na "tmp_nc
select * from #tmp_nc order by ano

select Ano,
	Area_Comercial/Area_Total* 100 as [Area_Comercial%],
	Area_Hibrida/Area_Total* 100 as [Area_Hibrida%],
	Area_Residencial/Area_Total* 100 as [Area_Residencial%],
	Area_Industrial/Area_Total* 100 as [Area_Industrial%],
	ValorPotencial
from #tmp_nc order by ano


-- Total de clientes com potencial
-- O cmd wuth rollup acrescenta mais uma linha à tabela com o total de clientes com potencial
select    a.ano, count(distinct CodCliente) as Qdade
from	  tbpotencial_final a
where     not exists (select 1
          from tbVendas_Final b
	      where a.CodCliente = b.CodCliente
	      and   a.Ano = b.Ano)
group by  a.ano
with rollup



--Análise por cidade
-- Slide 5
-- Ranking
-- Ordenando pelo campo 2 (valor) de maneira decrescente: order by  2 desc
select *
from      (
			select    top 10
			Cidade,
			sum(valor) as Valor
			from      tbvendas_final
			group by  Cidade
			order by  2 desc
) x order by 2


-- Para encontrar o valor total vendido das top 10
-- Primeiro, crio uma tabela temporária a partir do cmd into #tmp_cidade
select *
into #tmp_cidade
from      (
			select    top 10
			Cidade,
			sum(valor) as Valor
			from      tbvendas_final
			group by  Cidade
			order by  2 desc
) x order by 2


-- Total das top 10 cidades
select sum(valor) from #tmp_cidade


--Para formatar a tabela
select format(sum(valor), '###,##0.00','pt-br') from #tmp_cidade


-- Percentual das top 10 cidades
-- Declaramos uma variável do tipo float: @total_top10
-- Pego o valor total das cidades e jogo para a variável (segunda linha)
-- Depois divido pelo valor total de vendas
Declare @total_top10 as float
select  @total_top10 = sum(valor) from #tmp_cidade
select round(@total_top10/sum(valor)*100,2) as Perc from tbvendas_final


-- Qdade total de clientes que temos nas top 10
select count(distinct codcliente) from tbvendas_final where cidade in (select cidade from #tmp_cidade)

-- Qdade de transações
select count(codcliente) from tbvendas_final where cidade in (select cidade from #tmp_cidade)

-- Total de transações
select count(codcliente) from tbvendas_final


-- Entender como posso aumentar as vendas
-- Identificar clientes que vendo apenas 1 produto para tentar incluir um produto novo
-- Slide 6
drop table #tmp_produto1

with Clientes as
	 (
		select CodCliente,
			   produto
		from   tbVendas_Final
		group by produto, CodCliente
	 )
	 select *
	 into   #tmp_produto1
	 from   Clientes
-- Gráfico de rosca
	 select Categoria, sum(Valor) as Valor
	 from (select codcliente,
				 count(codcliente) as Qdade
		  from   #tmp_produto1
		  group by codcliente
		  having count(codcliente) = 1 ) as X
	      inner join tbvendas_final b
		  on X.CodCliente = b.CodCliente
		  group by Categoria
		  order by 1


-- Qdade de clientes que compraram apenas 1 produto e verifique quantas linhas tem no rodapé do programa
select CodCliente,
	   count(CodCliente) as Qdade
	from #tmp_produto1
	group by CodCliente
	having count(CodCliente) = 1 


-- Para verificar o valor transacionado (vendido)
--ADICIONEI O FORMAT PARA SAIR FORMATADO
select format(sum(Valor), '###,##0.00','pt-br') as Valor
	 from (select codcliente,
				 count(codcliente) as Qdade
		  from   #tmp_produto1
		  group by codcliente
		  having count(codcliente) = 1 ) as X
	      inner join tbvendas_final b
		  on X.CodCliente = b.CodCliente


-- Total de produtos (verificar a quantidade de linhas no rodapé do programa)
select distinct Produto
	from (select CodCliente,
			count(CodCliente) as Qdade
	from #tmp_produto1
	group by CodCliente
	having count(CodCliente) = 1) as X
inner join tbvendas_final b
on x.CodCliente = b.CodCliente
	
	
