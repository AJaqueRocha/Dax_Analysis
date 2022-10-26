-- Vamos criaar uma view que ser� utilizada dentro do Power BI
-- Tamb�m iremos carregar nossa tabela de vendas no Power BI
-- Nome da view = vw_Potencial
-- O cmd left join significa que estamo pegando todos os registros da tbPotencial_Final e apenas as informa��es dos clientes que converteram vendas dentro do ano

CREATE view [vw_Potencial]

as

select a.codcliente, a.Ano, a.Area_Comercial, a.Area_Hibrida, a.Area_Residencial, a.Area_Industrial, ValorPotencial,
	   sum(valor) as ValorVendas
from tbPotencial_Final a
left join tbVendas_Final b
on   a.codcliente = b.codcliente
and  a.ano = b.ano
group by a.codcliente, a.Ano, a.Area_Comercial, a.Area_Hibrida, a.Area_Residencial, a.Area_Industrial, ValorPotencial
GO