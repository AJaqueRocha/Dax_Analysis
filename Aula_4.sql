--
-- Criação da tabela de transação
--

create table tbVendas_Final (
CodCliente int,
Categoria varchar(50),
Subcategoria varchar(50),
Produto varchar(50),
Ano int,
Mes int,
Cidade varchar(50),
Valor float,
Volume float)

select * from tbVendas_Final

--
-- Carga de dados massiva via BULK INSERT (carrega os dados na tabela de forma otimizada e eficiente
--
-- Para apagar qualquer dado que existir na tabela criada
truncate table tbVendas_Final

BULK INSERT tbVendas_Final
	FROM 'C:\Users\jaque\OneDrive\Documents\EBAC\Imersao Dados\Imersao Dados\outPut\Aula 3\Dados para SQL\vendas_export.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a' -- delimitador de linha do Databricks
	)




create table tbPotencial_Final (
CodCliente int,
Ano int,
Area_Comercial float,
Area_Hibrida float,
Area_Residencial float,
Area_Industrial float,
ValorPotencial float
)


truncate table tbPotencial_Final

BULK INSERT tbPotencial_Final
	FROM 'C:\Users\jaque\OneDrive\Documents\EBAC\Imersao Dados\Imersao Dados\outPut\Aula 3\Dados para SQL\potencial_export.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a' -- delimitador de linha do Databricks
	)

-- Síntese de criação de index
CREATE INDEX index1 ON tbPotencial_Final (CodCliente);
CREATE INDEX index1 ON tbVendas_Final (CodCliente);

select * from tbPotencial_Final