/* Drop Tables

--========================================================================================================================--
-- Description:
--========================================================================================================================--

--========================================================================================================================--
--				!!!!!	CAUTION THIS WILL DROP ALL TABLE FOR THE USER DEFAUL SCHEMA ITS RUN UNDER		!!!!!
--========================================================================================================================--

Drops all tables under the default schema of the user executing it.

--========================================================================================================================--
-- Change comments:
--========================================================================================================================--

24/11/2023	BClothier	Create Rule
26/04/2024	BClothier	Change schema to miyademodw
25/06/2024	BClothier	Changed incorrect schema name as spotted by Manoj from  'miyademodw' to 'miyademowh'
16/07/2024	PNasti		Changed VARCHAR(5) to VARCHAR(10) for @SCHEMA
27/01/2025	BClothier	Added [] around table names
27/01/2025	BClothier	Set @SCHEMA to read the default schema of the user this is executed as
04/02/2025	BClothier	Set dropping of external table not to rely on OBJECT_ID ( its not compatible with external tables )
12/02/2025	BClothier	Changed VARCHAR(10) to VARCHAR(50) for @SCHEMA
23/06/2025	BClothier	Changed from slower loop to faster set based execution
*/

--========================================================================================================================--
--				!!!!!	CAUTION THIS WILL DROP ALL TABLE FOR THE USER DEFAUL SCHEMA ITS RUN UNDER		!!!!!
--========================================================================================================================--

--Declare variables
DECLARE @SQLSTATEMENT VARCHAR(MAX)
DECLARE @SCHEMA VARCHAR(50) = schema_name()
DECLARE @SCHEMA_ID INT =	(
							SELECT SCHEMA_ID 
							FROM SYS.SCHEMAS 
							WHERE NAME = @SCHEMA
							)

SET @SQLSTATEMENT = (
					-- combine all statements casting explicitly to NVARCHAR(MAX) to avoid 8000-byte limit in STRING_AGG
					SELECT STRING_AGG	(
										CAST(x.sqlDropStatement AS NVARCHAR(MAX)), CHAR(13) + CHAR(10))
					FROM
						(
							-- build the drop commands for all tables
							SELECT
								sqlDropStatement =	CASE
														WHEN ext.object_id IS NULL
														THEN 'DROP TABLE '+ @SCHEMA +'.' + o.[name] + '; PRINT ''Dropped table: '+@SCHEMA+'.'+ o.[name] +''';'
														ELSE 'DROP EXTERNAL TABLE '+ @SCHEMA +'.' + o.[name] + '; PRINT ''Dropped table: '+@SCHEMA+'.'+ o.[name] +''';'
													END
							FROM
								sys.objects o
								JOIN sys.schemas s ON o.schema_id = s.schema_id
								LEFT JOIN sys.external_tables ext ON o.object_id = ext.object_id
							WHERE 1=1
								AND o.[type] = 'U'
								AND o.Schema_id = @SCHEMA_ID
								AND @SCHEMA + '.' + o.[name] NOT IN (
										'kraken.IntExt_DateFinancialYear_18990406_01012099',
										'kraken.IntExt_CustomWards',
										'kraken.IntExt_Postcode_Map_2011',
										'kraken.IntExt_LSOA_Deprivation',
										'kraken.ExecutionLog',
										'kraken.CohortCountLog',
										'kraken.IntExt_DimNHSNationalCode'
									)
						) AS x
					)

--execute the command (change EXEC to PRINT to test running this rule without execution anything)
EXEC (@SQLStatement);