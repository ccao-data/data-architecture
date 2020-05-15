-- SELECT TOP 10 [centroid_x],[centroid_y]  FROM [dbo].[PINLOCATIONS]
-- // Preview conversion
SELECT top 10 [centroid_x], CAST([centroid_x] as float) as [centroid_xF]
,[centroid_y], CAST([centroid_y] as float) as [centroid_yD]
	FROM [dbo].[PINLOCATIONS]
-- // Add columns
ALTER TABLE [PINLOCATIONS] ADD [centroidX] float
ALTER TABLE [PINLOCATIONS] ADD [centroidY] float

UPDATE [PINLOCATIONS] SET [centroidX] = CAST([centroid_x] as float), [centroidY] = CAST([centroid_y] as float) -- 1,429,424

SELECT top 10 [centroid_x],[centroidX],[centroid_y], [centroidY] FROM [dbo].[PINLOCATIONS]

--drop index [IX_PL_MISC]
-- drop original columns
ALTER TABLE [PINLOCATIONS] DROP COLUMN [centroid_x]
ALTER TABLE [PINLOCATIONS] DROP COLUMN [centroid_y]
-- rename new columns
EXEC sp_rename '[PINLOCATIONS].[centroidX]', 'centroid_x', 'COLUMN'
EXEC sp_rename '[PINLOCATIONS].[centroidY]', 'centroid_y', 'COLUMN'
-- recreate index
CREATE NONCLUSTERED INDEX [IX_PL_MISC] ON [dbo].[PINLOCATIONS]
(
	[primary_polygon] ASC,
	[PIN999] ASC
)
INCLUDE ( 	[centroid_x],
	[centroid_y],
	[Name]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
--refresh indexes
DBCC DBREINDEX('PINLOCATIONS') -- 
