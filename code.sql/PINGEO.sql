CREATE VIEW PINGEO
AS
SELECT DISTINCT * FROM [CCAODATA].[dbo].[PINLOCATIONS] p
                INNER JOIN [CCAODATA].[dbo].[PROPLOCS] l
                                                ON l.PL_PIN = CAST(p.Name as bigint)    
                WHERE primary_polygon = 1