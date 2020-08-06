
ALTER VIEW VW_DESK_REVIEW AS 

SELECT 

TC.township_name AS Township, TC.triad_name AS [Triennial Area], HD_NBHD as [Neighborhood code]
, A.PIN, c.CLASS as [Class], A.[MODEL RESULT] as [2020 Model Result], A.[FIRST PASS] as [2020 Mailed Value]
, A2.[BOR RESULT] AS [2019 BOR Cert. Value]
, D65_COMMENT as [Desk review comment]
, CASE WHEN A.[FIRST PASS] IS NULL THEN (A.[MODEL RESULT]-A2.[BOR RESULT])/A2.[BOR RESULT]
ELSE (A.[FIRST PASS]-A2.[BOR RESULT])/A2.[BOR RESULT] END AS [YOY Percent Change]
, I.most_recent_sale_date as [Most recent sale date], I.most_recent_sale_price as [Most recent sale price]
, I.DOC_NO as [Document Number]
, C.AGE as [Age], C.BLDG_SF as [Building square feet], T.HD_HD_SF as [Land square feet]
, C.FBATH as [Full baths], C.HBATH as [Half baths], C.FRPL AS [Fireplaces]
, CASE WHEN C.BSMT=1 THEN 'Full' WHEN BSMT=2 THEN 'Slab' WHEN BSMT=3 THEN 'Partial' WHEN BSMT=4 THEN 'Crawl' ELSE NULL END AS [Basement type]
, CASE WHEN C.BSMT_FIN=1 THEN 'Formal rec room' WHEN C.BSMT_FIN=2 THEN 'Apartment' WHEN C.BSMT_FIN=3 THEN 'Unfinished' ELSE NULL END AS [Basement finish]
, CASE WHEN EXT_WALL=1 THEN 'Wood' WHEN EXT_WALL=2 THEN 'Masonry' WHEN EXT_WALL=3 THEN 'Wood & Masonry' WHEN EXT_WALL=4 THEN 'Stucco' ELSE NULL END AS [Exterior wall material]
, CASE WHEN ROOF_CNST=1 THEN 'Shingle/Asphalt' WHEN ROOF_CNST=2 THEN 'Tar & Gravel' WHEN ROOF_CNST=3 THEN 'Slate' WHEN ROOF_CNST=4 THEN 'Shake' WHEN ROOF_CNST=5 THEN 'Tile' WHEN ROOF_CNST=6 THEN 'Other' ELSE NULL END AS [Roof material]
, CASE WHEN ATTIC_TYPE=1 THEN 'Full' WHEN ATTIC_TYPE=2 THEN 'Partial' WHEN ATTIC_TYPE=3 THEN 'None' ELSE NULL END AS [Attic type]
, CASE WHEN ATTIC_FNSH=1 THEN 'Living area' WHEN ATTIC_FNSH=2 THEN 'Apartment' WHEN ATTIC_FNSH=3 THEN 'Unfinished' ELSE NULL END AS [Attic finish]
, CASE WHEN TP_DSGN=1 THEN 'Cathedral ceiling' WHEN TP_DSGN=2 THEN 'Secular ceiling' ELSE NULL END AS [Ceiling types]

FROM

VW_OPENDATA_ASSESSMENTS AS A
LEFT JOIN
VW_OPENDATA_ASSESSMENTS AS A2
ON A.Year=A2.Year+1 AND A.PIN=A2.PIN
LEFT JOIN CCAOSFCHARS AS C
ON A.PIN=C.PIN AND A.Year=C.TAX_YEAR
LEFT JOIN VW_MOST_RECENT_IDORSALES AS I
ON A.PIN=I.PIN AND A.Year=Year(I.most_recent_sale_date)
LEFT JOIN 
[65D] ON [65D].D65_PIN=A.PIN AND [65D].TAX_YEAR=A.Year
INNER JOIN 
AS_HEADT AS T
ON A.PIN=T.PIN AND A.Year=T.TAX_YEAR
INNER JOIN
FTBL_TOWNCODES AS TC
ON LEFT(T.HD_TOWN,2)=TC.township_code
WHERE A.Year=2020