SELECT 'aasysjur' AS table_name, COUNT(*) AS row_count FROM iasworld.aasysjur
UNION ALL
SELECT 'addn' AS table_name, COUNT(*) AS row_count FROM iasworld.addn
UNION ALL
SELECT 'addrindx' AS table_name, COUNT(*) AS row_count FROM iasworld.addrindx
UNION ALL
SELECT 'aprval' AS table_name, COUNT(*) AS row_count FROM iasworld.aprval
UNION ALL
SELECT 'asmt_all' AS table_name, COUNT(*) AS row_count FROM iasworld.asmt_all
UNION ALL
SELECT 'asmt_hist' AS table_name, COUNT(*) AS row_count FROM iasworld.asmt_hist
UNION ALL
SELECT 'cname' AS table_name, COUNT(*) AS row_count FROM iasworld.cname
UNION ALL
SELECT 'comdat' AS table_name, COUNT(*) AS row_count FROM iasworld.comdat
UNION ALL
SELECT 'comnt' AS table_name, COUNT(*) AS row_count FROM iasworld.comnt
UNION ALL
SELECT 'cvleg' AS table_name, COUNT(*) AS row_count FROM iasworld.cvleg
UNION ALL
SELECT 'cvown' AS table_name, COUNT(*) AS row_count FROM iasworld.cvown
UNION ALL
SELECT 'cvtran' AS table_name, COUNT(*) AS row_count FROM iasworld.cvtran
UNION ALL
SELECT 'dedit' AS table_name, COUNT(*) AS row_count FROM iasworld.dedit
UNION ALL
SELECT 'dweldat' AS table_name, COUNT(*) AS row_count FROM iasworld.dweldat
UNION ALL
SELECT 'exadmn' AS table_name, COUNT(*) AS row_count FROM iasworld.exadmn
UNION ALL
SELECT 'excode' AS table_name, COUNT(*) AS row_count FROM iasworld.excode
UNION ALL
SELECT 'exdet' AS table_name, COUNT(*) AS row_count FROM iasworld.exdet
UNION ALL
SELECT 'htagnt' AS table_name, COUNT(*) AS row_count FROM iasworld.htagnt
UNION ALL
SELECT 'htdates' AS table_name, COUNT(*) AS row_count FROM iasworld.htdates
UNION ALL
SELECT 'htpar' AS table_name, COUNT(*) AS row_count FROM iasworld.htpar
UNION ALL
SELECT 'land' AS table_name, COUNT(*) AS row_count FROM iasworld.land
UNION ALL
SELECT 'legdat' AS table_name, COUNT(*) AS row_count FROM iasworld.legdat
UNION ALL
SELECT 'lpmod' AS table_name, COUNT(*) AS row_count FROM iasworld.lpmod
UNION ALL
SELECT 'lpnbhd' AS table_name, COUNT(*) AS row_count FROM iasworld.lpnbhd
UNION ALL
SELECT 'oby' AS table_name, COUNT(*) AS row_count FROM iasworld.oby
UNION ALL
SELECT 'owndat' AS table_name, COUNT(*) AS row_count FROM iasworld.owndat
UNION ALL
SELECT 'pardat' AS table_name, COUNT(*) AS row_count FROM iasworld.pardat
UNION ALL
SELECT 'rcoby' AS table_name, COUNT(*) AS row_count FROM iasworld.rcoby
UNION ALL
SELECT 'sales' AS table_name, COUNT(*) AS row_count FROM iasworld.sales
UNION ALL
SELECT 'splcom' AS table_name, COUNT(*) AS row_count FROM iasworld.splcom
UNION ALL
SELECT 'valclass' AS table_name, COUNT(*) AS row_count FROM iasworld.valclass
ORDER BY table_name
