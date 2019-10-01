--Imports CCRD Transfer File data (change the year, YYYY.csv, in the line 5 as necessary) from Common drive into a ccrdsales data table, be sure to clean the data first in Excel, e.g. Remove trailing zipcodes, commas in unit no, 
--and trim left names to 50 characters.  If the bulk insert/import fails, you have to delete all contents and start over from 1999. Also, 2019 has monthly lists. 

BULK INSERT dbo.DTBL_CCRDSALES
FROM '\\fileserver\ocommon\CCAODATA\data\raw_data\Transfer List CCROD\Transfer List 1999.csv'
WITH (  
      DATAFILETYPE = 'char',  
      FIELDTERMINATOR = ',',  
      ROWTERMINATOR = '\n'  
);