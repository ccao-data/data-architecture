# License notice ----

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# You should have received a copy of the GNU General Public License along with this program. If not, see https://www.gnu.org/licenses/.

# Top comments ----

# This script pulls comparables from the COMPS table in order to run COMPS method models. The script assumes the user
# has a pre-defined directory structure on their local terminal, and is operating
# from within CCAO's network. Modification may be required for others.

# Load data ----
load(destfile)

# connect to retrieve data
CCAODATA <- dbConnect(odbc()
                      , driver   = "SQL Server"
                      , server   = odbc.credentials("server")
                      , database = odbc.credentials("database")
                      , uid      = odbc.credentials("uid")
                      , pwd      = odbc.credentials("pwd"))

COMPS1 <- dbGetQuery(
  CCAODATA,
  paste0("SELECT TARGET_PIN, AVG(CAST(ADJ_SALE_PRICE AS FLOAT)) AS VALUE FROM 
               (SELECT * FROM COMPS WHERE COMP_METHOD = '1' AND ADJ_SALE_PRICE > 5000) AS TMP GROUP BY TARGET_PIN"))

COMPS2 <- dbGetQuery(
  CCAODATA,
  paste0("SELECT TARGET_PIN, AVG(CAST(ADJ_SALE_PRICE AS FLOAT)) AS VALUE FROM 
               (SELECT * FROM COMPS WHERE COMP_METHOD = '2' AND ADJ_SALE_PRICE > 5000) AS TMP GROUP BY TARGET_PIN"))

COMPS3 <- dbGetQuery(
  CCAODATA,
  paste0("SELECT TARGET_PIN, AVG(CAST(ADJ_SALE_PRICE AS FLOAT)) AS VALUE FROM 
               (SELECT * FROM COMPS WHERE ADJ_SALE_PRICE > 5000) AS TMP GROUP BY TARGET_PIN"))

# disconnect after pulls
dbDisconnect(CCAODATA)