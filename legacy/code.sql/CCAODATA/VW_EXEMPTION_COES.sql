ALTER VIEW VW_EXEMPTION_COES AS

/*
The goal of this view is to present certifactes of error related only to exemptions by
PIN and tax year.
*/

SELECT PIN, COE_REASON as code, reason, 2000 + COE_TAX_YR AS TAX_YEAR FROM AS_RES_CERTOFCORRECTIONS CERTS

LEFT JOIN FTBL_CERT_OF_ERROR_REASONS REASONS ON CERTS.COE_REASON = REASONS.code

WHERE COE_REASON IN (1, 2, 4, 7, 8, 27, 28, 38, 43, 44, 48, 56, 57)