left_pad(`pin`, 14, '0')

CASE
  WHEN (`deed_type` = "01") THEN "Warranty"
  WHEN (`deed_type` = "02") THEN "Trustee"
  WHEN (`deed_type` = "03") THEN "Quit claim"
  WHEN (`deed_type` = "04") THEN "Executor"
  WHEN (`deed_type` = "05") THEN "Other"
  WHEN (`deed_type` = "06") THEN "Beneficiary"
  WHEN (`deed_type` = "99") THEN "Unknown"
  ELSE null
END