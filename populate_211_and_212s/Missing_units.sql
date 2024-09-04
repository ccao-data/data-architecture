SELECT pin
FROM vw_card_res_char
WHERE (char_apts = '0' OR char_apts IS NULL OR char_apts = '6')
    AND year = '2023'
    AND (class = '211' OR class = '212')
