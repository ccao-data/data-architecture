WITH recent_sales AS (
    SELECT
        REPLACE(sale.line_1_primary_pin, '-', '') AS pin,
        sale.line_8_intended_number_of_apartment_units,
        sale.line_8_current_number_of_apartment_units,
        sale.line_4_instrument_date,
        sale.line_3_additional_pins,
        sale.full_address,
        ROW_NUMBER() OVER (
            PARTITION BY REPLACE(sale.line_1_primary_pin, '-', '')
            ORDER BY DATE_PARSE(sale.line_4_instrument_date, '%Y-%m-%d') DESC
        ) AS rn
    FROM
        sale.mydec AS sale
    WHERE
        sale.line_8_intended_number_of_apartment_units > 0
)

SELECT
    rc.pin,
    rc.class,
    rc.char_apts,
    recent_sale.line_8_intended_number_of_apartment_units,
    recent_sale.full_address,
    recent_sale.line_8_current_number_of_apartment_units,
    recent_sale.line_4_instrument_date,
    recent_sale.line_3_additional_pins
FROM
    default.vw_card_res_char AS rc
INNER JOIN
    recent_sales AS recent_sale
    ON
    rc.pin = recent_sale.pin
WHERE
    recent_sale.rn = 1
    AND rc.year = '2023'
    AND (
        CAST(rc.char_apts AS INTEGER)
        != recent_sale.line_8_intended_number_of_apartment_units - 1
    )
    AND recent_sale.line_3_additional_pins = 0
