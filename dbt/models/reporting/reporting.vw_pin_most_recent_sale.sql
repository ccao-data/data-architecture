-- View containing most recent filtered sales

-- Universe of all PINs from most recent year of iasWorld data
WITH all_pins AS (
    SELECT DISTINCT parid
    FROM {{ source('iasworld', 'pardat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
        -- noqa: disable=RF02
        AND taxyr = (
            SELECT MAX(taxyr)
            FROM {{ source('iasworld', 'pardat') }}
        )
        -- noqa: enable=RF02
),

-- Order PINs by sale date descending and rank them
sale_rank AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY pin
            ORDER BY sale_date DESC
        ) AS rank
    FROM {{ ref('default.vw_pin_sale') }}
    WHERE NOT is_multisale
        AND NOT sale_filter_same_sale_within_365
        AND NOT sale_filter_less_than_10k
        AND NOT sale_filter_deed_type
)

SELECT
    COALESCE(sr.pin, ap.parid) AS pin,
    sr.year,
    sr.township_code,
    sr.class,
    sr.sale_date,
    sr.is_mydec_date,
    sr.sale_price,
    sr.sale_key,
    sr.doc_no,
    sr.deed_type,
    sr.seller_name,
    sr.is_multisale,
    sr.num_parcels_sale,
    sr.buyer_name,
    sr.sale_type,
    sr.sale_filter_is_outlier,
    sr.mydec_property_advertised,
    sr.mydec_is_installment_contract_fulfilled,
    sr.mydec_is_sale_between_related_individuals_or_corporate_affiliates,
    sr.mydec_is_transfer_of_less_than_100_percent_interest,
    sr.mydec_is_court_ordered_sale,
    sr.mydec_is_sale_in_lieu_of_foreclosure,
    sr.mydec_is_condemnation,
    sr.mydec_is_short_sale,
    sr.mydec_is_bank_reo_real_estate_owned,
    sr.mydec_is_auction_sale,
    sr.mydec_is_seller_buyer_a_relocation_company,
    sr.mydec_is_seller_buyer_a_financial_institution_or_government_agency,
    sr.mydec_is_buyer_a_real_estate_investment_trust,
    sr.mydec_is_buyer_a_pension_fund,
    sr.mydec_is_buyer_an_adjacent_property_owner,
    sr.mydec_is_buyer_exercising_an_option_to_purchase,
    sr.mydec_is_simultaneous_trade_of_property,
    sr.mydec_is_sale_leaseback,
    sr.mydec_is_homestead_exemption,
    sr.mydec_homestead_exemption_general_alternative,
    sr.mydec_homestead_exemption_senior_citizens,
    sr.mydec_homestead_exemption_senior_citizens_assessment_freeze

FROM sale_rank AS sr
-- This just makes sure that all PINs are included, even if they have no sales
FULL OUTER JOIN all_pins AS ap
    ON sr.pin = ap.parid
WHERE sr.rank = 1
    OR sr.rank IS NULL
