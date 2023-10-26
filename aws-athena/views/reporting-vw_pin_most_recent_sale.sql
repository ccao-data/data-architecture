-- View containing most recent filtered sales

-- Universe of all PINs from most recent year of iasWorld data
WITH all_pins AS (
    SELECT DISTINCT parid
    FROM {{ source('iasworld', 'pardat') }}
    WHERE taxyr = CAST(YEAR(CURRENT_DATE) AS VARCHAR)
        AND cur = 'Y'
        AND deactivat IS NULL
),

-- Order PINs by sale date descending and rank them
sale_rank AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY pin
            ORDER BY sale_date DESC
        ) AS rank
    FROM {{ ref('default.legacy_vw_pin_sale') }}
)

SELECT
    COALESCE(sr.pin, ap.parid) AS pin,
    sr.year,
    sr.township_code,
    sr.class,
    sr.sale_date,
    sr.is_mydec_date,
    sr.sale_price,
    sr.sale_price_log10,
    sr.sale_key,
    sr.doc_no,
    sr.deed_type,
    sr.seller_name,
    sr.is_multisale,
    sr.num_parcels_sale,
    sr.buyer_name,
    sr.sale_type,
    sr.sale_filter_lower_limit,
    sr.sale_filter_upper_limit,
    sr.sale_filter_count,
    sr.property_advertised,
    sr.is_installment_contract_fulfilled,
    sr.is_sale_between_related_individuals_or_corporate_affiliates,
    sr.is_transfer_of_less_than_100_percent_interest,
    sr.is_court_ordered_sale,
    sr.is_sale_in_lieu_of_foreclosure,
    sr.is_condemnation,
    sr.is_short_sale,
    sr.is_bank_reo_real_estate_owned,
    sr.is_auction_sale,
    sr.is_seller_buyer_a_relocation_company,
    sr.is_seller_buyer_a_financial_institution_or_government_agency,
    sr.is_buyer_a_real_estate_investment_trust,
    sr.is_buyer_a_pension_fund,
    sr.is_buyer_an_adjacent_property_owner,
    sr.is_buyer_exercising_an_option_to_purchase,
    sr.is_simultaneous_trade_of_property,
    sr.is_sale_leaseback,
    sr.is_homestead_exemption,
    sr.homestead_exemption_general_alternative,
    sr.homestead_exemption_senior_citizens,
    sr.homestead_exemption_senior_citizens_assessment_freeze

FROM sale_rank AS sr
-- This just makes sure that all PINs are included, even if they have no sales
FULL OUTER JOIN all_pins AS ap
    ON sr.pin = ap.parid
WHERE sr.rank = 1
    OR sr.rank IS NULL
