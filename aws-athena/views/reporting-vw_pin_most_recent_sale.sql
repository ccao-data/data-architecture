-- View containing most recent filtered sales
CREATE OR replace VIEW reporting.vw_pin_most_recent_sale
AS
-- Universe of all PINs from most recent year of iasWorld data
WITH all_pins AS (
    SELECT DISTINCT
        parid
    FROM iasworld.pardat
    WHERE taxyr = CAST(Year(current_date) AS VARCHAR)
    ),
-- Order PINs by sale date descending and rank them
sale_rank AS (
    SELECT
        *,
        rank() over(
            PARTITION BY pin
            ORDER BY sale_date DESC
            ) AS rank
    FROM default.vw_pin_sale
    )

SELECT

    CASE WHEN pin IS NULL THEN parid ELSE pin END AS pin,
    year,
    township_code,
    class,
    sale_date,
    is_mydec_date,
    sale_price,
    sale_price_log10,
    sale_key,
    doc_no,
    deed_type,
    seller_name,
    is_multisale,
    num_parcels_sale,
    buyer_name,
    sale_type,
    sale_filter_lower_limit,
    sale_filter_upper_limit,
    sale_filter_count,
    property_advertised,
    is_installment_contract_fulfilled,
    is_sale_between_related_individuals_or_corporate_affiliates,
    is_transfer_of_less_than_100_percent_interest,
    is_court_ordered_sale,
    is_sale_in_lieu_of_foreclosure,
    is_condemnation,
    is_short_sale,
    is_bank_reo_real_estate_owned,
    is_auction_sale,
    is_seller_buyer_a_relocation_company,
    is_seller_buyer_a_financial_institution_or_government_agency,
    is_buyer_a_real_estate_investment_trust,
    is_buyer_a_pension_fund,
    is_buyer_an_adjacent_property_owner,
    is_buyer_exercising_an_option_to_purchase,
    is_simultaneous_trade_of_property,
    is_sale_leaseback,
    is_homestead_exemption,
    homestead_exemption_general_alternative,
    homestead_exemption_senior_citizens,
    homestead_exemption_senior_citizens_assessment_freeze

FROM sale_rank
-- This just makes sure that all PINs are included, even if they have no sales
FULL OUTER JOIN all_pins ON sale_rank.pin = all_pins.parid
WHERE rank = 1 OR rank IS NULL