# airport_noise

{% docs table_airport_noise %}
Airport noise for each PIN, measured by [DNL](https://www.faa.gov/regulations_policies/policy_guidance/noise/community#:~:text=DNL%20is%20a%20metric%20that,basis%20of%20annual%20aircraft%20operations.).

Computed using a kriging surface of point measurements taken from noise
monitors around each major airport.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# ari

{% docs table_ari %}
[Illinois Housing Development Authority](https://www.ihda.org/developers/market-research/affordability-risk-index/)
affordability risk index

**Primary Key**: `geoid`, `year`
{% enddocs %}

# dci

{% docs table_dci %}
[Economic Innovation Group](https://eig.org/distressed-communities)
Distressed Communities Index

**Primary Key**: `year`, `geoid`
{% enddocs %}

# flood_first_street

{% docs table_flood_first_street %}
PIN-level flood risk and risk change provided by
[First Street](https://firststreet.org/) in 2019.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# great_schools_rating

{% docs table_great_schools_rating %}
Individual school locations and ratings sourced from
[GreatSchools.org](https://www.greatschools.org/).

**Primary Key**: `universal_id`, `year`
{% enddocs %}

# ihs_index

{% docs table_ihs_index %}
[DePaul Institute of Housing Studies (IHS)](https://www.housingstudies.org/)
quarterly House Price Index.

**Primary Key**: `year`, `geoid`, `quarter`
{% enddocs %}
