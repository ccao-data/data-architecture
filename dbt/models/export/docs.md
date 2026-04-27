# vw_cmap

{% docs view_vw_cmap %}
View to gather all necessary output for our yearly CMAP export. CMAP usues the
Assessor's data to attach characteristics and valuation data to parcels.

### Nuance

- Assumes yrblt should be maxed within PIN.
- PINs with rows in `iasworld.dweldat` should be unique by pin, card, and year,
other PINs should be unique by pin and year.

**Primary Key**: `year`, `pin`, `card`
{% enddocs %}
