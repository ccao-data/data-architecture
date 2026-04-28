# vw_cmap

{% docs view_vw_cmap %}
View to gather all necessary output for our yearly CMAP export.

### Nuance

- Assumes year built (`yrblt`) should be maxed within PIN.
- PINs with rows in `iasworld.dweldat` should be unique by pin, card, and year;
other PINs should be unique by pin and year.

**Primary Key**: `year`, `pin`, `card`
{% enddocs %}
