{% docs dist_pin_to_pin %}
View that finds the three nearest neighbor PINs for every PIN for every year.
{% enddocs %}

{% docs dist_pin_to_pin_intermediate %}
Intermediate table used to generate `proximity.dist_pin_to_pin`.

The `proximity.dist_pin_to_pin` view is intended to record distances to the
three closest PINs for all PINs in the county for all years in the data.
This type of recursive spatial query is expensive, however, and some PINs are
quite far (>1km) from the nearest three PINs, so we use intermediate tables
to strike a balance between data completeness and computational efficiency.

To compute the full set of distances in `proximity.dist_pin_to_pin`, we first
generate PIN-to-PIN distances using a 1km buffer and store the results in the
`proximity.dist_pin_to_pin_1km` table. Then, we query for PINs that did not have
any matches within 1km and redo the distance query with an expanded 10km buffer,
storing the results in the `proximity.dist_pin_to_pin_10km` table. Finally, the
10km table is aliased to the `proximity.dist_pin_to_pin` view for ease of
querying.
{% enddocs %}
