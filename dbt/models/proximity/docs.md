{% docs dist_pin_to_pin %}
Table that records the three nearest neighbor PINs for every PIN for
every year.

This table is intended to record distances to the three closest PINs for
all PINs in the county for all years in the data. This type of recursive
spatial query is expensive, however, and some PINs are quite far (>1km)
from the nearest three PINs, so we use intermediate CTEs to strike a
balance between data completeness and computational efficiency.

To compute the full set of distances in `proximity.dist_pin_to_pin`, we
first generate PIN-to-PIN distances using a 1km buffer. Then, we query for
PINs that did not have any matches within 1km and redo the distance query
with an expanded 10km buffer. Finally, the union of the 1km query and the
10km query is stored as the `proximity.dist_pin_to_pin` table.
{% enddocs %}
