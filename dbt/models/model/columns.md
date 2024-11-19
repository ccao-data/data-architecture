## parcel_centroid_dist_ft_sd

{% docs column_parcel_centroid_dist_ft_sd %}
Standard deviation of the distance from each major parcel vertex to
the parcel centroid.

Vertices on straight lines or that are otherwise redundant are not considered
in this calculation. For parcels with multiple polygons, distance is calculated
to the centroid of the _largest_ polygon.

The goal of this feature is to identify large parcels with extremely variable
shapes, e.g. `12-08-100-006` (the O'Hare Airport parcel).
{% enddocs %}

## parcel_edge_len_ft_sd

{% docs column_parcel_edge_len_ft_sd %}
Standard deviation of the edge length between parcel vertices.

Vertices on straight lines or that are otherwise redundant are not considered
in this calculation.

The goal of this feature is to identify long, narrow rectangular parcels like
those of roads and railroads, e.g. `18-15-400-009` (a railroad track parcel).
{% enddocs %}

## parcel_interior_angle_sd

{% docs column_parcel_interior_angle_sd %}
Standard deviation of the interior angles of the parcel polygon.

Nearly straight angles (close to 180 degrees) are excluded from this
calculation.

The goal of this feature is to identify parcels with lots of acute or strange
angles in their geometry, e.g. `04-11-501-003` (a rhombus-shaped parcel).
{% enddocs %}

## parcel_mrr_area_ratio

{% docs column_parcel_mrr_area_ratio %}
Ratio of the parcel's area to the area of its
[minimum rotated bounding rectangle](https://en.wikipedia.org/wiki/Minimum_bounding_rectangle).

The goal of this feature is to identify parcels with interior holes or strange
concave shapes, e.g. `16-01-402-008` (basically a right angle parcel).
{% enddocs %}

## parcel_mrr_side_ratio

{% docs column_parcel_mrr_side_ratio %}
Ratio of the longest to the shortest side of the parcel's
[minimum rotated bounding rectangle](https://en.wikipedia.org/wiki/Minimum_bounding_rectangle).

The goal of this feature is to identify parcels with consistent, square shapes.
In practice, parcels with a high ratio tend to have long, extraneous
elements/slivers, e.g. `02-23-311-027`.
{% enddocs %}

## parcel_num_vertices

{% docs column_parcel_num_vertices %}
The number of vertices of the parcel.

Vertices that are redundant or unnecessary (e.g. those on straight lines) are
removed by simplification. The minimum number of vertices is 3.

The goal of this feature is to identify parcels with complex edges and/or
shapes, e.g. `01-33-200-020` (the roads of a subdivision).
{% enddocs %}
