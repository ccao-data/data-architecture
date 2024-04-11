# final_model_raw

{% docs seed_final_model_raw %}
A table containing metadata and information about the final model used for
each tax year and township.

This is the _raw_ version of the table used to construct `model.final_model`,
which is used to pull the correct model values for reporting views.

**Primary Key**: `year`, `run_id`, `township_code_coverage`
{% enddocs %}
