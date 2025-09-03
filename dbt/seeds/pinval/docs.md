# model_run

{% docs seed_model_run %}

This table holds the production model runs that the homeval application uses to pull data from.

We can have multiple model runs for different purposes.
For example, we can have different types of model runs represented in the `type` column:
    - the model run that generated our predicted values (`"card"`)
    - the model run that generated the top 5 sales (`"comps"`)
    - the model run that generated the shap values used in the Score column (`"shap"`)

Importantly, we will need at least three rows per assessment year in this table,
one for each of the three types of model runs.

The reason that we have multiple different model runs per assessment year and for
each of the different types is that we may want to re-run a model for a specific
type without changing the other types. For example, we may want to re-run the comps
model to get updated top 5 sales due to a change in methodology, without changing
the predicted values or shap values.

It is also theoretically possible to have the same run_id for the all 3 types of
model run. This could happen if we generated shaps and comps along with predicted
values in the same run, and we didn't retroactively make any updates that would
require a new run. In this case, we would still need to add all 3 rows.

**Primary Key**: `assessment_year, type, run_id`
{% enddocs %}

# vars_dict

{% docs seed_vars_dict %}

Table containing human-readable descriptions and tooltips for terms in the pinval application

**Primary Key**: `code`
{% enddocs %}