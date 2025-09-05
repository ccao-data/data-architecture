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

Multiple model runs are maintained for each assessment year to allow independent
updates by type. This enables re-running a model for a specific purpose without
affecting the others. For example, the `"comps"` model can be re-run to refresh
the top 5 sales when methodology changes, while leaving the `"card"` (predicted
values) and `"shap"` runs unchanged.

It is also possible for all three types of runs to share the same `run_id`. This
occurs when predicted values, comps, and shap values are generated in the same
execution, and no subsequent updates require separate runs. In this case, all three
rows must still be recorded explicitly to ensure complete coverage of the required
types.

**Primary Key**: `assessment_year`, `type`, `run_id`
{% enddocs %}

# vars_dict

{% docs seed_vars_dict %}

Table containing human-readable descriptions and tooltips for terms in the pinval application

**Primary Key**: `code`
{% enddocs %}