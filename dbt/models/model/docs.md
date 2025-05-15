# assessment_card

{% docs table_assessment_card %}
Card-level (building) model outputs by model run (`run_id`).

Includes predicted values per card, as well as nearly all the input
variables used for prediction. Each model run covers the entire county,
even areas that are not being reassessed.

Predictions in this table result from the "fully trained" model, which uses
all possible sales. As a result, the predictions here are effectively
in-sample.

**Primary Key**: `year`, `run_id`, `meta_pin`, `meta_card_num`
{% enddocs %}

# assessment_pin

{% docs table_assessment_pin %}
PIN-level model outputs by model run (`run_id`).

Includes recent sales, PIN-level characteristics, desk review flags,
PIN-level predicted values, etc. Predicted values are aggregated from
`model.assessment_card` and post-processed in a variety of ways.

**Primary Key**: `year`, `run_id`, `meta_pin`
{% enddocs %}

# comp

{% docs table_comp %}

Table containing comparable sales along with similarity scores
for each comparable sale extracted from the structure of the
tree model.

These comparable sales are experimental and not yet public.
For details on our current approach to extracting comparable
sales from the model, see [this
vignette](https://ccao-data.github.io/lightsnip/articles/finding-comps.html).

**Primary Key**: `pin`, `card`, `run_id`
{% enddocs %}

# feature_importance

{% docs table_feature_importance %}
Overall feature importance by model run (`run_id`).

Includes metrics such as gain, cover, and frequency. This is the output
of the built-in LightGBM/XGBoost feature importance methods.

**Primary Key**: `year`, `run_id`, `model_predictor_all_name`
{% enddocs %}

# final_model

{% docs table_final_model %}
A table containing metadata and information about the final model used for
each tax year and township.

This is the _parsed_ version of `model.final_model_raw` and is used to pull
the correct model values for reporting views.

**Primary Key**: `year`, `run_id`, `township_code_coverage`
{% enddocs %}

# metadata

{% docs table_metadata %}
Information about every model run by `run_id`.

Includes information like the run timestamp, all input parameters, commit
hashes of the model code, notes, etc.

**Primary Key**: `year`, `run_id`
{% enddocs %}

# parameter_final

{% docs table_parameter_final %}
Final hyperparameters used by each model run (`run_id`), chosen via
cross-validation.

If hyperparameters are blank for a given run, then that parameter was not used.

**Primary Key**: `year`, `run_id`
{% enddocs %}

# parameter_range

{% docs table_parameter_range %}
Range of hyperparameters searched by a given model run (`run_id`)
during cross-validation.

**Primary Key**: `year`, `run_id`, `parameter_name`
{% enddocs %}

# parameter_search

{% docs table_parameter_search %}
Hyperparameters used for _every_ cross-validation iteration, along with
the corresponding performance statistics.

**Primary Key**: `year`, `run_id`, `iteration`, `configuration`, `fold_id`
{% enddocs %}

# performance

{% docs table_performance %}

Geographically-delineated model performance statistics by model run (`run_id`).

Includes breakouts for many levels of geography, as well as different "stages".
The stages are:

- `test` - Performance on the out-of-sample test set (typically the
  most recent 10% of sales)
- `assessment` - Performance on the most recent year of sales (after being
  trained on all sales, so in-sample)

**Primary Key**: `year`, `run_id`, `stage`, `triad_code`, `geography_type`,
`geography_id`, `by_class`
{% enddocs %}

# performance_quantile

{% docs table_performance_quantile %}
Identical to `model.performance`, but additionally broken out by quantile.

**Primary Key**: `year`, `run_id`, `stage`, `triad_code`, `geography_type`,
`geography_id`, `by_class`, `num_quantile`, `quantile`
{% enddocs %}

# pinval_test_training_data

{% docs pinval_test_training_data %}

Testing table for storing training data in athena.

{% enddocs %}

# shap

{% docs table_shap %}
SHAP (SHapley Additive exPlanations) values for every predicted value
for every model run (`run_id`).

These are the per feature per observation contribution to the final predicted
model value starting from a baseline value. See the
[shap repository](https://github.com/shap/shap) for more information.

**Primary Key**: `year`, `run_id`, `meta_pin`, `meta_card_num`
{% enddocs %}

# test_card

{% docs table_test_card %}
Card-level (building) model outputs on the test set by model run (`run_id`).

The test set is the out-of-sample data used to evaluate model performance.
Predictions in this table are trained using only data _not in this set
of sales_.

**Primary Key**: `year`, `run_id`, `meta_pin`, `meta_card_num`, `meta_sale_document_num`
{% enddocs %}

# timing

{% docs table_timing %}
Wall time of each stage (train, assess, etc.) for each model run (`run_id`).

**Primary Key**: `year`, `run_id`
{% enddocs %}

# training_data

{% docs table_training_data %}

A table containing the training data from the final model runs. This is uploaded
manually at the end of modeling via the [`S3 model-training_data.R`](https://github.com/ccao-data/data-architecture/tree/master/etl/scripts-ccao-data-warehouse-us-east-1/model/model-training_data.R)
script.

**Primary Key**: `run_id`, `meta_card_num`, `meta_pin`
{% enddocs %}

# vw_card_res_input

{% docs view_vw_card_res_input %}
Main residential model input view containing characteristics and features
used for yearly predictive modeling.

Observations are at the card (building) level. This view forward fills certain
missing characteristic data that is unlikely to change. It also includes
data necessary to calculate performance statistics and desk review flags,
such as different geographies.

Note that this view is _very_ heavy and takes a long time to run. Use
data cached by DVC when possible. See
[model-res-avm#getting-data](https://github.com/ccao-data/model-res-avm#getting-data)
for more information.

**Primary Key**: `year`, `meta_pin`, `meta_card_num`
{% enddocs %}

# vw_pin_condo_input

{% docs view_vw_pin_condo_input %}
Main condominium model input view containing condo characteristics and
features for yearly predictive modeling.

Observations are at the PIN-14 (condo unit) level. Unlike the residential
input view, this view does not perform filling. Instead condo characteristics
are backfilled in `default.vw_pin_condo_char`.

**Primary Key**: `year`, `meta_pin`
{% enddocs %}

# vw_pin_shared_input

{% docs view_vw_pin_shared_input %}
View to compile PIN-level model inputs shared between the residential
(`model.vw_card_res_input`) and condo (`model.vw_pin_condo_input`) model views.

**Primary Key**: `year`, `meta_pin`
{% enddocs %}
