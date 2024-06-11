---
name: Add a new dbt model
about: Request the addition of a new model to the dbt DAG.
title: Add a new dbt model
labels: ''
assignees: ''
---

_(Replace or delete anything in parentheses with your own issue content.)_

# New dbt model

_(Brief description of the task here.)_

## Model attributes

* **Name**: _(What should the model be called? See [Model
 naming](/ccao-data/data-architecture/tree/master/dbt#model-naming) for guidance.)_
* **Model type**: _(SQL or Python? See [Model type (SQL or
  Python)](/ccao-data/data-architecture/tree/master/dbt#model-type-sql-or-python)
  for guidance.)_
* **Materialization**: _(Should the model be a table or a view? See [Model
  materialization](/ccao-data/data-architecture/tree/master/dbt#model-materialization) for
  guidance.)_
* **Tests**:
  * _(Add a bulleted list of tests here. See [Model
  tests](/ccao-data/data-architecture/tree/master/dbt#model-tests) for guidance.)_
* **Description**: _(Provide a rich description for this model that will be
  displayed in documentation. Markdown is supported, and encouraged for more
  complex models. See [Model
  description](/ccao-data/data-architecture/tree/master/dbt#model-description) for guidance.)_

## Short checklist

_(Use this checklist if the assignee already knows how to add a dbt model.
Otherwise, delete it in favor of the long checklist in the following section.)_

- [ ] Confirm that a subdirectory for this model's database exists in
  the `dbt/models/` directory, and if not, create one, add a new `schema.yml`
  file, and update `dbt_project.yml` to document the `+schema`
- [ ] Define the SQL query or Python script that creates the model in the model
  subdirectory, following any existing file naming schema
- [ ] Use `source()` and `ref()` to reference other models where possible
- [ ] Optionally configure model materialization within the query or script file
- [ ] Update the `schema.yml` file in the subfolder of `dbt/models/` to point
  to the new model definition
- [ ] _[Python models only]_ Configure any third-party pure Python packages in
  the `config.packages` attribute of your model schema definition and in the
  script file
- [ ] Add tests to the model schema definition in `schema.yml`
- [ ] _[SQL models only]_ If your model definition requires any new macros, make
  sure those macros are tested in `dbt/macros/tests/test_all.sql`
- [ ] Commit your changes to a branch and open a pull request

## Checklist

Complete the following checklist to add the model:

- [ ] Confirm that a subdirectory for this model's database exists in
  the `dbt/models/` directory (e.g. `dbt/models/default/` for
  the `default.new_model` model). If a subdirectory does not yet exist, create
  one, add a `schema.yml` file to the directory to store [model
  properties](https://docs.getdbt.com/reference/model-properties), and update
  `dbt_project.yml` to document the new directory under the `models.athena`
  key with a `+schema` attribute.

```yaml
# Table example (only the model name would change for a view)
# schema.yml
models:
  - name: default.new_model
```

```diff
# View or table example
--- a/dbt/dbt_project.yml
+++ b/dbt/dbt_project.yml

 models:
   athena:
     +materialized: view
+    default:
+      +schema: default
     census:
       +schema: census
```

- [ ] Define the SQL query or Python script that creates the model in the
  appropriate subfolder of the `dbt/models/` directory. For example, if you're
  adding a view to the `default` schema, then the model definition file should
  live in `dbt/models/default`. The file should have the same name as the model
  that appears in Athena. A period in the model name should separate the
  entity name from the database namespace (e.g. `default.vw_pin_universe.sql`).
  All views should have a name prefixed with `vw_`.

```bash
# SQL view example
touch dbt/models/default/default.vw_new_model.sql

# SQL table example
touch dbt/models/proximity/proximity.new_model.sql

# Python model example
touch dbt/models/proximity/proximity.new_model.py
```

- [ ] Use
  [`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
  and [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) to
  reference other models where possible in your query or script.

```sql
-- SQL view or table example
-- Either dbt/models/default/default.vw_new_model.sql
-- or dbt/models/default/default.new_model.sql
select pin10, year
from {{ source('raw', 'foobar') }}
join {{ ref('default.vw_pin_universe') }}
using (pin10, year)
```

```python
# Python model example
# dbt/models/default/default.new_model.py
import pandas as pd

def model(dbt, spark_session):
    raw_foobar = dbt.source("raw", "foobar")
    vw_pin_universe = dbt.ref("default.vw_pin_universe")
    result = pd.merge(raw_foobar, vw_pin_universe, on=["pin10", "year"])
    dbt.write(result[["pin10", "year"]])
```

- [ ] Optionally configure model materialization. If the output of the query
  should be a view, no action is necessary, since the default for all models in
  this repository is to materialize as views; but if the output should be a
  table, with table data stored in S3, then you'll need to add a config block
  to the top of the view to configure materialization. Note that all Python
  models must be materialized as tables.

```diff
# Table example
--- dbt/models/default/default.new_model.sql
+++ dbt/models/default/default.new_model.sql
+ {{
+   config(
+     materialized='table',
+     partitioned_by=['year'],
+     bucketed_by=['pin10'],
+     bucket_count=1
+   )
+ }}
select pin10, year
from {{ source('raw', 'foobar') }}
join {{ ref('default.vw_pin_universe') }}
using (pin10, year)
```

```diff
# Python model example
--- dbt/models/default/default.new_model.py
+++ dbt/models/default/default.new_model.py
import pandas as pd

def model(dbt, spark_session):
+    dbt.config(materialized="table")
    raw_foobar = dbt.source("raw", "foobar")
    vw_pin_universe = dbt.ref("default.vw_pin_universe")
    result = pd.merge(raw_foobar, vw_pin_universe, on=["pin10", "year"])
    dbt.write(result[["pin10", "year"]])
```

- [ ] Update the `schema.yml` file in the subfolder of `dbt/models/` where you
  created your model definition. Make sure to add descriptions for new entities
  (models, sources, columns, etc). See
  [Model description](/ccao-data/data-architecture/tree/master/dbt#model-description)
  and [Column descriptions](/ccao-data/data-architecture/tree/master/dbt#column-descriptions)
  for specific guidance on doc locations and using docs blocks.

```diff
# Table example (only the model name would change for a view)
--- a/dbt/models/default/schema.yml
+++ b/dbt/models/default/schema.yml

 models:
+  - name: default.new_model
+    description: New model
+    columns:
+      - name: pin10
+        description: 10-digit PIN
+      - name: year
+        description: Year
   - name: default.vw_pin_history
     description: PIN history
     data_tests:
```

- [ ] _[Python models only]_ If you need any third-party pure Python packages
  that are not [preinstalled in the Athena PySpark
  environment](https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-preinstalled-python-libraries.html),
  include them and their dependencies in the `config.packages` attribute and
  in your model code. See [A note on third-party pure Python dependencies for Python
  models](/ccao-data/data-architecture/tree/master/dbt#a-note-on-third-party-pure-python-dependencies-for-python-models)
  for more details.

```diff
# Example
--- a/dbt/models/default/schema.yml
+++ b/dbt/models/default/schema.yml
 models:
  - name: default.new_model
    description: New model
+    config:
+      packages:
+        - "assesspy==1.1.0"
    columns:
      - name: pin10
        description: 10-digit PIN
      - name: year
        description: Year
```

```diff
# Example
--- dbt/models/default/default.new_model.py
+++ dbt/models/default/default.new_model.py
+ # pylint: skip-file
+ # type: ignore
+ sc.addPyFile(  # noqa: F821
+     "s3://ccao-athena-dependencies-us-east-1/assesspy==1.1.0.zip"
+ )
+
+ import assesspy
import pandas as pd

def model(dbt, spark_session):
    dbt.config(materialized="table")
    raw_foobar = dbt.source("raw", "foobar")
    vw_pin_universe = dbt.ref("default.vw_pin_universe")
    result = pd.merge(raw_foobar, vw_pin_universe, on=["pin10", "year"])
    dbt.write(result[["pin10", "year"]])

```

- [ ] Add tests to your new model definition in `schema.yml`.

```diff
# Table example (only the model name would change for a view)
--- a/dbt/models/default/schema.yml
+++ b/dbt/models/default/schema.yml

 models:
   - name: default.new_model
     description: New model
     columns:
       - name: pin10
         description: 10-digit PIN
       - name: year
         description: Year
+    data_tests:
+      - unique_combination_of_columns:
+        name: new_model_unique_by_pin_and_year
+        combination_of_columns:
+          - pin
+          - year
   - name: default.vw_pin_history
     description: PIN history
     data_tests:
```

- [ ] _[SQL models only]_ If your model definition requires any new macros, make
  sure those macros are tested in `dbt/macros/tests/test_all.sql`. If any tests
  need implementing, follow the pattern set by existing tests to implement them.

- [ ] Commit your changes to a branch and open a pull request to build your
  model and run tests in a CI environment.
