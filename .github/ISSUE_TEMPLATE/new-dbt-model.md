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
 naming](/ccao-data/data-architecture#model-naming) for guidance.)_
* **Materialization**: _(Should the model be a table or a view? See [Model
  materialization](/ccao-data/data-architecture#model-materialization) for
  guidance.)_
* **Tests**:
  * _(Add a bulleted list of tests here. See [Model
  tests](/ccao-data/data-architecture#model-tests) for guidance.)_
* **Description**: _(Provide a rich description for this model that will be
  displayed in documentation. Markdown is supported, and encouraged for more
  complex models. See [Model
  description](/ccao-data/data-architecture#model-description) for guidance.)_

## Short checklist

_(Use this checklist if the assignee already knows how to add a dbt model.
Otherwise, delete it in favor of the long checklist in the following section.)_

- [ ] Define the SQL query that creates the model in the `aws-athena/` directory
  - [ ] Optionally configure model materialization
- [ ] Confirm that a subdirectory for this model's database exists in
  the `dbt/models/` directory, and if not, create one, add a new `schema.yml`
  file, and update `dbt_project.yml` to document the `+schema`
- [ ] Add a symlink from the appropriate subfolder of the `dbt/models/`
  directory to the new SQL query in the `aws-athena/` directory
- [ ] Add docs for the model to the subdirectory `docs.md` file
- [ ] Update the `schema.yml` file in the subfolder of `dbt/models/` where you
  created your symlink to add a definition for your model
- [ ] Add tests to your new model definition in `schema.yml`
- [ ] If your model definition requires any new macros, make sure those macros
  are tested in `dbt/macros/tests/test_all.sql`
- [ ] Commit your changes to a branch and open a pull request

## Checklist

Complete the following checklist to add the model:

- [ ] Define the SQL query that creates the model in the appropriate subfolder
  of the `aws-athena/` directory. Views should live in `aws-athena/views/`
  while tables should live in `aws-athena/ctas/`. When naming the file for the
  query, the period in the model name that separates the entity name from the
  database namespace should be changed to a hyphen (e.g. `default.new_model`
  should become `default-new_model` for the purpose of the SQL filename).


```bash
# View example
touch aws-athena/views/default-vw_new_model.sql

# Table example
touch aws-athena/ctas/default-new_model.sql
```

- [ ] Use
  [`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
  and [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) to
  reference other models where possible in your query.


```sql
-- View or table example
-- Either aws-athena/views/default-vw_new_model.sql
-- or aws-athena/ctas/default-new_model.sql
select pin10, year
from {{ source('raw', 'foobar') }}
join {{ ref('default.vw_pin_universe') }}
using (pin10, year)
```

- [ ] Optionally configure model materialization. If the output of the query
  should be a view, no action is necessary, since the default for all models in
  this repository is to materialize as views; but if the output should be a
  table, with table data stored in S3, then you'll need to add a config block
  to the top of the view to configure materialization.

```sql
-- Table example
-- aws-athena/ctas/default-new_model.sql
{{
  config(
    materialized='table',
    partitioned_by=['year'],
    bucketed_by=['pin10'],
    bucket_count=1
  )
}}
select pin10, year
from {{ source('raw', 'foobar') }}
join {{ ref('default.vw_pin_universe') }}
using (pin10, year)
```

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
version: 2


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

- [ ] Add a symlink from the appropriate subfolder of the `dbt/models/`
  directory to the SQL query you created in the `aws-athena/` directory.
  Note that the path for the target file in the `aws-athena/` directory
  needs to be relative to the location of the symlink, _not_ to the
  location of the working directory where you run the `ln` command;
  as a result, the path to the target file in the `aws-athena/` directory
  will not autocomplete in your shell as it usually does when you use
  the tab key.

```bash
# View example
ln -s ../../../aws-athena/views/default-vw_new_model.sql dbt/models/default/default.vw_new_model.sql

# Table example
ln -s ../../../aws-athena/ctas/default-new_model.sql dbt/models/default/default.new_model.sql
```

- [ ] The `ln` command won't raise an error if the link it creates is invalid,
  so use `cat` to confirm that the link is pointing to the correct file.

```bash
# View example
cat dbt/models/default/default.vw_new_model.sql

# Table example
dbt/models/default/default.new_model.sql
```

- [ ] Add or edit the docs file for the `dbt/models/` subdirectory your symlink
  is in to add docs for your model.


```diff
# Table example (only the model name would change for a view)
--- a/dbt/models/default/docs.md
+++ b/dbt/models/default/docs.md

 `spatial.township` is not yearly.
 {% enddocs %}

+{% docs new_model %}
+
+Your Markdown docs go here.
+
+{% enddocs %}
+
 {% docs vw_pin_value %}
 CCAO mailed total, CCAO final, and BOR final values for each PIN by year.
```

- [ ] Update the `schema.yml` file in the subfolder of `dbt/models/` where you
  created your symlink to add a definition for your model.

```diff
# Table example (only the model name would change for a view)
--- a/dbt/models/default/schema.yml
+++ b/dbt/models/default/schema.yml

 models:
+  - name: default.new_model
+    description: '{{ doc("new_model") }}'
+    columns:
+      - name: pin10
+        description: 10-digit PIN
+      - name: year
+        description: Year
   - name: default.vw_pin_history
     description: '{{ doc("vw_pin_history") }}'
     tests:
```

- [ ] Add tests to your new model definition in `schema.yml`.

```diff
# Table example (only the model name would change for a view)
--- a/dbt/models/default/schema.yml
+++ b/dbt/models/default/schema.yml

 models:
   - name: default.new_model
     description: '{{ doc("new_model") }}'
     columns:
       - name: pin10
         description: 10-digit PIN
       - name: year
         description: Year
+    tests:
+      - unique_combination_of_columns:
+        name: new_model_unique_by_pin_and_year
+        combination_of_columns:
+          - pin
+          - year
   - name: default.vw_pin_history
     description: '{{ doc("vw_pin_history") }}'
     tests:
```

- [ ] If your model definition requires any new macros, make sure those macros
  are tested in `dbt/macros/tests/test_all.sql`. If any tests need implementing,
  follow the pattern set by existing tests to implement them.

- [ ] Commit your changes to a branch and open a pull request to build your
  model and run tests in a CI environment.
