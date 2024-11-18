# dbt

This directory stores the configuration for building our data catalog using
[dbt](https://docs.getdbt.com/docs/core).

## Quick links

### In this document

* [🖼️ Background: What does the data catalog do?](#%EF%B8%8F-background-what-does-the-data-catalog-do)
* [💻 How to develop the catalog](#-how-to-develop-the-catalog)
* [➕ How to add a new model](#-how-to-add-a-new-model)
* [🔨 How to rebuild models using GitHub Actions](#-how-to-rebuild-models-using-github-actions)
* [🧪 How to add and run tests and QC reports](#-how-to-add-and-run-tests-and-qc-reports)
* [🐛 Debugging tips](#-debugging-tips)

### Outside this document

* [📖 Data documentation](https://ccao-data.github.io/data-architecture)
* [📝 Design doc for our decision to develop our catalog with
  dbt](../docs/design-docs/data-catalog.md)
* [🧪 Generic tests we use for testing](./tests/generic/README.md)

## 🖼️ Background: What does the data catalog do?

The data catalog accomplishes a few main goals:

### 1. Store our data transformations as a directed acyclic graph (DAG)

The models defined in [`models/`](./models/) represent the SQL operations that
we run in order to transform raw data into output data for use in statistical
modeling and reporting. These transformations comprise a DAG that we use for
documenting our data and rebuilding it efficiently. We use the [`dbt
run`](https://docs.getdbt.com/reference/commands/run) command to build these
models into views and tables in AWS Athena.

> [!NOTE]
When we talk about "models" in these docs, we generally mean [the
resources that dbt calls
"models"](https://docs.getdbt.com/docs/build/models), namely the definitions of
the tables and views in our Athena warehouse. In contrast, we will use the
phrase "statistical models" wherever we mean to discuss the algorithms that we
use to predict property values.

### 2. Define integrity checks that specify the correct format for our data

The tests defined in the `schema.yml` files in the `models/` directory
set the specification for our source data and its transformations. These
specs allow us to build confidence in the
integrity of the data that we use and publish. We use the
[`dbt test`](https://docs.getdbt.com/docs/build/tests) command to run these
tests against our Athena warehouse.

### 3. Generate documentation for our data

The DAG definition is parsed by dbt and used to build our [data
documentation](https://ccao-data.github.io/data-architecture). We use the
[`dbt docs generate`](https://docs.getdbt.com/reference/commands/cmd-docs)
command to generate these docs.

### 4. Automation for all of the above using GitHub Actions

The workflows, actions, and scripts defined in [the `.github/`
directory](../.github/) work together to perform all of our dbt operations
automatically, and to integrate with the development cycle such that new
commits to the main branch of this repository automatically deploy changes to
our tables, views, tests, and docs. Automated tasks include:

* (Re)building any models that have been added or modified since the last commit
  to the main branch (the `build-and-test-dbt` workflow)
* Running tests for any models that have been added or modified since the last
  commit, including tests that have themselves been added or modified (the
  `build-and-test-dbt` workflow)
* Running tests for all models once per day (the `test-dbt-models` workflow)
* Checking [the freshness of our source
  data](https://docs.getdbt.com/docs/deploy/source-freshness) once per day
  (the `test-dbt-source-freshness` workflow)
* Generating and deploying data documentation to our [docs
  site](https://ccao-data.github.io/data-architecture) on every commit to the
  main branch (the `deploy-dbt-docs` workflow)
* Cleaning up temporary resources in our Athena warehouse whenever a pull
  request is merged into the main branch (the `cleanup-dbt-resources` workflow)

## 💻 How to develop the catalog

### Installation

These instructions are for Ubuntu, which is the only platform we've tested.

For background, see the docs on [installing dbt with
pip](https://docs.getdbt.com/docs/core/pip-install). (You don't need to
follow these docs in order to install dbt; the steps that follow will take
care of that for you.)

#### Requirements

* Python3 with `uv` installed (pre-installed on the CCAO server)
* [AWS CLI installed
  locally](https://github.com/ccao-data/wiki/blob/master/How-To/Connect-to-AWS-Resources.md)
  * You'll also need permissions for Athena, Glue, and S3
* [`aws-mfa` installed locally](https://github.com/ccao-data/wiki/blob/master/How-To/Setup-the-AWS-Command-Line-Interface-and-Multi-factor-Authentication.md)

#### Install dependencies

Run the following commands in this directory:

```bash
uv venv
source .venv/bin/activate
uv python install
uv pip install .
dbt deps
```

### Useful commands

To run dbt commands, make sure you have the virtual environment activated:

```bash
source .venv/bin/activate
```

You must also authenticate with AWS using MFA if you haven't already today:

```bash
aws-mfa
```

#### Build tables and views

We use the [`dbt build`
command](https://docs.getdbt.com/reference/commands/build) to build tables and
views (called [models](https://docs.getdbt.com/docs/build/models) and
[seeds](https://docs.getdbt.com/docs/build/seeds) in dbt jargon) in our
Athena data warehouse. See the following sections for specific instructions
on how to build [development](#build-tables-and-views-in-development) and
[production](#build-tables-and-views-in-production) models.

#### Build tables and views in development

When passed no arguments, `dbt build` will default to building
_all_ tables and views in development schemas dedicated to your user. The full
build takes about three hours and thirty minutes, so we don't recommend running
it from scratch.

Instead, start by copying the production dbt state file (also known as the
[manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)):

```bash
aws s3 cp s3://ccao-dbt-cache-us-east-1/master-cache/manifest.json master-cache/manifest.json
```

Then, use [`dbt clone`](https://docs.getdbt.com/reference/commands/clone) to
clone the production tables and views into your development environment:

```bash
dbt clone --state master-cache
```

This will copy all production views and tables into a new set of Athena schemas
prefixed with your Unix `$USER` name (e.g. `z_dev_jecochr_default` for the
`default` schema when `dbt` is run on Jean's machine).

Once you've copied prod tables and views into your development schemas, you can
rebuild specific tables and views using [dbt's node selection
syntax](https://docs.getdbt.com/reference/node-selection/syntax).

Use `--select` to build one specific table/view, or a group of tables/views.
Here are some example commands that use `--select` to build a subset of all
tables/views:

```bash
# This builds just the vw_pin_universe view
dbt build --select default.vw_pin_universe --resource-types model

# This builds vw_pin_universe as well as vw_pin10_location
dbt build --select default.vw_pin_universe location.vw_pin10_location --resource-types model

# This builds all models and seeds in the default schema
dbt build --select default.* --resource-types model seed
```

> [!NOTE]
> If you are building a [Python model](https://docs.getdbt.com/docs/build/python-models),
> your model may require external dependencies be available on S3.
> To make these dependencies available to your model, run the
> `build-and-test-dbt` workflow on your branch to deploy any Python dependencies
> that you've added to [the `config.packages` attribute](https://docs.getdbt.com/docs/build/python-models#configuring-packages)
> on your model.

#### Build tables and views in production

By default, all `dbt` commands will run against the `dev` environment (called
a [target](https://docs.getdbt.com/reference/dbt-jinja-functions/target) in
dbt jargon), which namespaces the resources it creates by prefixing database
names with `z_dev_` and your Unix `$USER` name.

You should almost never have to manually build tables and views in our
production environment, since this repository is configured to automatically
deploy production models using GitHub Actions for continuous integration.
However, in the rare case that you need to manually build models in production,
see [🔨 How to rebuild models using GitHub
Actions](#-how-to-rebuild-models-using-github-actions).

#### Clean up development resources

If you'd like to remove your development resources to keep our Athena
data sources tidy, you have two options: Delete all of your development Athena
databases, or delete a selection of Athena databases.

To delete all the resources in your local environment (i.e. every Athena
database with a name matching the pattern `z_dev_$USER_$SCHEMA`):

```
../.github/scripts/cleanup_dbt_resources.sh dev
```

To instead delete a selected database, use the [`aws glue delete-database`
command](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/glue/delete-database.html):

```
aws glue delete-database z_dev_jecochr_default
```

Note that these two operations will only delete Athena databases, and will leave
intact any parquet files that your queries created in S3. If you would like to
remove those files as well, delete them in the S3 console or using the [`aws
s3 rm` command](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/rm.html).

#### Run tests

Run tests for all models:

```
dbt test
```

Run all tests for one model:

```
dbt test --select default.vw_pin_universe
```

Run only one test:

```
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year
```

Run a test against the prod models:

```
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year --target prod
```

Run tests for dbt macros:

```
dbt run-operation test_all
```

#### Generate documentation

Note that we configure dbt's [`asset-paths`
attribute](https://docs.getdbt.com/reference/project-configs/asset-paths) in
order to link to images in our documentation. Some of those images, like the
Mermaid diagram defined in `assets/dataflow-diagram.md`, are generated
automatically during the `deploy-dbt-docs` deployment workflow. To generate
them locally, make sure you have
[`mermaid-cli`](https://github.com/mermaid-js/mermaid-cli) installed (we
recommend a [local
installation](https://github.com/mermaid-js/mermaid-cli#install-locally)) and
run the following command:

```bash
for file in assets/*.mmd; do
  ./node_modules/.bin/mmdc -i "$file" -o "${file/.mmd/.svg}"
done
```

Then, generate the documentation:

```
dbt docs generate
```

This will create a set of static files in the `target/` subdirectory that can
be used to serve the docs site.

To serve the docs locally:

```
dbt docs serve
```

Then, navigate to http://localhost:8080 to view the site.

## ➕ How to add a new model

To request the addition of a new model, open an issue using the [Add a new dbt
model](../.github/ISSUE_TEMPLATE/new-dbt-model.md) issue template. The assignee
should follow the checklist in the body of the issue in order to add the model
to the DAG.

There are a few subtleties to consider when requesting a new model, outlined
below.

### Model type (SQL or Python)

We default to SQL models, since they are simple and well-supported, but in
some cases we make use of [Python
models](https://docs.getdbt.com/docs/build/python-models) instead.
Prefer a Python model if all of the following conditions are true:

* The model requires complex transformations that are simpler to express using
  pandas than using SQL
* The model only depends on (i.e. joins to) other models materialized as tables,
  and does not depend on any models materialized as views
* The model's pandas code only imports third-party packages that are either
  [preinstalled in the Athena PySpark
  environment](https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-preinstalled-python-libraries.html)
  or that are pure Python (i.e. that do not include any C extensions or code in
  other languages)
    * The most common packages that we need that are _not_ pure Python are
      geospatial analysis packages like `geopandas`

#### A note on third-party pure Python dependencies for Python models

If your Python model needs to use a third-party pure Python package that is not
[preinstalled in the Athena PySpark
environment](https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-preinstalled-python-libraries.html),
you can configure the dependency to be automatically deployed to our S3 bucket
that stores PySpark dependencies as part of the dbt build workflow on GitHub
Actions. Follow these steps to include your dependency:

1. Update the `config.packages` array on your model definition in your
   model's `schema.yml` file to add elements for each of the packages
   you want to install
     * Make sure to provide a specific version for each package so that our
       builds are deterministic
     * Unlike a typical `pip install` call, the dependency resolver will _not_
       automatically install your dependency's dependencies, so check the
       dependency's documentation to see if you need to manually specify any
       other dependencies in order for your dependency to work

```yaml
# Example -- replace `model.name` with your model name, `dependency_name` with
# your dependency name, and `X.Y.Z` with the version of the dependency you want
# to install
models:
  - name: database_name.table_name
    config:
      packages:
        - "dependency_name==X.Y.Z"
```

2. Add an `sc.addPyFile` call to the top of the Python code that represents your
   model's query definition so that PySpark will make the dependency available
   in the context of your code

```python
# Example -- replace `dependency_name` with your dependency name and `X.Y.Z`
# with the version of the dependency you want to import
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-athena-dependencies-us-east-1/dependency_name==X.Y.Z.zip"
)
```

3. Call `import dependency_name` as normal in your script to make use of the
   dependency

```python
# Example -- replace `dependency_name` with your dependency name
import dependency_name
```

See the `reporting.ratio_stats` model for an example of this type of
configuration.

### Model materialization

There are a number of different ways of materializing tables in Athena
using dbt; see the [dbt
docs](https://docs.getdbt.com/docs/build/materializations) for more
detail.

So far our DAG only uses view and table materialization, although we are
interested in eventually incorporating incremental materialization as well.

The choice between view and table materialization depends on the runtime and
downstream consumption of the model query. There is no hard and fast rule, but
as a general guideline, consider materializing a model as a table if queries
using the table take longer than 30s to execute, or if the model is consumed
by another model that is itself computationally intensive.

Our DAG is configured to materialize models as views by default, so
extra configuration is only required for non-view materialization.

### Model naming

Models should be namespaced according to the database that the model lives in
(e.g. `location.tax` for the `tax` table in the `location` database.) Since
dbt does not yet support namespacing for refs, we include the database as a
prefix in the model name to simulate real namespacing, and we override the
`generate_alias_name` macro to strip out this fake namespace when generating
names of tables and views in Athena
([docs](https://docs.getdbt.com/docs/build/custom-aliases)).

In addition to database namespacing, views should be named with a `vw_` prefix
(e.g. `location.vw_pin10_location`) to mark them as a view, while tables do not
require any prefix (e.g. `location.tax`).

Finally, for the sake of consistency and ease of interpretation, all tables and views should be named using the singular case e.g. `location.tax` rather than `location.taxes`.

### Model location

Models are generally defined in the `schema.yml` file within each database
subdirectory. Resources related to each model should be defined inline (with
the exception of columns, see [Column descriptions](#column-descriptions).

For complicated models with *many* columns or tests, we split `schema.yml`
files into individual files per model. These files should be contained in a
`schema/` subdirectory within each database directory, and should be named
using the fully namespaced model name. For example, the model definition
for `iasworld.sales` lives in `models/iasworld/schema/iasworld.sales.yml`.

### Model description

All new models should include, at minimum, a
[description](https://docs.getdbt.com/reference/resource-properties/description)
of the model itself. We store these model-level descriptions as [docs
blocks](https://docs.getdbt.com/reference/resource-properties/description#use-a-docs-block-in-a-description)
within the `docs.md` file of each schema subdirectory.

Descriptions related to models in a `schema/` subdirectory should still live
in `docs.md`. For example, the description for `default.vw_pin_universe` lives
in `models/default/docs.md`.

#### Column descriptions

New models should also include descriptions for each column. Since the first
few characters of a column description will be shown in the documentation in
a dedicated column on the "Columns" table, column descriptions should always
start with a sentence that is short and simple. This allows docs readers to
scan the "Columns" table and understand what the column represents at a high level.

Column descriptions can live in three separate places with the following hierarchy:

1. `models/shared_columns.md` - Definitions shared across all databases and models
2. `models/$DATABASE/columns.md` - Definitions shared across a single database
3. `models/$DATABASE/schema.yml` OR `models/$DATABASE/schema/$DATABASE-$MODEL.yml` - Definitions specific to a single model

We use the following pattern to determine where to define each column description:

1. If a description is shared by three or more resources *across multiple
  databases*, its text should be defined as a [docs block](https://docs.getdbt.com/reference/resource-properties/description#use-a-docs-block-in-a-description) in `models/shared_columns.md`.
  The docs block identifier for each column should have a `shared_column_` prefix.
2. If a description is shared by three or more resources *across multiple
  models in the same database*, its text should be defined as a
  [docs block](https://docs.getdbt.com/reference/resource-properties/description#use-a-docs-block-in-a-description) in `models/$DATABASE/columns.md`.
  The docs block identifier for each column should have a `column_` prefix.
3. If a description is shared between two or fewer columns, its text should
  be defined inline in the `schema.yml` file under the `description` key for
  the column.

### Model tests

New models should generally be added with accompanying tests to ensure the
underlying data and transformations are correct. For more information on
testing, see [🧪 How to add and run tests and QC reports](#-how-to-add-and-run-tests-and-qc-reports).

## 🔨 How to rebuild models using GitHub Actions

GitHub Actions can be used to manually rebuild part or all of our dbt DAG.
To use this functionality:

- Go to the `build-and-test-dbt` [workflow page](https://github.com/ccao-data/data-architecture/actions/workflows/build_and_test_dbt.yaml)
- Click the **Run workflow** dropdown on the right-hand side of the screen
- Populate the input box following the instructions below
- Click **Run workflow**, then click the created workflow run to view progress

The workflow input box expects a space-separated list of dbt model names or selectors.
Multiple models can be passed at the same time, as the input box values are
passed directly to `dbt build`. Model names _must include the database schema name_. Some possible inputs include:

- `default.vw_pin_sale` - Rebuild a single view
- `default.vw_pin_sale default.vw_pin_universe` - Rebuild two views at once
- `+default.vw_pin_history` - Rebuild a view and all its upstream dependencies
- `location.*` - Rebuild all views under the `location` schema
- `path:models` - Rebuild the full DAG (:warning: takes a long time!)

For more possible inputs using dbt node selection, see the [documentation site](https://docs.getdbt.com/reference/node-selection/syntax#examples).

## 🧪 How to add and run tests and QC reports

We test the integrity of our raw data and our transformations using a few different
types of tests and reports:

| Test type          | Description | Implemented with | Runs on | Runs when | Outputs | Fails on | Who fixes | Use when |
| ------------------ | ----------- | ---------------- | ------- | --------- | ------- | -------- | --------- | -------- |
| [**iasWorld tests**](#iasworld-tests) | Test that hard-and-fast assumptions about iasWorld tables are correct. | [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) | [GitHub Actions](https://github.com/ccao-data/data-architecture/actions/workflows/test_dbt_models.yaml), or [command line](https://github.com/ccao-data/data-architecture/blob/master/dbt/scripts/run_iasworld_data_tests.py) | Daily after Spark ingest, or on demand | Excel workbook and metadata parquet files | Doesn't fail unless tests encounter an error | Valuations | Test logic exclusively references iasWorld tables, and test failures indicate clear, fixable problems |
| [**Unit tests**](#unit-tests) | Test that the transformation logic inside a model definition produces the correct output on a specific set of hypothetical input data. | [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests) | [GitHub Actions](https://github.com/ccao-data/data-architecture/actions/workflows/build_and_test_dbt.yaml) | Every PR or commits to main branch that modify models/tests | GitHub Actions failure notification | Any test failure | Data | Test logic is checking transformation behavior and the full universe of possible input values is easy to express in a few examples |
| [**Data integrity tests**](#data-integrity-tests) | Test that hard-and-fast assumptions about non-iasWorld tables, or tables that merge iasWorld and non-iasWorld data, are correct. | [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) | [GitHub Actions](https://github.com/ccao-data/data-architecture/actions/workflows/test_dbt_models.yaml) | Weekly on a schedule, or on PRs and commits to main branch that modify models/tests | GitHub Actions failure notification | Any test failure | Data | Test logic references data outside iasWorld, or the full universe of possible input values cannot be easily expressed in a few examples |
| [**QC reports**](#qc-reports) | Query for suspicious cases that _might_ indicate a problem with our data, but that can't be confirmed automatically. | [dbt models](https://docs.getdbt.com/docs/build/models) | [Command line](https://github.com/ccao-data/data-architecture/blob/master/dbt/scripts/export_models.py) | On demand | One or more Excel workbooks | Doesn't fail unless queries encounter an error | Valuations | Failures do not necessarily indicate data problems, and a stakeholder has agreed to review output |

The distinctions between these types of tests and reports can be subtle. Here
is a rough guide to how we think about choosing the right type of test or
report for a given task:

* If you can express the test's logic exclusively in the context of an iasWorld
  table, and if you think an iasWorld data owner could immediately act on the
  results of the test without having to know any additional context other than
  attributes of the row in the iasWorld table that failed the test, you should
  define the test as an [**iasWorld test**](#iasworld-tests).
* If the test is checking that a transformation produces a correct value, and
  if you can represent the full universe of possible raw values that could
  produce the transformed value in a few simple examples, you should define
  the test as a [**unit test**](#unit-tests).
* If you can't be sure whether a failure indicates a data problem, and if you
  have partnered with a stakeholder who is committed to reviewing the
  failures, you should define the check as a [**QC report**](#qc-reports).
* In all other cases, you should define the test as a [**data integrity
  test**](#data-integrity-tests).

Here are some real-world examples of each type of test or report:

* **iasWorld tests**: Test that the Basement Type column in the
  `iasworld.dweldat` table is not null
    * [`iasworld_dweldat_bsmt_not_null`](models/iasworld/schema/iasworld.dweldat.yml)
* **Unit tests**: Test that the `default.vw_pin_appeal` view strips
  non-alphanumeric characters from the class code
    * [`default_vw_pin_appeal_class_strips_non_alphanumerics`](models/default/schema/default.vw_pin_appeal.yml)
* **Data integrity tests**: Test that the `default.vw_pin_condo_char` view is
  unique by PIN and year
    * [`default_vw_pin_condo_char_unique_by_14_digit_pin_and_year`](models/default/schema/default.vw_pin_condo_char.yml)
* **QC reports**: Query for condo PINs that are nonlivable but that have
  characteristics corresponding a livable unit
    * [`qc.vw_pin_appeal_mismatched_outcomes`](models/qc/schema.yml)

The following sections describe how to add and run each of these types of
tests and reports.

### iasWorld tests

Our iasWorld data test suite checks that hard-and-fast assumptions about data
in our iasWorld system of record are correct.

For help running iasWorld tests, see [Running iasWorld
tests](#running-iasworld-tests). For help adding iasWorld tests, see
[Adding iasWorld tests](#adding-iasworld-tests) and [Choosing a
generic test for your data test](#choosing-a-generic-test-for-your-data-test).

#### Running iasWorld tests

The iasWorld test suite can be run using the [`run_iasworld_data_tests`
script](./scripts/run_iasworld_data_tests.py).
This script runs the tests and reads the metadata for the run to output a number of
different artifacts with information about the tests:

* An Excel workbook with detailed information on each failure to aid in resolving
  data problems
* Parquet files representing metadata tables that can be uploaded to S3 for aggregate
  analysis

There are two instances when iasWorld tests typically run:

1. Once per day by the [`test-dbt-models` GitHub
   workflow](https://github.com/ccao-data/data-architecture/actions/workflows/test_dbt_models.yaml),
   which pushes Parquet output to S3 in order to support our analysis of test failures over time
2. On demand by a Data team member whenever a Valuations staff member requests
   a copy of the Excel workbook for a township, usually right before the town closes

Since the first instance is a scheduled job that requires no intervention, the following
steps describe how to respond to a request from Valuations staff for
a fresh copy of the test failure output before town closing.

Typically, Valuations staff will ask for test output for a specific township. We'll refer to the
[township code](https://github.com/ccao-data/wiki/blob/master/Data/Townships.md) for this township
using the bash variable `$TOWNSHIP_CODE`.

Run the tests locally using the [`run_iasworld_data_tests`
script](./scripts/run_iasworld_data_tests.yml):

```bash
# Make sure you're in the dbt subdirectory with the virtualenv activated
cd dbt
source venv/bin/activate

# Run the script
python3 scripts/run_iasworld_data_tests.py --township $TOWNSHIP_CODE
```

Then, check the Excel workbook that the script produced to make sure it's formatted
correctly, and send it to Valuations staff for review.

#### Adding iasWorld tests

We implement iasWorld tests using [dbt tests](https://docs.getdbt.com/docs/build/tests)
to check that hard-and-fast assumptions about our raw iasWorld data are correct.
We prefer adding tests inline in `schema.yml` config files using [generic
tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests),
rather than [singular
tests](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests).

There are a few specific modifications a test author needs to make to
ensure that the `run_iasworld_data_tests` script can properly run the test and
interpret its results:

* One of either the test or the model that the test is defined on must be
[tagged](https://docs.getdbt.com/reference/resource-configs/tags) with
the tag `data_test_iasworld`
  * Prefer tagging the model, and fall back to tagging the test if for
    some reason the model cannot be tagged (e.g. if it has some non-QC
    tests defined on it)
  * If you would like to disable a data test but you don't want to remove it
    altogether, you can tag it or its model with `data_test_iasworld_exclude_from_workbook`,
    which will prevent the test (or all of the model's tests, if you tagged
    the model) from running as part of the `select_data_test_iasworld` selector
* The test definition must supply a few specific parameters:
  * `name` must be set and follow the pattern
    `iasworld_<table_name>_<test_description>`
  * `additional_select_columns` must be set to an array of strings
    representing any extra columns that need to be output by the test
    for display in the workbook
    * Generics typically select any columns mentioned by other parameters,
      but if you are unsure which columns will be selected by default
      (meaning they do not need to be included in `additional_select_columns`),
      consult our [documentation](./tests/generic/README.md) for the generic
      test you're using
  * `config.where` should typically set to provide a filter expression
    that restricts tests to unique rows and to rows matching a date range
    set by the `data_test_iasworld_year_start` and `data_test_iasworld_year_end`
    [project variables](https://docs.getdbt.com/docs/build/project-variables)
  * `meta` should be set with a few specific string attributes:
    * `description` (required): A short human-readable description of the test
    * `category` (optional): A workbook category for the test, required if
      a category is not defined for the test's generic in the `TEST_CATEGORIES`
      constant in the [`run_iasworld_data_tests`
      script](./scripts/run_iasworld_data_tests.py)
    * `table_name` (optional): The name of the table to report in the output
      workbook, if the workbook should report a different table name than the
      name of the model that the test is defined on

See the [`iasworld_pardat_class_in_ccao_class_dict`
test](https://github.com/ccao-data/data-architecture/blob/bd4bc1769fe33fdba1dbe827791b5c41389cf6ec/dbt/models/iasworld/schema/iasworld.pardat.yml#L78-L96)
for an example of a test that sets these attributes.

Due to the similarity of parameters defined on iasWorld tests, we make extensive use
of YAML anchors and aliases to define symbols for commonly-used values.
See [here](https://support.atlassian.com/bitbucket-cloud/docs/yaml-anchors/)
for a brief explanation of the YAML anchor and alias syntax.

#### Choosing a generic test for your data test

Writing a data test in a `schema.yml` file requires a [generic
test](https://docs.getdbt.com/best-practices/writing-custom-generic-tests)
to define the underlying test logic. Our generic tests are defined
in the `tests/generic/` directory. Before writing a test, look at
[the documentation for our generics](./tests/generic/README.md) to see if
any of them meet your needs.

If a generic test does not meet your needs but seems like it could be
easily extended to meet your needs (say, if it inner joins two tables
but you would like to be able to configure it to left join those tables
instead) you can modify the macro that defines the generic test as part
of your PR to make the change that you need.

If no generic tests meet your needs and none can be easily modified to
do so, you have two options:

1. **Define a new model in the `models/qc/` directory that _can_ use a pre-existing generic**.
   This is a good option if, say, you need to join two or more tables in a
   complex way that is specific to your test and not easily generalizable.
   With this approach, you can perform that join in the model, and then
   the generic test doesn't need to know anything about it.
2. **Write a new generic test**. If you decide to take this approach,
   make sure to read the docs on [writing custom generic
   tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests).
   This is a good option if you think that the logic you need
   for your test will be easily generalizable to other models
   and other tests. You'll also need to follow a few extra steps that are specific
   to our environment:
     1. Add a default category for your generic test in
        the `TEST_CATEGORIES` constant in the [`run_iasworld_data_tests`
        script](./scripts/run_iasworld_data_tests.py)
     2. Make sure that your generic test supports the `additional_select_columns`
        parameter that most of our generic tests support, making use
        of the `format_additional_select_columns` macro to format the
        parameter when applying it to your `SELECT` condition

### Data integrity tests

Data integrity tests check hard-and-fast assumptions about all of our data
outside of iasWorld. This includes data that come from external sources like
seeds or third-party data providers, as well as the views that we define as
transformations on top of iasWorld data sources to clean them up and join them
to non-iasWorld data.

We implement data integrity tests using [dbt
tests](https://docs.getdbt.com/docs/build/tests). As with our
[iasWorld tests](#iasworld-tests), we prefer adding data tests inline in
`schema.yml` config files using [generic
tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests),
rather than [singular
tests](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests).

Some examples of data integrity tests include:

* Check that the `default.vw_card_res_char` view is unique by the columns `pin`,
  `year`, and `card`
  ([`default_vw_card_res_char_unique_by_card_pin_and_year`](models/default/schema/default.vw_card_res_char.yml))
* Check that our transformations of condo characteristic data in the
  `default.vw_pin_condo_char` view don't ever produce a `null` value for `card`
  ([`default_vw_pin_condo_char_card_not_null`](models/default/schema/default.vw_pin_condo_char.yml))
* Check that GEOIDs are the correct length in the `location.vw_pin10_location`
  view ([`location_vw_pin10_location_7_digit_ids_are_correct_length`](models/location/schema.yml))

#### Running data integrity tests

There are two different times in which data integrity tests run automatically:

1. In the `build-and-test-dbt` GitHub workflow as part of our CI suite when
   a test's model changes, or when the test itself changes
2. Once per week in the `test-dbt-models` GitHub workflow, to proactively warn
   the team about any data problems that we need to fix

You can also run the tests locally using the `select_data_test_non_iasworld`
selector:

```bash
dbt test --selector select_data_test_non_iasworld
```

#### Adding data integrity tests

In contrast to [iasWorld tests](#iasworld-tests), data integrity tests do not
require any special tags or attributes because we do not process their results
in a structured fashion.

See [Choosing a generic test for your data
test](#choosing-a-generic-test-for-your-data-test) for help with choosing a
generic test.

### Unit tests

Unit tests help ensure that the transformations we apply on top of our raw data
do not introduce errors. We use dbt's [unit
testing](https://docs.getdbt.com/docs/build/unit-tests) feature to implement
these tests. Unit tests currently do not require any special tags or
attributes, although this may change in the future as we build out a more
extensive suite of tests.

Unit tests run during the `build-and-test-dbt` GitHub workflow as part of
our CI suite whenever a PR or commit to the main branch adds or modifies a
model or unit test. As such, unit tests will only run automatically in cases
where it's possible that a code change might accidentally violate the
assumptions of the test.

Run the unit tests locally using the `unit_test` resource type:

```bash
dbt test --select resource_type:unit_test
```

### QC reports

QC reports help us investigate suspicious data that _might_ indicate a problem, but
that can't be confirmed automatically. We implement QC reports using dedicated
dbt models that are configured with attributes that can be parsed by the
[`export_models` script](./scripts/export_models.py) and other scripts that
build on it for specific workflows, like the [`export_qc_town_close_reports`
script](./scripts/export_qc_town_close_reports.py).

#### Running QC reports with `export_models`

We run QC reports when Valuations staff ask for them, which most often occurs before
a major event in the Valuations calendar like the close of a township.

The [`export_models` script](./scripts/export_models.py) is the foundation for
our QC reports. The script expects certain Python requirements, which can be installed
by running `uv pip install .[dbt_tests]` in a virtual
environment.

The script exposes a few options that help to export the right data:

* **`--select`** (required unless `--selector` is set): This option can control which
  models the script will export, along with the `--selector` option. This option
  is equivalent to the [dbt `--select`
  option](https://docs.getdbt.com/reference/node-selection/syntax), and any valid
  dbt `--select` expression will work for this option.
* **`--selector`** (required unless `--select` is set): This is an alternate way to
  control which models the script will export using a [dbt
  selector](https://docs.getdbt.com/reference/node-selection/yaml-selectors).
  This option is equivalent to the dbt `--selector` option. One of the `--select` or
  `--selector` options must be set in order to run the script, but both cannot be set or
  else the script will raise an error.
* **`--where`** (optional): This option controls which rows the script will return for the selected
  model in a similar fashion as a SQL `WHERE` clause. Any expression that could follow a
  `WHERE` keyword in a SQL filter condition will work for this option.
* **`--rebuild` or `--no-rebuild`** (optional): This flag determines whether or not the script will rebuild
  the selected models using `dbt run` prior to export. It defaults to false (`--no-rebuild`) and
  is most useful in rare cases where the underlying models that comprise the reports have been
  edited since the last run, typically during the period when a QC report is under active development.
* **`--target`** (optional): The name of the [dbt
  target](https://docs.getdbt.com/reference/dbt-jinja-functions/target) to run
  queries against. Defaults to `dev`. You should never have to set this option
  when running the script locally.

Other scripts that build on `export_models` for specific workflows tend to
expose similar options. See the documentation for these workflows below for
more details.

#### Running town close QC reports with `export_qc_town_close_reports`

We run town close reports using the [`scripts/export_qc_town_close_reports.py`
script](./scripts/export_qc_town_close_reports.py), which builds on top of
`export_models`. As such, `export_qc_town_close_reports` expects the same set
of Python requirements as `export_models`, which can be installed in a virtual
environment by running `uv pip install .[dbt_tests]`.

The script exposes the following options, many of which are the same as
`export_models`:

* **`--township`** (optional): One or more space-separated [township
  codes](https://github.com/ccao-data/wiki/blob/master/Data/Townships.md) to use
  for filtering results. If you omit this parameter, the script will default to
  exporting reports for all towns.
* **`--year`** (optional): The year to use for filtering results. Defaults to the current year.
* **`--target`** (optional): The name of the [dbt
  target](https://docs.getdbt.com/reference/dbt-jinja-functions/target) to run
  queries against. Defaults to `dev`. You should never have to set this option
  when running the script locally.
* **`--rebuild` or `--no-rebuild`** (optional): This flag determines whether or not the script will rebuild
  the selected models using `dbt run` prior to export. It defaults to false (`--no-rebuild`) and
  is most useful in rare cases where the underlying models that comprise the reports have been
  edited since the last run, typically during the period when a QC report is under active development.
* **`--print-table-refresh-command`** (optional): Instructs the script to print a command that can be
  run on the server to refresh underlying iasWorld tables. Will not export any reports
  when set. Useful if you want to refresh iasWorld table data before running exports.
  See [Refreshing iasWorld tables prior to running town close QC
  reports](#refreshing-iasworld-tables-prior-to-running-town-close-qc-reports) for more
  details.
* **`output-dir`** (optional): The Unix-formatted path to the directory where
  the script will store output files. Defaults to `./export/output/`.

Assuming a township code defined by `$TOWNSHIP_CODE` and a tax year defined by
`$TAXYR`, the following command will generate town close reports for the township/year combo:

```
python3 scripts/export_qc_town_close_reports.py --township "$TOWNSHIP_CODE" --year "$TAXYR"
```

You can omit the `--year` flag and the script will default to the current year of data:

```
python3 scripts/export_qc_town_close_reports.py --township "$TOWNSHIP_CODE"
```

Omit all options to generate reports for all towns in the current year:

```
python3 scripts/export_qc_town_close_reports.py
```

In both cases, the script will output the reports to the `dbt/export/output/`
directory, and will print the names of the reports that it exports during execution.

#### Refreshing iasWorld tables prior to running town close QC reports

The queries that generate town close reports run against our data warehouse, which
ingests data from iasWorld overnight once daily. Sometimes a Valuations staff member
will request a report during the middle of the workday, and they will need the most
recent data, which will not exist in our warehouse yet. In these cases, you can use
the `--print-table-refresh-command` flag to output a command that you can run on the server to
refresh any iasWorld tables that the town close reports rely on. Note that when
you pass `--print-table-refresh-command` to the script, it will _not_ export any reports, and
will instead exit immediately after printing the refresh command.

The following command will print a refresh command that can be run on the server
to refresh the iasWorld tables that comprise our town close reports for the
township with code `$TOWNSHIP_CODE` and the current year of data:

```
python3 scripts/export_qc_town_close_reports.py --township "$TOWNSHIP_CODE" --print-table-refresh-command
```

You should see output like this, which you can run in the context of the
[`service-spark-iasworld`](https://github.com/ccao-data/service-spark-iasworld/)
repository on the server in order to refresh iasWorld tables:

```
Run the following commands on the Data Team server:

cd /path/to/service-spark-iasworld/
docker compose up -d
docker exec spark-node-master ./submit.sh --json-string --no-run-github-workflow
'{"aprval": {"table_name": "iasworld.aprval", "min_year": 2024, "cur": ["Y"], ...
```

#### Running the AHSAP change in value QC report

We define the AHSAP change in value QC report using one model, `qc.vw_change_in_ahsap_values`,
which we filter for a specific township name (like "Hyde Park") and tax year during export.

Here's an example of how to export that model for a township name defined by `$TOWNSHIP_NAME`
and a tax year defined by `$TAXYR`:

```
python3 scripts/export_models.py --select qc.vw_change_in_ahsap_values --where "taxyr = '$TAXYR' and township_name = '$TOWNSHIP_NAME'"
```

The script will output the reports to the `dbt/export/output/` directory, and will print the
name of the report that it exports during execution.

#### Adding QC reports

Since QC reports are built on top of models, adding a new QC report can be as simple
as adding a new model and exporting it using the [`export_models`
script](./scripts/export_models.py).
You should default to adding your model to the `qc` schema and subdirectory, unless there is
a good reason to define it elsewhere. For details on how to add a model, see
[➕ How to add a new model](#-how-to-add-a-new-model).

There are a number of configuration options that allow you to control the format of your
model during export:

* **Tagging**: [Model tags](https://docs.getdbt.com/reference/resource-configs/tags) are not
  required for QC reports, but they are helpful in cases where we need to export more than one
  report at a time. For example, Valuations typically requests all of the town close QC
  reports at the same time, so we tag each model with the `qc_report_town_close` tag such that
  we can select them all at once when running the `export_models` script.
  For consistency, prefer tags that start with the `qc_report_*` prefix.
* **Filtering**: Since the `export_models` script can filter your model using the `--where`
  option, you should define your model such that it selects any fields that you want to use
  for filtering in the `SELECT` clause. It's common to filter reports by `taxyr` and
  one of either `township_name` or `township_code`.
* **Formatting**: You can set a few different optional configs on the `meta` attribute of
  your model's schema definition in order to control the format of the output workbook:
    * **`meta.export_name`**: The base name that the script will use for the output file, not
      including the file extension. The script will output the file to
      `dbt/export/output/{meta.export_name}.xlsx`. If unset, defaults to the
      name of the model.
    * **`meta.export_template`**: The base name for an Excel file that the script will use as
      a template to populate with data, not including the file extension. The script will
      read this file from `dbt/export/templates/{meta.export_template}.xlsx`.
      Templates are useful if you want to apply custom headers, column widths, or other
      column formatting to the output that are not otherwise configurable
      by the `meta.export_format` config attribute described below. If unset,
      the script will search for a template with the same name as the model; if
      it does not find a template, it will default to a simple layout with filterable
      columns and striped rows.
    *  **`meta.export_format`**: An object with the following schema that controls the
       format of the output workbook:
         * `columns` (required): A list of one or more columns to format, each of which should be
           an object with the following schema:
             * `index` (required): The letter index of the column to be formatted, like `A` or `AB`.
             * `name` (optional): The name of the column as it appears in the header of the workbook.
               The script does not use this attribute and instead uses `index`, but we set it in order
               to make the column config object more readable.
             * `horizontal_align` (optional): The horizontal alignment to set on the column, one of
               `left` or `right`.
             * `number_format` (optional): The number format to apply to the
               column. See the [openpyxl source
               code](https://openpyxl.readthedocs.io/en/stable/_modules/openpyxl/styles/numbers.html)
               for a list of options

#### Example: Adding a new QC report

Here's an example of a model schema definition that sets all of the different optional and required
formatting options for a new QC report:

```yaml
models:
  - name: qc.vw_qc_report_new
    description: '{{ doc("view_vw_qc_report_new") }}'
    config:
      tags:
        - qc_report_new
    meta:
      export_name: QC Report (New)
      export_template: qc_report_new.xslx
      export_format:
        columns:
          - index: B
            name: Percent Change
            horizontal_align: right
            number_format: "0.00%"
```

In the case of this model, the `export_models` script:

* Will export the model if either `--select qc.vw_qc_report_new` or `--select tag:qc_report_new`
  is set
* Will use the template `dbt/export/templates/qc_report_new.xlsx` to populate data
* Will export the output workbook to `dbt/export/output/QC Report (New).xlsx`
* Will right-align column B, a column with the name `Percent Change`
* Will format column B as a percentage with two decimal places

## 🐛 Debugging tips

### How do I debug a failing test?

Most of our dbt tests are simple SQL statements that we run against our
models in order to confirm that models conform to spec. If a test is
failing, you can run or edit the underlying query in order to investigate
the failure and determine whether the root cause is a code change we made,
new data that was pushed to the system of record, or a misunderstanding about
the data specification.

To edit or run the query underlying a test, first run the test in isolation:

```
dbt test --select <test_name>
```

Then, navigate to the [Recent
queries](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor/history)
tab in Athena. Your test will likely be one of the most recent queries; it
will also start with the string `-- /* {"app": "dbt", ...`, which can be
helpful for spotting it in the list of recent queries.

Open the query in the Athena query editor, and edit or run it as necessary
to debug the test failure.

#### A special note on failures related to code changes

To quickly rule out a failure related to a code change, you can switch to the
main branch of this repository (or to an earlier commit where we know tests
passed, if tests are failing on the main branch) and rerun the test against prod
using the `--target prod` option. If the test continues to fail in the same
fashion, then we can be confident that the root cause is the data and not the
code change.

### What do I do if the `cleanup-dbt-resources` workflow fails?

The `cleanup-dbt-resources` workflow removes all AWS resources that were created
by GitHub Actions for a pull request once that pull request has been merged into
the main branch of the repository. On rare occasions, this workflow might fail
due to changes to our dbt setup that invalidate the assumptions of the workflow.

There are two ways to clean up a PR's resources manually:

1. **Using the AWS console**: Login to the AWS console and navigate to the
   Athena homepage. Select `Data sources` in the sidebar. Click on the
   `AwsDataCatalog` resource. In the `Associated databases` table, select each
   data source that matches the database pattern for your pull request
   (i.e. prefixed with `z_ci_` plus the name of your branch) and click the
   `Delete` button in the top right-hand corner of the table.
2. **Using the command-line**: If the workflow has failed, it most likely means
   there is a bug in the `.github/scripts/cleanup_dbt_resources.sh` script
   ([source code](../.github/scripts/cleanup_dbt_resources.sh)). Once you've
   identified and fixed the bug, confirm it works by running the following
   command to clean up the resources created by the pull request:

```
HEAD_REF=$(git branch --show-current) ../.github/scripts/cleanup_dbt_resources.sh ci
```

### What do I do if dbt found two schema.yml entries for the same resource?

If you get this error:

```
Compilation Error
  dbt found two schema.yml entries for the same resource named location.vw_pin10_location. Resources and their associated columns may only be described a single time. To fix this, remove one of the resource entries for location.vw_pin10_location in this file:
   - models/location/schema.yml
```

It usually means that dbt's state has unresolvable conflicts with the current
state of your working directory. To resolve this, run `dbt clean` to clear your
dbt state, reinstall dbt dependencies with `dbt deps`, and then try rerunning
the command that raised the error.

### What do I do if dbt says a schema or table does not exist?

When attempting to build models, you may occasionally run into the following
error indicating that a model that your selected model depends on does not
exist:

```
Runtime Error in model default.vw_card_res_char (models/default/default.vw_card_res_char.sql)
  line 24:10: Table 'awsdatacatalog.z_dev_jecochr_default.vw_pin_land' does not exist
```

The error may look like this if an entire schema is missing:

```
Runtime Error in model default.vw_pin_universe (models/default/default.vw_pin_universe.sql)
  line 130:11: Schema 'z_dev_jecochr_location' does not exist
```

To resolve this error, you can prefix your selected model's names with a plus
sign (`+`) to instruct dbt to (re)build its dependency models as well. However,
note that this will rebuild _all_ dependency models, even ones that already
exist in your development environment, so if your model depends on another model
that is compute-intensive (basically, anything in the `location` or `proximity`
schemas) you should use [the `--exclude`
option](https://docs.getdbt.com/reference/node-selection/exclude) to exclude
these compute-intensive models from being rebuilt:

```
dbt build --select +model.vw_pin_shared_input --exclude location.* proximity.* --resource-types model seed
```

### How do I enable debug logging?

If you'd like to know what dbt is doing under the hood, you can use [the `--log-level`
parameter](https://docs.getdbt.com/reference/global-configs/logs#log-level) to enable debug
logging when running dbt commands:

```
dbt --log-level debug build --select model.vw_pin_shared_input
```
