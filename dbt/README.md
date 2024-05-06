# dbt

This directory stores the configuration for building our data catalog using
[dbt](https://docs.getdbt.com/docs/core).

## Quick links

### In this document

* [🖼️ Background: What does the data catalog do?](#%EF%B8%8F-background-what-does-the-data-catalog-do)
* [🔨 How to rebuild models using GitHub Actions](#-how-to-rebuild-models-using-github-actions)
* [💻 How to develop the catalog](#-how-to-develop-the-catalog)
* [➕ How to add a new model](#-how-to-add-a-new-model)
* [📝 How to add and run tests](#-how-to-add-and-run-tests)
* [🐛 Debugging tips](#-debugging-tips)

### Outside this document

* [📖 Data documentation](https://ccao-data.github.io/data-architecture)
* [📝 Design doc for our decision to develop our catalog with
  dbt](../documentation/design-docs/data-catalog.md)

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

The tests defined in the `schema.yml` files in the `models/` directory,
alongside some one-off tests defined in the [`tests/`](./tests/) directory,
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

## 💻 How to develop the catalog

### Installation

These instructions are for Ubuntu, which is the only platform we've tested.

For background, see the docs on [installing dbt with
pip](https://docs.getdbt.com/docs/core/pip-install). (You don't need to
follow these docs in order to install dbt; the steps that follow will take
care of that for you.)

#### Requirements

* Python3 with venv installed (`sudo apt install python3-venv`)
* [AWS CLI installed
  locally](https://github.com/ccao-data/wiki/blob/master/How-To/Connect-to-AWS-Resources.md)
  * You'll also need permissions for Athena, Glue, and S3
* [`aws-mfa` installed locally](https://github.com/ccao-data/wiki/blob/master/How-To/Setup-the-AWS-Command-Line-Interface-and-Multi-factor-Authentication.md)

#### Install dependencies

Run the following commands in this directory:

```
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
dbt deps
```

### Useful commands

To run dbt commands, make sure you have the virtual environment activated:

```
source venv/bin/activate
```

You must also authenticate with AWS using MFA if you haven't already today:

```
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

```
aws s3 cp s3://ccao-dbt-cache-us-east-1/master-cache/manifest.json master-cache/manifest.json
```

Then, use [`dbt clone`](https://docs.getdbt.com/reference/commands/clone) to
clone the production tables and views into your development environment:

```
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

#### Build tables and views in production

By default, all `dbt` commands will run against the `dev` environment (called
a [target](https://docs.getdbt.com/reference/dbt-jinja-functions/target) in
dbt jargon), which namespaces the resources it creates by prefixing database
names with `z_dev_` and your Unix `$USER` name.

You should almost never have to manually build tables and views in our
production environment, since this repository is configured to automatically
deploy production models using GitHub Actions for continuous integration.
However, in the rare case that you need to manually build models in production,
use the `--target` option:

```
dbt build --target prod --resource-types model seed
```

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
testing, see [📝 How to add and run tests](#-how-to-add-and-run-tests).

## 📝 How to add and run tests

We test our data and our transformations using [dbt
tests](https://docs.getdbt.com/docs/build/tests). We prefer adding tests
inline in `schema.yml` config files using [generic
tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests),
rather than [singular
tests](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests).

### Data tests vs. unit tests

There are two types of tests that we might consider for a model:

1. **Data tests** check that our assumptions about our raw data are correct
    * For example: Test that a table is unique by `parid` and `taxyr`
2. **Unit tests** check that transformation logic inside a model definition
   produces the correct output on a specific set of input data
    * For example: Test that an enum column computed by a `CASE... WHEN`
      expression produces the correct output for a given input string

dbt tests are data tests by default, although a dedicated unit testing syntax
[is coming soon](https://docs.getdbt.com/docs/build/unit-tests). Until unit
tests are natively supported, however, we do not have a way of implementing them.
We plan to change this once unit testing is released, but for now, make sure that
any new tests you write are data tests and not unit tests.

### Adding data tests

There are two types of data tests that we support:

1. **QC tests** confirm our assumptions about iasWorld data and are run at
   scheduled intervals to confirm that iasWorld data meets spec
2. **Non-QC tests** confirm all other assumptions about data sources outside
   of iasWorld, and are run in an ad hoc fashion depending on the needs of
   the transformations that sit on top of the raw data

#### Adding QC tests

QC tests are run on a schedule by the [`test-dbt-models`
workflow](https://github.com/ccao-data/data-architecture/actions/workflows/test_dbt_models.yaml)
and their output is interpreted by the [`transform_dbt_test_results`
script](https://github.com/ccao-data/data-architecture/blob/master/.github/scripts/transform_dbt_test_results.py).
This script reads the metadata for a test run and outputs an Excel
workbook with detailed information on each failure to aid in resolving
any data problems that the tests reveal.

There are a few specific modifications a test author needs to make to
ensure that QC tests can be run by the workflow and interpreted by the script:

* One of either the test or the model that the test is defined on must be
[tagged](https://docs.getdbt.com/reference/resource-configs/tags) with
the tag `test_qc_iasworld`
  * Prefer tagging the model, and fall back to tagging the test if for
    some reason the model cannot be tagged (e.g. if it has some non-QC
    tests defined on it)
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
    set by the `test_qc_year_start` and `test_qc_year_end`
    [project variables](https://docs.getdbt.com/docs/build/project-variables)
  * `meta` should be set with a few specific string attributes:
    * `description` (required): A short human-readable description of the test
    * `category` (optional): A workbook category for the test, required if
      a category is not defined for the test's generic in the `TEST_CATEGORIES`
      constant in the [`transform_dbt_test_results`
      script](https://github.com/ccao-data/data-architecture/blob/master/.github/scripts/transform_dbt_test_results.py)
    * `table_name` (optional): The name of the table to report in the output
      workbook, if the workbook should report a different table name than the
      name of the model that the test is defined on

See the [`iasworld_pardat_class_in_ccao_class_dict`
test](https://github.com/ccao-data/data-architecture/blob/bd4bc1769fe33fdba1dbe827791b5c41389cf6ec/dbt/models/iasworld/schema/iasworld.pardat.yml#L78-L96)
for an example of a test that sets these attributes.

Due to the similarity of parameters defined on QC tests, we make extensive use
of YAML anchors and aliases to define symbols for commonly-used values.
See [here](https://support.atlassian.com/bitbucket-cloud/docs/yaml-anchors/)
for a brief explanation of the YAML anchor and alias syntax.

#### Adding non-QC tests 

QC tests are much more common than non-QC tests in our test suite. If you
are being asked to add a test that appears to be a non-QC test, double
check with the person who assigned the test to you and ask them when
and how the test should be run so that its attributes can be set
accordingly.

### Choosing a generic test

Writing a test in a `schema.yml` file requires a [generic
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
        the `TEST_CATEGORIES` constant in the [`transform_dbt_test_results`
        script](https://github.com/ccao-data/data-architecture/blob/master/.github/scripts/transform_dbt_test_results.py)
     2. Make sure that your generic test supports the `additional_select_columns`
        parameter that most of our generic tests support, making use
        of the `format_additional_select_columns` macro to format the
        parameter when applying it to your `SELECT` condition

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
