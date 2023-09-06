# dbt

This directory stores the configuration for building our data catalog using
[dbt](https://docs.getdbt.com/docs/core).

## Quick links

### In this document

* [🖼️ Background: What does the data catalog
  do?](background-what-does-the-data-catalog-do)
* [💻 How to develop the catalog](#how-to-develop-the-catalog)
* [➕ How to add a new model](#how-to-add-a-new-model)
* [🐛 Debugging tips](#debugging-tips)

### Outside this document

* [📖 Data documentation](https://ccao-data.github.io/data-architecture)
* [📝 Design doc for our decision to develop our catalog with
  dbt](../documentation/design-docs/data-catalog.md)

<h2 id="background-what-does-the-data-catalog-do">🖼️ Background: What does the data catalog do?</h2>

The data catalog accomplishes a few main goals:

### 1. Store our data transformations as a directed acyclic graph (DAG)

The models defined in [`models/`](./models/) represent the SQL operations that
we run in order to transform raw data into output data for use in statistical
modeling and reporting. These transformations comprise a DAG that we use for
documenting our data and rebuilding it efficiently. We use the [`dbt
run`](https://docs.getdbt.com/reference/commands/run) command to build these
models into views and tables in AWS Athena.

Note that when we talk about "models" in these docs, we generally mean [the
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

<h2 id="how-to-develop-the-catalog"> 💻 How to develop the catalog </h2>

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

#### Install dependencies

Run the following commands in this directory:

```
python3 -m venv venv
source venv/bin/activate
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

Build all the tables and views in our Athena warehouse:

```
dbt run
```

By default, all `dbt` commands will run against the `dev` environment, which
namespaces the resources it creates by prefixing target database names with
your Unix `$USER` name (e.g. `dev_jecochr_default` for the `default` database
when `dbt` is run on Jean's machine). To instead **run commands against prod**,
use the `--target` flag:

```
dbt run --target prod
```

To build a subset of the models, use the `--select` option:

```
dbt run --select location.vw_pin10_location default.vw_pin_universe
```

Prefix a model's names with a plus sign (`+`) to instruct dbt to (re)build its
parents as well:

```
dbt run --select +default.vw_pin_universe
```

Download the prod state and use the `--defer` and `--state` options in order to
build a model without having to build its dependencies (for more details, see
[the docs on deferral](https://docs.getdbt.com/reference/node-selection/defer)):

```
aws s3 cp s3://ccao-dbt-cache-us-east-1/master-cache/manifest.json state/manifest.json
dbt run --select default.vw_pin_universe --defer --state state
```

After downloading the prod state, only build models that have been added or
changed since the last prod run:

```
dbt run -s state:modified state:new --defer --state state
```

Delete all the resources created in your local environment:

```
../.github/scripts/cleanup_dbt_resources.sh dev
```

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
dbt test --select vw_pin_universe_unique_by_14_digit_pin_and_year
```

Run a test against the prod models:

```
dbt test --select vw_pin_universe_unique_by_14_digit_pin_and_year --target prod
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

<h3 id="how-to-add-a-new-model"> ➕ How to add a new model </h3>

To request the addition of a new model, open an issue using the [Add a new dbt
model](.github/ISSUE_TEMPLATE/new-dbt-model.md) issue template. The assignee
should follow the checklist in the body of the issue in order to add the model
to the DAG.

There are a few subtleties to consider when requesting a new model, outlined
below.

#### Model materialization

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

#### Model naming

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

#### Model description

All new models should include, at minimum, a
[description](https://docs.getdbt.com/reference/resource-properties/description)
of the model itself. We generally store these descriptions as [docs
blocks](https://docs.getdbt.com/reference/resource-properties/description#use-a-docs-block-in-a-description).

Ideally, new models should also include descriptions for each column,
implemented directly in the `schema.yml` model definition. However, our
column descriptions are currently sparse, so it is not a strict requirement just
yet.

#### Model tests

Any assumptions underlying the new model should be documented in the form of
[dbt tests](https://docs.getdbt.com/docs/build/tests). We prefer adding tests
inline in `schema.yml` model properties files, as opposed to defining one-off
tests in the `tests/` directory.

Conceptually, there are two types of tests that we might consider for a new
model:

1. **Data tests** check that the assumptions that a model makes about the raw
   data it is transforming are correct.
    * For example: Test that the table is unique by `pin10` and `year`.
2. **Unit tests** check that the transformation logic itself produces
   the correct output on a hardcoded set of input data.
    * For example: Test that an enum column computed by a `CASE... WHEN`
      expression produces the correct enum output for a given input string.

The dbt syntax does not distinguish between data and unit tests, but it has
emerged as a valuable distinction that we make on our side. Data tests are
generally much easier to define and to implement, since they operate directly on
source data and do not require hardcoded input and output values to execute.
Due to this complexity, we currently do not have a way of supporting unit
tests, although we plan to revisit this in the future; as such, when proposing
new tests, check to ensure that they are in fact data tests and not unit tests.

<h2 id="debugging-tips"> 🐛 Debugging tips </h2>

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
   (i.e. prefixed with `ci_` plus the name of your branch) and click the
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
