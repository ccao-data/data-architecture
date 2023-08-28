# dbt

This directory stores the configuration for building our data catalog using
[dbt](https://docs.getdbt.com/docs/core).

## Installation

These instructions are for Ubuntu, which is the only platform we've tested.

For background, see the docs on [installing dbt with
pip](https://docs.getdbt.com/docs/core/pip-install). (You don't need to
follow these docs in order to install dbt; the steps that follow will take
care of that for you.)

### Requirements

* Python3 with venv installed (`sudo apt install python3-venv`)
* [AWS CLI installed
  locally](https://github.com/ccao-data/wiki/blob/master/How-To/Connect-to-AWS-Resources.md)
  * You'll also need permissions for Athena, Glue, and S3

### Install dependencies

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
dbt deps
```

## Usage

To run dbt commands, make sure you have the virtual environment activated:

```
source venv/bin/activate
```

You must also authenticate with AWS using MFA if you haven't already today:

```
aws-mfa
```

### Build tables and views

Build the models to create tables and views in our Athena warehouse:

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

### Generate documentation

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
  ./node_modules/.bin/mmdc -i "$file" -o "${file/.mmd/.png}"
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

### Run tests

Run the tests:

```
dbt test
```

Run tests for only one model:

```
dbt test --select <model_name>
```

Run tests for dbt macros:

```
dbt run-operation test_all
```

#### Debugging dbt test failures

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
passed, if tests are failing on the main branch) and rerun the test. If the
test continues to fail in the same fashion, then we can be confident that the
root cause is the data and not the code change.
