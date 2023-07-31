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

Make sure you have the virtual environment activated:

```
source venv/bin/activate
```

Authenticate with AWS MFA if you haven't already today:

```
aws-mfa
```

Build the models to create views in our Athena warehouse:

```
dbt run
```

By default, all `dbt` commands will run against the `dev` environment, which
namespaces the resources it creates by prefixing target database names with
your Unix `$USER` name (e.g. `jecochr-default` for the `default` database when
`dbt` is run on Jean's machine). To instead **run commands against prod**,
use the `--target` flag:

```
dbt run --target prod
```

Generate the documentation:

```
dbt docs generate
```

This will create a new file `target/index.html` representing the static
docs site.

You can also serve the docs locally:

```
dbt docs serve
```

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
