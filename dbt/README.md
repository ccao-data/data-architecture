# dbt

This directory stores the configuration for building our data catalog using
[dbt](https://docs.getdbt.com/docs/core).

## Installation

These instructions are for Ubuntu, which is the only platform we've tested.

For background, see the docs on [installing dbt with
pip](https://docs.getdbt.com/docs/core/pip-install).

### Requirements

* Python3 with venv installed (`sudo apt install python3-venv`)
* [AWS CLI installed
  locally](https://github.com/ccao-data/wiki/blob/master/How-To/Connect-to-AWS-Resources.md)

### Install dependencies

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd athena
dbt deps
```

## Usage

All dbt commands should be run in the project directory:

```
cd athena
```

Build the models to create views in our Athena warehouse:

```
dbt run
```

Generate the documentation:

```
dbt docs generate
```

This will create a new file `target/index.html` representing the static
docs website.

You can also serve the docs locally:

```
dbt docs serve
```

Run the tests:

```
dbt test
```
