# New DBT Test

## Issue Summary

_(Brief description of the test and the motivation of the test here)_

## Checklist

Before going through this checklist, make sure to have a basic understanding of DBT tests,
the [official documentation](https://docs.getdbt.com/docs/build/data-tests) is a great place to start.

Our workflow prioritizes generalizable tests, so we create re-usable generic test templates in the `dbt/tests/generic/` directory which are then
invoked in the `schema.yaml` file for a given data model. Before adding a test template, check to make sure it doesn't already exist as a generic test template, or than
an existing generic test template can't be modified to meet the needed functionality.

- [ ] Create test template in the `dbt/tests/generic/` directory  

An example of a DBT test can be seen [here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql).
We use [Jinja](https://jinja.palletsprojects.com/en/3.1.x/templates/) to make the generic test templates able to be parametrized

- [ ] Add test to the `schema.yaml` file for a data model in `dbt/models/`  

And this is an example of an implementation of this test on a specific data model, in this case the `default.vw_pin_universe` view.

```yaml
    - unique_combination_of_columns:
        name: default_vw_pin_universe_unique_by_14_digit_pin_and_year
        combination_of_columns:
            - pin
            - year
```

_Link to code [here](https://github.com/ccao-data/data-architecture/blob/66ad8159bcb3d96dcdc62b7355f8fbce64affc78/dbt/models/default/schema/default.vw_pin_universe.yml#L248-L252)_

## Run tests

A test can be ran against development or production resources with the following bash commands.

Run only one test:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year
```

Run a test against the prod models:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year --target prod
```
