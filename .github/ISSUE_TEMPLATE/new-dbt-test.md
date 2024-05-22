# New dbt test

## Issue Summary

_Provide a brief description of the test and its motivation._

## Prerequisites

Before proceeding, make sure you've read the [official dbt documentation on data tests](https://docs.getdbt.com/docs/build/data-tests) and [our internal dbt documentation](https://github.com/ccao-data/data-architecture/blob/master/dbt/README.md) so that you know how dbt models and tests work. In particular, you should understand:

* The difference between a generic test and a test that is defined on a model in a `schema.yml` file
* How generic tests are written and extended
* How tests defined on a model in a `schema.yml` file are written and run

If you don't feel like you have a strong understanding of all of these items, ask a senior data scientist for clarification before starting.

## New DBT Tests

Our workflow focuses on creating reusable generic test templates in the `dbt/tests/generic/` directory, which are invoked in the `schema.yaml` file for a given data model.
We can either
- [ ] Utilize an existing generic test
- Modify an existing generic test
- Add a new generic test.

### Utilize Existing Generic Test

We already have a number of generic test templates found in `dbt/tests/generic/`. [Here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql) is an example of a DBT test template.
   - _We use [Jinja](https://jinja.palletsprojects.com/en/3.1.x/templates/) to parameterize the generic test templates._

**Integrate Test with Data Model:**
   - We include the test in the `schema.yaml` file under the `dbt/models/` directory for the specific data model.
   - Example: Here's how the test is implemented for the `default.vw_pin_universe` view:

 ```yaml
    - unique_combination_of_columns:
        name: default_vw_pin_universe_unique_by_14_digit_pin_and_year
        combination_of_columns:
            - pin
            - year
 ```
   - Reference code implementation [here](https://github.com/ccao-data/data-architecture/blob/66ad8159bcb3d96dcdc62b7355f8fbce64affc78/dbt/models/default/schema/default.vw_pin_universe.yml#L248-L252).

### Modify an Existing Generic Test

If an existing generic test almost meets the user's need, it can be modified in order to support a new use case.

### Add a New Test

1. **Check for Existing Tests:** Ensure the test or a similar functionality does not already exist among our generic templates. 

2. **Create Test Template:**
   - Add a new test template in the `dbt/tests/generic/` directory.
   - Example: View an existing DBT test template [here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql).



## Running Tests

Execute the tests against development or production environments using the following commands:

Run your test against development models. Make sure to change the name of the test that is passed to the `--select` flag below (`default_vw_pin_universe_unique_by_14_digit_pin_and_year`) to match the name(s) of the test(s) you want to run:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year
```

Run a test against the prod models:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year --target prod
