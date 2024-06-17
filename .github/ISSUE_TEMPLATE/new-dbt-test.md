# New dbt test

## Issue Summary

_Provide a brief description of the test and its motivation._

## Prerequisites

Before proceeding, make sure you've read the [official dbt documentation on data tests](https://docs.getdbt.com/docs/build/data-tests) and [our internal dbt documentation](https://github.com/ccao-data/data-architecture/blob/master/dbt/README.md) so that you know how dbt models and tests work. In particular, you should understand:

* The difference between a generic test and a test that is defined on a model in a `schema.yml` file
* How generic tests are written and extended
* How tests defined on a model in a `schema.yml` file are written and run

If you don't feel like you have a strong understanding of all of these items, ask a senior data scientist for clarification before starting.

## Types of Tests

There are two types of tests that we might consider for a model:

1. **Data tests** check that our assumptions about our raw data are correct
    * For example: Test that a table is unique by `parid` and `taxyr`
2. **Unit tests** check that transformation logic inside a model definition
   produces the correct output on a specific set of input data
    * For example: Test that an enum column computed by a `CASE... WHEN`
      expression produces the correct output for a given input string

Unit tests are currently in development and until they are natively supported we do not have a way of implementing them.

### Data Tests

There are two types of data tests that we support:

1. **QC tests** confirm our assumptions about iasWorld data and are run at
   scheduled intervals to confirm that iasWorld data meets spec. Generally, these
   are tests invoked on iasworld tables.
3. **Non-QC tests** confirm all other assumptions about data sources outside
   of iasWorld, and are run in an ad hoc fashion depending on the needs of
   the transformations that sit on top of the raw data

If you plan on adding a QC test, additional instructions are [here](https://github.com/ccao-data/data-architecture/blob/master/dbt/README.md#adding-data-tests), along with more details about the distinction between QC and non-QC tests

## Add DBT Tests

Our workflow focuses on creating reusable generic test templates in the `dbt/tests/generic/` directory, which are invoked in the `schema.yaml` file for a given data model.
We can either
- [ ] Utilize an existing generic test
- [ ] Modify an existing generic test
- [ ] Add a new generic test.

Before proceeding, make sure you know whether you're adding a QC or a non-QC test. If you plan to add a QC test, there are a [few specific modifications needed](https://github.com/ccao-data/data-architecture/blob/master/dbt/README.md#adding-qc-tests) 

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

If a generic test does not meet your needs but seems like it could be easily extended to meet your needs (say, if it inner
joins two tables but you would like to be able to configure it to left join those tables instead) you can modify the macro
that defines the generic test as part of your PR to make the change that you need.

### Add a New Test

1. **Check for Existing Tests:** Ensure the test or a similar functionality does not already exist among our generic templates. 

2. **Create Test Template:** If an existing generic test cannot be used nor extended to meet the user's needs, a new generic test can be defined in `dbt/test/generics/`. [Example here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql). Make sure to read the docs on [writing custom generic tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests). We'll also need to follow a few extra steps:
   1. Add a default category for your generic test in
        the `TEST_CATEGORIES` constant in the [`transform_dbt_test_results`
        script](https://github.com/ccao-data/data-architecture/blob/master/.github/scripts/transform_dbt_test_results.py)
   2. Make sure that your generic test supports the `additional_select_columns`
        parameter that most of our generic tests support, making use
        of the `format_additional_select_columns` macro to format the
        parameter when applying it to your `SELECT` condition




## Running Tests

Execute the tests against development or production environments using the following commands:

Run your test against development models. Make sure to change the name of the test that is passed to the `--select` flag below (`default_vw_pin_universe_unique_by_14_digit_pin_and_year`) to match the name(s) of the test(s) you want to run:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year
```

Typically, you'll want to be running against development models. However, in the event you want to run a test against the prod models, you can specify the ` --target prod` flag:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year --target prod
```

