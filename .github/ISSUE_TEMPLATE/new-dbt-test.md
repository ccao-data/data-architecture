# New dbt test

## Issue Summary

_Provide a brief description of the test and its motivation._

## Prerequisites

Before proceeding, make sure you've read the [official dbt documentation on data tests](https://docs.getdbt.com/docs/build/data-tests) and [our internal dbt documentation](https://github.com/ccao-data/data-architecture/blob/master/dbt/README.md) so that you know how dbt models and tests work. In particular, you should understand:

* The difference between a generic test and a test that is defined on a model in a `schema.yml` file
* How generic tests are written and extended
* How tests defined on a model in a `schema.yml` file are written and run
* The difference between a QC-test and a non-QC test

If you don't feel like you have a strong understanding of all of these items, ask a senior data scientist for clarification before starting.


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
   - Example: Here's how the test is implemented for the `iasworld.dweldat` table. This test checks to make sure there aren't null values in the `bsmt` column.

 ```yaml
 - name: bsmt
   description: '{{ doc("shared_column_char_bsmt") }}'
   data_tests:
        - not_null:
            name: iasworld_dweldat_bsmt_not_null
            additional_select_columns: *select-columns
            config: *unique-conditions
            meta:
              description: bsmt (Basement Type) should not be null
 ```
   - Reference code implementation [here](https://github.com/ccao-data/data-architecture/blob/bce7ae1f78c9858a937192ab009c4f873f671917/dbt/models/iasworld/schema/iasworld.dweldat.yml#L93)

### Modify an Existing Generic Test

If a generic test does not meet your needs but seems like it could be easily extended to meet your needs (say, if it inner
joins two tables but you would like to be able to configure it to left join those tables instead) you can modify the macro
that defines the generic test as part of your PR to make the change that you need.

### Add a New Generic Test

1. **Check for Existing Tests:** Ensure the test or a similar functionality does not already exist among our generic templates. 

2. **Create Test Template:** If an existing generic test cannot be used nor extended to meet the user's needs, a new generic test can be defined in `dbt/test/generics/`. [Example here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql). Make sure to read the docs on [writing custom generic tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests). We'll also need to follow [a few extra steps](https://github.com/ccao-data/data-architecture/tree/master/dbt#choosing-a-generic-test)





## Running Tests

When developing, we generally want to run against development resources, although it is possible to run them against prod resources. Execute the tests against development or production environments using the following commands:

Run your test against development models. Make sure to change the name of the test that is passed to the `--select` flag below (`default_vw_pin_universe_unique_by_14_digit_pin_and_year`) to match the name(s) of the test(s) you want to run:

```bash
dbt test --select iasworld_dweldat_bsmt_not_null
```

Typically, you'll want to be running against development models. However, in the event you want to run a test against the prod models, you can specify the ` --target prod` flag:

```bash
dbt test --select diasworld_dweldat_bsmt_not_null --target prod
```

