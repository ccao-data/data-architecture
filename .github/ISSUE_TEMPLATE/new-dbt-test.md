# New DBT Test

_(Brief description of the test and the motivation of the test here)_

## Short checklist

- [ ] Create test in the `dbt/tests/generic/` directory
- [ ] Add test to the `schema.yaml` file for a data model in `dbt/models/`

## Test Example

An example of a DBT test can be seen [here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql).
And this is an example of an implementation of this test on a specific data model, in this case the `default.vw_pin_universe` view.

```yaml
    - unique_combination_of_columns:
        name: default_vw_pin_universe_unique_by_14_digit_pin_and_year
        combination_of_columns:
            - pin
            - year
```

_Link to code [here](https://github.com/ccao-data/data-architecture/blob/66ad8159bcb3d96dcdc62b7355f8fbce64affc78/dbt/models/default/schema/default.vw_pin_universe.yml#L248-L252)_

### Run tests

A test can be ran against development or production resources with the following bash commands.

Run only one test:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year
```

Run a test against the prod models:

```bash
dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year --target prod
```
