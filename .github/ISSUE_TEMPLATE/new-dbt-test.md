# New DBT Test

## Issue Summary

_Provide a brief description of the test and its motivation._

## Prerequisites

Before proceeding, ensure you have a basic understanding of DBT tests. Review the [official DBT documentation on data tests](https://docs.getdbt.com/docs/build/data-tests) for foundational knowledge.

## Checklist

Our workflow focuses on creating reusable generic test templates in the `dbt/tests/generic/` directory, which are invoked in the `schema.yaml` file for a given data model. Follow these steps to add a new test:

1. **Check for Existing Tests:** Ensure the test or a similar functionality does not already exist among our generic templates. Consider modifying an existing template if it closely aligns with the required functionality.

2. **Create Test Template:**
   - [ ] Add a new test template in the `dbt/tests/generic/` directory.
   - Example: View an existing DBT test template [here](https://github.com/ccao-data/data-architecture/blob/master/dbt/tests/generic/test_unique_combination_of_columns.sql).
   - Use [Jinja](https://jinja.palletsprojects.com/en/3.1.x/templates/) to parameterize the generic test templates.

3. **Integrate Test with Data Model:**
   - [ ] Include the new test in the `schema.yaml` file under the `dbt/models/` directory for the specific data model.
   - Example: Here's how the test is implemented for the `default.vw_pin_universe` view:

    ```yaml
    - unique_combination_of_columns:
        name: default_vw_pin_universe_unique_by_14_digit_pin_and_year
        combination_of_columns:
            - pin
            - year
    ```

   - Reference code implementation [here](https://github.com/ccao-data/data-architecture/blob/66ad8159bcb3d96dcdc62b7355f8fbce64affc78/dbt/models/default/schema/default.vw_pin_universe.yml#L248-L252).

## Running Tests

Execute the tests against development or production environments using the following commands:

- **Run a specific test:**

  ```bash
  dbt test --select default_vw_pin_universe_unique_by_14_digit_pin_and_year
