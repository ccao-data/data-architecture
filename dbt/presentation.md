# Tech talk: dbt

Jean Cochrane | September 2023

---

# Data architecture refresher

https://ccao-data.github.io/data-architecture

---

# What is dbt?

dbt stands for "**D**ata **B**uild **T**ool". It's a tool for building data!

Things we do with dbt include:

* Define and run a Directed Acyclic Graph (DAG) to transform our data and store
  the results as AWS Athena views and tables
* Define and run tests to ensure our data meet spec
* Generate documentation for our data warehouse

We will discuss each of these in more detail in the coming slides.

---

# What _isn't_ dbt?

There are a few important parts of our data catalog stack that use tools other
than dbt:

* Task orchestration
* Continuous integration and deployment
* Monitoring builds and tests
* Raw data extraction
* Publishing data to platforms like Socrata and Tableau

We will discuss the tools we use to accomplish these tasks shortly.

---

# Demo time!

---

# Demo outline

* Show `data-architecture/dbt`
  * Explain each subfolder
    1. `models`
      * `spatial`: Sources
      * `default`: Models (views)
    2. `macros`
    3. `tests`
    4. `target`
    4. YAML files
      * `dbt_project.yml`
      * `packages.yml`
      * `profiles.yml`

* Make sure we have no resources built yet

```
../.github/scripts/cleanup_dbt_resources.sh dev
```

* Confirm we can't build the target without dependencies

```
dbt run --select default.vw_pin_universe
```

* Build a view and its dependencies

```
dbt run --select +default.vw_pin_universe
```

* Navigate to Athena console and confirm the views exist

* Test the view

```
dbt test --select default.vw_pin_universe
```

* Test the view on prod

```
dbt test --select default.vw_pin_universe --target prod
```

* Delete the dependencies

```
aws glue delete-database --name dev_jecochr_location
```

* Download and defer to remote state

```
aws s3 cp s3://ccao-dbt-cache-us-east-1/master-cache/manifest.json state/manifest.json
dbt run --select default.vw_pin_universe --defer --state state
```

* Navigate to Athena console and confirm that `default.vw_pin_universe` points
  to the prod model

* Only build new and changed state; confirm that this doesn't do anything

```
dbt run -s state:modified state:new --defer --state state
```

* Edit `default.vw_pin_universe`

* Rerun above command to confirm it works this time

* Build docs

```
dbt docs generate --target prod
```

* Serve docs

```
dbt docs serve
```
