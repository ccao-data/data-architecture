---
author: Jean Cochrane
date: MMMM dd, YYYY
---

# Tech talk: dbt

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

---

# What _isn't_ dbt?

There are a few important parts of our data catalog stack that use tools other
than dbt:

* Task orchestration
* Continuous integration and deployment
* Monitoring builds and tests
* Raw data extraction
* Publishing data to platforms like Socrata and Tableau

---

# Repo overview

https://github.com/ccao-data/data-architecture

---

# Demo time!

Make sure we have no resources built yet:

```
../.github/scripts/cleanup_dbt_resources.sh dev
```

---

# Demo: dbt run

Let's try building a model:

```
dbt run --select default.vw_pin_universe
```

---

# Demo: dbt run

Oops, that didn't work!

Add a plus sign to build a view including its dependencies:

```
dbt run --select +default.vw_pin_universe
```

---

# Demo: dbt run

You can also defer to remote state to avoid building things locally.

Start by deleting the dependency database, so that we have to rebuild it:

```
aws glue delete-database --name dev_jecochr_location
```

---

# Demo: dbt run

Now try downloading and deferring to remote state:

```
aws s3 cp s3://ccao-dbt-cache-us-east-1/master-cache/manifest.json state/manifest.json && \
dbt run --select default.vw_pin_universe --defer --state state
```

---

# Demo: dbt run

We can also instruct dbt to only build models that are new or have changed.

This won't build any models:

```
dbt run -s state:modified state:new --defer --state state
```

---

# Demo: dbt run

But if we edit `default.vw_pin_universe`, it will get rebuilt!

Let's rerun that previous command:

```
dbt run -s state:modified state:new --defer --state state
```

---

# Demo: dbt test

We can also run tests using dbt.

Run all tests for a model:

```
dbt test --select default.vw_pin_universe
```

---

# Demo: dbt test

Run tests on prod:

```
dbt test --select default.vw_pin_universe --target prod
```

---

# Demo: dbt docs

Lastly, dbt lets us build our data docs:

```
dbt docs generate --target prod
```
---

# Demo: dbt docs

Docs are built into a static website, but we can also use this convenience
function to serve them locally:

```
dbt docs serve
```

---

# Automation, orchestration, and monitoring

What do these terms mean?

* **Automation**: Running builds and tests automatically
* **Orchestration**: Running builds and tests on schedule and in order
* **Monitoring**: Making sure we get notified if builds and tests fail

We use **GitHub Actions** to automate, orchestrate, and monitor dbt.

Let's take a look:

https://github.com/ccao-data/data-architecture/actions

---

# Questions?
