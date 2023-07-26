# Design doc: Data catalog

This doc describes the design of a data catalog system for improving our
data documentation, quality, and orchestration.

## Motivation

From the [Architecture Upgrades 2023 project
README](https://github.com/orgs/ccao-data/projects/11/views/1?pane=info):

> The Data Department has developed data infrastructure to facilitate its core
> functions: modeling, reporting, publishing open data, and research. This
> infrastructure uses the system-of-record (iasWorld) as a "base" dataset, then
> adds third-party, collected, and generated data.
>
> The data mostly flows in a single direction: it is extracted from the
> system-of-record to S3, cleaned and transformed via Athena, then pushed to
> modeling, open data, etc. The only outputs that currently go back into the
> system-of-record are predicted values from the residential and condominium
> models.
>
> This approach is simple and has worked well for us, but is difficult to expand,
> maintain, and trust. Our data stack is essentially a big one-way funnel that
> feeds the models; it wasn't designed with more complex applications in mind.
> As the Data Department grows, so too will the needs from our infrastructure.

This document describes the design of a data catalog system that can resolve the
problems with our current data infrastructure described in the README, problems
like:

* Referential integrity issues
* Schema inconsistency
* Lack of documentation around existing data
* Orchestration issues (timing, automating data creation)
* Complex joins and views
* Lack of data validation and alerting for iasWorld and third-party data

For a full list of issues we intend to solve with the catalog, see the
[requirements](#requirements).

## Requirements

Each of the following requirements has two components: A motivation ("why do
do we want to do this?") plus a list of requirements ("how will we do this?").

### Referential integrity

#### Motivation

From the [project
README](https://github.com/orgs/ccao-data/projects/11?pane=info):

> [Some tables within iasWorld] often contain duplicate or non-active
> records. We have to do a fairly large amount of deduping and filtering (in
> Athena) to get the "correct" records out of iasWorld.

#### Requirements

We should be able to:

* Define keys on top of unconstrained data
* Allow for deduplication and filtering of source data

### Schema consistency

#### Motivation

From the project README:

> The rollout of iasWorld has been piece-wise. Not all data from the
> different agencies involved with the property tax process has been migrated
> yet. Even within the CCAO, certain data is essentially backlogged, i.e. it
> needs to be cleaned, QC'd, and added to the system-of-record at some
> point in the future.
>
> Because of this piece-wise rollout, some tables and fields change over time,
> meaning we need to create queries that likewise change over time.

#### Requirements

We should be able to:

* Track changes to schemas over time
* Track changes to queries in response to changing schemas

### Documentation

#### Motivation

From the project README:

> Due to the complexity and general unreliability of our "base" data set, the
> Data Department has created views and scripts for cleaning raw iasWorld data.
> These views work well but are quite complicated. They codify a lot of
> hard-earned domain knowledge about how the backend system actually works and
> perform a lot of data cleaning [...].

#### Requirements

We should be able to:

* Clearly document all of our data sources, transformations, and products, including:
  * ETL operations
  * Databases, tables, views, schemas
  * Manual processes

### Orchestration and automation

#### Motivation

From the project README:

> Most of our third-party data is created and uploaded via simple R scripts.
> This works well enough since most data needs to be gathered only once per
> year. However, a better system for gathering and uploading third-party data
> would be ideal.

#### Requirements

We should be able to:

* Build and update our data warehouse automatically
* Write scripts for loading third-party data and run them as part of the build
* Define dependencies between data sources in the form of a DAG
* Schedule time-sensitive operations, e.g. wait for current parcel shapefiles to be updated before joining to third-party data
  * This should also be clearly documented

### Simpler joins and views

#### Motivation

From the project README:

> In addition to the first-party backend data, the Data Department also attaches
> a plethora of third-party data that we use for modeling and reporting. This
> includes things like spatial data (neighborhood, census tract) and economic
> data (median income).
>
> Joining this third-party data to the primary records can be complex, and
> currently involves running many CREATE TABLE AS statements. There's lots of
> room for error in this process.

#### Requirements

We should be able to:

* Separate intermediate views from final views
* Factor out common logic in views for more clarity and better modularization
* Provide clear provenance for views in published documentation
* Document specific choices in our queries

### Data validation

#### Motivation

See "Data validation issues" in the project README.

#### Requirements

We should be able to:

* Add layers of validation on top of untyped varchar fields
* Automatically flag mistakes introduced by data entry

### Automated flagging

#### Motivation

See "Lack of automated flagging" in the project README.

#### Requirements

We should be able to:

* Notify teams automatically of potential data integrity issues
* Define a process to push output from our data catalog to the system of record

### Monitoring

#### Motivation

From the project README:

> [People] ask things like "Is PROCNAME updated for this township?" or
> "Did sqoop pull the latest data last night?" We use CloudWatch for extremely
> basic logging and alerts (on failures), but we don't have a system to provide
> visibility into what data is recently updated.

#### Requirements

We should be able to:

* Know when data has been updated
* See results of ETL jobs and debug them in cases of failure
* Notify ourselves of job failures or warnings

### Athena integration

#### Motivation

Our data stack is currently built on top of [AWS
Athena](https://aws.amazon.com/athena/), in particular all of our views on
top of iasWorld. Whatever tool we use for our catalog should work with Athena,
or else we would need to rewrite all of our existing view code.

#### Requirements

We should be able to:

* Define and document views in AWS Athena

## Design

After a decent amount of research and experimentation, we propose to build our
data catalog and data integrity checks in **dbt**.

dbt is a "Data Build Tool" that allows users to define and document ETL
workflows using common software engineering patterns like version control
and modularization. For a longer introduction, see [the
docs](https://docs.getdbt.com/docs/introduction).

For a fun take on what dbt is through the lens of what it is _not_, see
[this blog post](https://stkbailey.substack.com/p/what-exactly-isnt-dbt).

There are two distinct tools that are both somewhat confusingly referred to as
"dbt":

1. **dbt Cloud**: The SaaS version of dbt, including a web view with CI/CD, orchestration, monitoring, RBAC, and an IDE
  * **Pros**: Provides a full-featured, hosted data catalog service
  * **Cons**: Super expensive (minimum $100/seat/mo); owned and operated by a VC-funded startup
2. **dbt Core**: Open source CLI that dbt Cloud is built on top of
  * **Pros**: Free and open source; users have full control and ownership
  * **Cons**: Basically only handles DAG builds (equivalent to e.g. CMake); orchestration/monitoring has to be handled by another tool

dbt Cloud might save us time in the short run, but the process to get it
approved and procured may take a number of months. It also seems risky to
build our catalog on top of a VC-funded service, and the
orchestration/monitoring tools it provides on top of the Core features
are not high priority for us.
As such, we think it would be  more prudent for us to build with dbt Core
and design our own orchestration/monitoring/authentication integrations on top.
Hence, when this doc refers to "dbt", we are actually referring to dbt Core.

The downside of this choice is that we would have to choose a separate tool for
orchestrating and monitoring our DAGs if we move forward with dbt. This is an
important fact to note in our decision, because [orchestrators are notoriously
controversial](https://stkbailey.substack.com/p/what-exactly-isnt-dbt):

> Every other application, besides dbt, is an orchestrator. Every application
> pays the price for it. Every person hates every application as soon as it
> starts orchestrating. To orchestrate is to grapple with reality, to embrace
> mortality, to welcome death.

As such, we evaluate this choice with an eye towards options for third-party
orchestration and monitoring.

### Demo

See the
[`jeancochrane/dbt-spike`](https://github.com/ccao-data/data-architecture/tree/jeancochrane/dbt-spike/dbt)
branch of this repo for an example of a simple dbt setup using our views.

### Requirements matrix

| Requirement | dbt | Notes |
| ----------- | --- | ----- |
| Referential integrity    | ✅ | Via tests. |
| Schema consistency       | ✅ | Via tests. |
| Documentation            | ✅ | Via docs generator. |
| Orchestration/automation | ❌  | Requires dbt Cloud, or integration with another service. |
| Simpler joins/views      | 🟡 | Can't simplify views, but can help factor them out and document them better. |
| Data validation          | ✅ | Via tests. |
| Automated flagging       | ❌  | Requires dbt Cloud, or integration with another service. |
| Monitoring               | ❌  | Requires dbt Cloud, or integration with another service. |
| Athena integration       | ✅ | Via `dbt-athena-community` plugin. |

After some discussion, we consider the lack of orchestration, automation,
automated flagging, and monitoring to be acceptable. The only mission critical
pieces are are flagging and monitoring, and we provide some thoughts on how
to accomplish those goals using external services in the [Design](#design)
section below.

### Design

Here is a sketch of how we plan to implement a workflow for cataloging
and validating our data using dbt:

#### Code organization

* Our dbt configuration will be stored in a `dbt/` folder in the
  `data-architecture` repo.
* View definitions will be stored in a `models/` subdirectory, with
  nested subdirectories for each schema (e.g. `default/`, `location/`).
* Tests will be defined in a `schema.yml` file, and docs will be defined
  as doc blocks in a `docs.md` file.
    * It may end up being wise to split these files up by schema.
* We will have two profiles, one for `dev` and one for `prod`, which will point
  to two separate AWS data catalogs.
    * We may want to eventually namespace these by users, but we should push off
      that optimization until we run into conflicts.

#### Building and updating views

* Data science team members will edit views by editing the model files and
  making pull requests against the `data-architecture/` repo.
* Team members will build views manually by running `dbt run` from
  the root of the folder.
* In rare cases where we want to preserve the behavior of old views, we will
  enable [dbt
  versioning](https://docs.getdbt.com/docs/collaborate/govern/model-versions)
  for those views.

#### Building and updating documentation

* Like views, documentation will be edited via pull request.
* Documentation will be rebuilt by running `dbt docs generate` from the
  root of the folder.
* Documentation will be hosted as a static site on S3 with a CloudFront CDN.
* We will define a helper script in the repo to update documentation.
* As an optional improvement, we may decide to incorporate docs generation and
  upload into a CI/CD step (GitHub Action).

#### Running data integrity checks

* Data integrity checks will run via the `dbt test` command, and will be defined
  as a GitHub Action workflow.
* Data integrity checks will be run once per night after the sqoop operation
  completes.
* The sqoop process will trigger data integrity checks by issuing an API request
  to [trigger the GitHub Action
  workflow](https://docs.github.com/en/rest/actions/workflows?apiVersion=2022-11-28#create-a-workflow-dispatch-event).
* As an initial MVP, data integrity results will print to the console in the
  output of the GitHub Action workflow.
* As an initial MVP, we will subscribe to [workflow
  notifications](https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/notifications-for-workflow-runs)
  to get notified about results of data integrity checks.
* As an iterative improvement, we will design a more complicated result
  notification process using e.g. [stored dbt test
  failures](https://docs.getdbt.com/reference/resource-configs/store_failures)
  and [AWS SNS](https://aws.amazon.com/sns/) for notification management.

### Pros

* Designed around software engineering best practices (version control, reproducibility, testing, etc.)
* Free and open source
* Built on top of established, familiar tools (e.g. command line, SQL, Python)
* Documentation and data validation out of the box

### Cons

* No native support for R scripting as a means of building models, so we would have to either rewrite our raw data extraction scripts or use some kind of hack like running our R scripts from a Python function
* We would need to use a [community plugin](https://dbt-athena.github.io/) for Athena support; this plugin is not supported on dbt Cloud, if we ever decided to move to that
* Requires a separate orchestrator for automation, monitoring, and alerting
* Tests currently do not support the same rich documentation descriptions that other entities do (see [this GitHub issue](https://github.com/dbt-labs/dbt-core/issues/2578))

### Open questions

* Given the way tests are defined in dbt, we expect each validation check to be
  implemented as a query that runs against Athena. According to experiments
  by other users, Athena has a soft concurrency limit of 4-5 queries at once
  ([source](https://stackoverflow.com/q/57145967)). Given that table scans
  take ~10s in my experience, is this flow going to scale to the size of the
  validation we expect to need?

### Raw dbt notes

<details>
<summary>Click here for raw notes on dbt</summary>
<br>

* Requirements
  * Referential integrity
    * Built in via [tests](https://docs.getdbt.com/docs/build/tests) (relationship)
  * Schema consistency
    * [Ref versioning](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#versioned-ref)
    * [Snapshots](https://docs.getdbt.com/docs/build/snapshots)
  * Documentation
    * [Docs](https://docs.getdbt.com/docs/collaborate/documentation)
    * Stored separately from the code, in YAML file
      * Although [SQL comments](https://docs.getdbt.com/sql-reference/comments) are supported, and inline descriptions of databases/tables/views are allowed
    * `dbt docs generate` compiles docs, `dbt docs serve` runs a server
      * [Sample docs](https://www.getdbt.com/mrr-playbook)
    * There are also [contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts#how-are-contracts-different-from-tests)
      * But these seem too strict for our use case (basically build failures)
        * We would likely want to set `enforced: false`
  * Orchestration
    * Nothing built in, some recommended solutions include:
      * Airflow
      * [Prefect](https://www.prefect.io/)
      * [Dagster](https://dagster.io/)
      * [Meltano](https://docs.meltano.com/getting-started/meltano-at-a-glance)
  * Views and joins
    * [Docs](https://docs.getdbt.com/terms/view)
    * Can be defined and documented like other tables
  * Data validation
    * [Tests](https://docs.getdbt.com/docs/build/tests) can be any SQL query
    * Two failiure levels: `fail` and `warn`
  * Automated flagging
    * Nothing built in; we would have to produce output views and then use the
      orchestration layer to define an automated process
  * Monitoring
    * Nothing built in, this will likely need to be part of the orchestration layer
* We would have to use a [community adapter](https://dbt-athena.github.io/) to connect to Athena
  * Migration may be challenging
  * Not a [verified adapter](https://docs.getdbt.com/docs/supported-data-platforms#verified-adapters)
* Web UI seems [expensive](https://www.getdbt.com/pricing/)
  * Team is $100/seat/mo, and we likely need Enterprise features
    * E.g. Team plan only allows 5 readonly users
  * Core example uses [Meltano](https://docs.meltano.com/getting-started/meltano-at-a-glance) for ETL
* A lot of useful stuff is only available in Cloud, including:
  * CI/CD
  * Job scheduling
  * Deploy environments
  * Built-in IDE
  * Job monitoring
* There are clearly [a lot of orchestrators](https://www.reddit.com/r/dataengineering/comments/10x4n7n/best_orchestration_tool_to_run_dbt_projects/)!

</details>

## Alternatives considered

### [AWS Glue](https://aws.amazon.com/glue/)

AWS Glue is an AWS service for ETL. It also provides utilities for data
quality checks and data cataloging.

#### Pros
* Built-in to AWS, which we are already locked into
* High confidence that this service will last for a long time
* Pricing for data integrity checks is similar to running ETL tasks

#### Cons
* It may be a strategic risk to lock ourselves into AWS even further
* "Push logs to CloudWatch" and "query failures using Athena" do not
give me confidence in ease of use
* No easy way of keeping code and configuration under version control
* No data documentation beyond what is visible in the AWS console

#### Raw notes

<details>
<summary>Click here for raw notes on AWS Glue</summary>
<br>

* (Getting started guide for data quality)[https://aws.amazon.com/blogs/big-data/getting-started-with-aws-glue-data-quality-from-the-aws-glue-data-catalog/]
* [Version control integration](https://aws.amazon.com/blogs/big-data/code-versioning-using-aws-glue-studio-and-github/) seems like it requires some AWS spaghetti code
* As seen in the [quickstart guide](https://docs.aws.amazon.com/glue/latest/dg/start-console-overview.html), authoring jobs seems very UI-heavy
* [Schemas](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-schemas) are defined as either JSON or protobuf

</details>

### [Great Expectations](https://greatexpectations.io/gx-oss)

Great Expectations is an open source Python library for data validation. It
is often used for data integrity in conjunction with dbt, most notably in the
[Meltano](https://docs.meltano.com/getting-started/meltano-at-a-glance/)
ELT stack.

#### Pros
* Simple design (data quality checks are just Python functions)
* More extensive documentation of tests than dbt
* Native Athena support
* Validation runs can [output HTML](https://docs.greatexpectations.io/docs/terms/data_docs) or other [custom rendering formats](https://docs.greatexpectations.io/docs/terms/renderer/)

#### Cons
* Documentation of non-test entities is less featureful than dbt (e.g. no lineage graphs)
* Can't build or version views; can only run integrity checks
* Only satisfies data validation requirement
* [Custom checks](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/overview) are complicated and must be defined in Python

### [Apache Atlas](https://atlas.apache.org)

Apache Atlas is an open source data governance and metadata management service.

#### Pros
* Open source
* We could run it on prem

#### Cons
* Uncertain how well maintained it is, e.g. on 7/18 the [last
commit](https://github.com/apache/atlas/commit/75eb33f0fb3ac7baf6547bdd35bd65acdea0d8f9)
was on 6/13
* Integrity checks require a [third-party package](https://github.com/osmlab/atlas-checks)

### A huge Excel spreadsheet

Theoretically, we could use a large Excel spreadsheet to document our data,
and write macros for running integrity checks.

#### Pros
* Easy for non-Data team members to operate
* Excel is likely to be supported for a long time

#### Cons
* Excel macros are an esoteric art and will probably be hard to debug and
  maintain in the long run
* No clear path forward for integrating with Athena
