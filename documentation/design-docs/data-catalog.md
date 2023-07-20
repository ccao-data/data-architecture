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
* Share docs with less technical teammates

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

## Design

## Options

### [AWS
Glue](https://aws.amazon.com/blogs/big-data/getting-started-with-aws-glue-data-quality-from-the-aws-glue-data-catalog/)

#### Pros
* Built-in to AWS
* Pricing is similar to ETL tasks

#### Cons
* Even more AWS lockin
* "Push logs to CloudWatch" and "query failures using Athena" do not
give me confidence in ease of use
* Pricing might not scale well with usage

#### Raw notes

* [Version control integration](https://aws.amazon.com/blogs/big-data/code-versioning-using-aws-glue-studio-and-github/) seems like it requires some AWS spaghetti code
* As seen in the [quickstart guide](https://docs.aws.amazon.com/glue/latest/dg/start-console-overview.html), authoring jobs seems very UI-heavy
* [Schemas](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-schemas) are defined as either JSON or protobuf

### dbt

dbt is a "Data Build Tool" that allows users to define and document ETL
workflows using common software engineering patterns like version control
and modularization. For a longer introduction, see [the
docs](https://docs.getdbt.com/docs/introduction).

For a fun take on what dbt is through the lens of what it is _not_, see
[this blog post](https://stkbailey.substack.com/p/what-exactly-isnt-dbt).

There are two distinct tools that are both somewhat confusingly referred to as
"dbt":

1. **dbt Cloud**: The SaaS version of dbt, including a web view with CI/CD, orchestration, monitoring, RBAC, and an IDE
  * **Pros**: Provides all the features we want for a data catalog
  * **Cons**: Super expensive (minimum $100/seat/mo); we don't control it
2. **dbt Core**: Open source CLI that dbt Cloud is built on top of
  * **Pros**: Free and open source; we would have full control
  * **Cons**: Basically only handles DAG builds (equivalent to e.g. CMake); orchestration/monitoring/etc would have to be handled by another tool

dbt Cloud would save us time in the short run, but is likely prohibitively
expensive for our office, and we also have no guarantee how long the company
will survive. Instead, it seems more prudent for us to build on top of dbt Core
and integrate it with our own orchestration/monitoring/authentication services.
Hence, when this doc refers to "dbt", we are actually referring to dbt Core.

The downside of this choice is that we would have to choose a separate tool for
orchestrating and monitoring our DAGs if we move forward with dbt. This is an
important fact to note in our decision, because [orchestrators are notoriously
controversial](https://stkbailey.substack.com/p/what-exactly-isnt-dbt):

> Every other application, besides dbt, is an orchestrator. Every application
> pays the price for it. Every person hates every application as soon as it
> starts orchestrating. To orchestrate is to grapple with reality, to embrace
> mortality, to welcome death.

As such, we evaluate this choice with an eye towards the options for third-party
orchestration and monitoring.

#### Pros

* Designed around software engineering best practices (version control, reproducibility, testing, etc.)
* Open source
* Built on top of old, comfortable tools (command line, SQL, Python)
* Documentation and data validation out of the box

#### Cons

* No support for R scripting, so we would have to either rewrite it or write some kind of hack like running our R scripts from a Python function
* We would need to use a community plugin for Glue/Athena support
* Requires a separate orchestrator for automation, monitoring, and alerting

#### Raw notes

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

### [Apache Atlas](https://atlas.apache.org)

#### Pros
* Open source
* We could run it on prem

#### Cons
* Uncertain how well maintained it is, e.g. on 7/18 the [last
commit](https://github.com/apache/atlas/commit/75eb33f0fb3ac7baf6547bdd35bd65acdea0d8f9)
was on 6/13

### Custom software

#### Pros
* We would control every feature and can tailor it exactly to our needs

#### Cons
* Would likely require a huge amount of engineering time to ship basic features

### A huge Excel spreadsheet

#### Pros
* Easy for non-Data team members to operate
* Excel is likely to be supported for a long time

#### Cons
* Excel macros are an esoteric art and will probably be hard to debug and
  maintain in the long run

## Open questions

* The project README says "We have to do a fairly large amount of deduping and
  filtering (in Athena) to get the "correct" records out of iasWorld." Where
  does this deduping/filtering happen? Can we easily port it over to the new
  catalog?
