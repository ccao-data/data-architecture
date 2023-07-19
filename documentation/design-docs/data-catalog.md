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
following issues with our current data infrastructure described in the README:

* Referential integrity issues
* Schema inconsistency
* Lack of documentation around existing data
* Orchestration issues (timing, automating data creation)
* Complex joins and views
* Lack of data validation and alerting for iasWorld and third-party data

For a full list of issues we intend to solve with the catalog, see the
[requirements](#requirements).

## Requirements

### Referential integrity

* Defining keys on top of unkeyed data
* Deduplication
* Filtering

### Schema consistency

* Track changes to schemas over time
* Track changes to queries in response to changing schemas

### Documentation

* ETL operations
* Databases, tables, views, schemas
* Manual processes

### Orchestration and automation

* Timing, e.g. waiting for current parcel shapefiles to join to third-party data
* Clear organization of scripts for loading third-party data

### Views

* Clear provenance for views
* Documentation of choices in view queries

### Data validation

* Add layers of validation on top of varchar fields
* Automated flagging for process mistakes

### Bidirectional data flows

* Clear process for pushing output from data catalog to system of record

### Monitoring

* Know when data has been updated
* See results of ETL jobs and debug them
* Alerting based on job status

## Design

## Alternatives considered

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

### dbt

Raw notes:
  * Requirements
    * Referential integrity
      * Built in via [tests](https://docs.getdbt.com/docs/build/tests)
    * Schema consistency
      * [Ref versioning](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#versioned-ref)
    * Documentation
      * [Docs](https://docs.getdbt.com/docs/collaborate/documentation)
      * Stored separately from the code, in YAML file
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
    * Views
    * Data validation
      * [Tests](https://docs.getdbt.com/docs/build/tests) can be any SQL query
      * Two failiure levels: `fail` and `warn`
    * Bidirectional data flows
    * Monitoring
    * ETL automation
  * We would have to use a [community adapter](https://dbt-athena.github.io/) to connect to Athena
    * Migration may be challenging
    * Not a [verified adapter](https://docs.getdbt.com/docs/supported-data-platforms#verified-adapters)
  * Web UI seems [expensive](https://www.getdbt.com/pricing/)
    * Team is $100/seat/mo, and we likely need Enterprise features
      * E.g. Team plan only allows 5 readonly users
    * Core example uses
      [Meltano](https://docs.meltano.com/getting-started/meltano-at-a-glance)
      for ETL
  * A lot of useful stuff is only available in Cloud, including:
    * CI/CD
    * Job scheduling
    * Deploy environments
    * Built-in IDE
    * Job monitoring

#### Pros
* Emerging industry standard

#### Cons

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
