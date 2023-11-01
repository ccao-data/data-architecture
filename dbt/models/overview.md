{% docs __overview__ %}
# Cook County Assessor's Office Data Catalog and Documentation

This site documents the data infrastructure created and used by
the Data Department of the Cook County Assessor's Office.

### Using This Site

The site is under active development and is generated automatically
using [dbt](https://docs.getdbt.com/docs/introduction). It contains
descriptions and documentation for nearly all of the actively used tables
and views in the Data Department's
[AWS Athena](https://aws.amazon.com/athena/)-based data warehouse.
Assets (tables and views) are grouped by topic or source into databases within
a single AWS data catalog. To view the documentation of a single table:

1. Click the **Database** tab in the top left of this page,
   then **awsdatacatalog** to view a list of all current databases
2. Click one of the database names (e.g. **model**) to view all the tables
   and/or views within that database
3. Click a table name (e.g. **model** > **metadata**) to view a description
   of the table, its tags, any related tables or tests, and the SQL
   source code that created it (if applicable)
4. Click a column name within a table (e.g. **model** > **metadata** > **run_id**)
   to get a full description of that column

You can also view the
[lineage graph](https://docs.getdbt.com/terms/data-lineage) of any view
or table by clicking the teal button on the bottom right of this page. This
lets you explore the upstream and downstream dependencies of any asset.
Assets in the graph are color-coded by type:

- ![source](/data-architecture/assets/swatch-source.png) [Sources](https://docs.getdbt.com/docs/build/sources) - Data loaded into the warehouse by scripts and other tools
- ![model](/data-architecture/assets/swatch-model.png) [Models](https://docs.getdbt.com/docs/build/models) - Data transformation and assets created and managed by dbt
- ![exposure](/data-architecture/assets/swatch-exposure.png) [Exposures](https://docs.getdbt.com/docs/build/exposures) - Downstream consumers of dbt assets e.g. [Open Data](https://datacatalog.cookcountyil.gov/browse?tags=cook+county+assessor)

For more information on the function and construction of this system, visit the
[Data Department's dbt README on GitHub](https://github.com/ccao-data/data-architecture/tree/master/dbt#readme).

### Overall Architecture

The overall data architecture outlined on this site is designed to be as
simple as possible while supporting the Data Department's two primary duties:
residential modeling and overall assessment reporting.

The architecture is primarily built around an
[AWS Athena](https://aws.amazon.com/athena/)-based data "lakehouse" which
facilitates SQL querying of static files stored on [S3](https://aws.amazon.com/s3/)
(mostly Parquet files). The core of this warehouse is built around a mirror
of the County's property tax system-of-record, iasWorld. The Data Department
collects, cleans, and appends additional data to this core to support residential
modeling. It also aggregates and exports data to support valuation reporting
via Tableau and the Assessor's site. The following diagram summarizes the
Data Department's current infrastructure and data flows.

#### Data Flow Diagram

![Data Flow Diagram](/data-architecture/assets/dataflow-diagram.svg)

#### iasWorld Entity Relationship Diagram

The following entity relation diagram (ERD) represents the Data Department's
best current understanding of the main iasWorld tables used by the CCAO. Note
that this is _not_ an official diagram, only an approximation. The source for this
diagram can be [found here](https://lucid.app/lucidchart/da854c6c-eead-4d15-8989-8f2060e3ba71/edit?invitationId=inv_f226cccb-c40c-4260-8334-f2f6bae216aa).

![iasWorld ERD](/data-architecture/assets/iasworld-erd.pdf)

### Additional Resources

- [CCAO Data Department Wiki](https://github.com/ccao-data/wiki)
- [iasWorld Tables and Columns Overview](https://github.com/ccao-data/wiki/raw/master/Data/iasWorld-Tables.xlsx)
- [iasWorld Primary and Foreign Key List](https://github.com/ccao-data/wiki/raw/master/Data/iasWorld-PK-FK-2021-06-14.xlsx)

{% enddocs %}
