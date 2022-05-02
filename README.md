[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Stripe Airbyte

This package models Stripe data from [Airbyte's connector](https://airbyte.com/connectors/stripe).

Let us know which connectors you would like to see next [here](https://19au6qz3a6s.typeform.com/to/c284SPEN)

## Models

This package contains staging models, with the following naming conventions across all packages:
 * Boolean fields are prefixed with `is_` or `has_`
 * Timestamps are appended with `_timestamp`
 * ID primary keys are prefixed with the name of the table. For example, the campaign table's ID column is renamed `campaign_id`.

## DBT Metrics

This package contains configurations for DBT metrics for you to get up and running quickly with standard Stripe metrics in your existing BI tools.

## Installation Instructions

Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: cerebriumAI/dbt-stripe
    version: ["0.1.0"]
```

## Configuration

### Source Data Location
By default, this package will look for your gStripe data in the `stripe` schema of your [target database](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/configure-your-profile). If this is not where your Stripe data is, please add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
    stripe_schema: your_schema_name
    stripe_database: your_database_name 
```

## Database Support

This package has been tested on BigQuery, Snowflake, Redshift, Postgres, and Databricks.

### Databricks Dispatch Configuration
dbt `v0.20.0` introduced a new project-level dispatch configuration that enables an "override" setting for all dispatched macros. If you are using a Databricks destination with this package you will need to add the below (or a variation of the below) dispatch configuration within your `dbt_project.yml`. This is required in order for the package to accurately search for macros within the `dbt-labs/spark_utils` then the `dbt-labs/dbt_utils` packages respectively.
```yml
# dbt_project.yml

dispatch:
  - macro_namespace: dbt_utils
    search_order: ['spark_utils', 'dbt_utils']
```
## Contributions

Additional contributions to this package are very welcome! Please create issues or open PRs against `master`. Check out [this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package. Suggestions to the DBT metrics are welcome too!

## Resources:
- Provide [feedback](https://19au6qz3a6s.typeform.com/to/c284SPEN) on our existing dbt packages or what you'd like to see next
