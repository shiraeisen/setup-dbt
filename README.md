This repo orchestrates the dbt transformations to materialize tables through fivetran into BigQuery

### Models

The tables that materialize via the transformations contained in this repo include:
- appointments_materialized_dbt_incremental
- fulfillment_orders_materialized_dbt_incremental
- fulfillment_shipments_materialized_dbt_incremental
- ticket_order_mapping_dbt

These tables materialize into [iron-zodiac-336013.dbt_fivetran](https://console.cloud.google.com/bigquery?referrer=search&cloudshell=false&project=iron-zodiac-336013&supportedpurview=project&ws=!1m4!1m3!3m2!1siron-zodiac-336013!2sdbt_fivetran)

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
