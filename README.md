This repo orchestrates the dbt transformations to materialize tables through fivetran into BigQuery

### Models

The tables that materialize via the transformations contained in this repo include:
- appointments_materialized_dbt
- fulfillment_orders_materialized_dbt
- fulfillment_shipments_materialized_dbt
- ticket_order_mapping_dbt

These tables materialize into [iron-zodiac-336013.dbt_fivetran](https://console.cloud.google.com/bigquery?referrer=search&cloudshell=false&project=iron-zodiac-336013&supportedpurview=project&ws=!1m4!1m3!3m2!1siron-zodiac-336013!2sdbt_fivetran)

### Instructions / Good-to-Know's:
- dbt_project.yml orchestrates how/where each of the four models are built
- deployment.yml can determine the cadence at which the models are run, or this can be set when the transformations are built in fivetran
- fivetran syncs with this repo whenever changes are made - no action is required for fivetran to use the most recent version 