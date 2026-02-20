"""
Job definitions using Dagster asset selections.

- bronze_job       — materialise all bronze/* assets
- silver_job       — materialise all silver/* assets
- gold_job         — materialise all gold/* assets
- full_pipeline_job — bronze → silver → gold end-to-end
"""

from dagster import define_asset_job, AssetSelection

bronze_job = define_asset_job(
    name="bronze_job",
    selection=AssetSelection.groups("bronze"),
    description="Ingests all Kafka topics into the bronze layer.",
    tags={"layer": "bronze"},
)

silver_job = define_asset_job(
    name="silver_job",
    selection=AssetSelection.groups("silver"),
    description="Cleans and validates bronze data into the silver layer.",
    tags={"layer": "silver"},
)

gold_job = define_asset_job(
    name="gold_job",
    selection=AssetSelection.groups("gold"),
    description="Builds all business aggregates in the gold layer.",
    tags={"layer": "gold"},
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description="End-to-end pipeline: bronze ingestion → silver cleaning → gold aggregation.",
    tags={"layer": "all"},
)
