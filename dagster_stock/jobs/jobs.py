"""
Job definitions using Dagster asset selections.

Layer execution order:
    bronze → staging → { invalid, reference } → silver → gold

Jobs
----
bronze_job        — materialise all bronze/* assets
staging_job       — materialise all staging/* assets  (depends on bronze)
invalid_job       — materialise all invalid/* assets  (depends on staging)
reference_job     — materialise all reference/* assets (depends on staging)
silver_job        — materialise all silver/* assets   (depends on staging)
gold_job          — materialise all gold/* assets     (depends on silver)
full_pipeline_job — end-to-end: bronze → staging → invalid + reference → silver → gold
"""

from dagster import define_asset_job, AssetSelection

bronze_job = define_asset_job(
    name="bronze_job",
    selection=AssetSelection.groups("bronze"),
    description="Ingests all Kafka topics into the bronze layer.",
    tags={"layer": "bronze"},
)

staging_job = define_asset_job(
    name="staging_job",
    selection=AssetSelection.groups("staging"),
    description="Labels and normalizes bronze records; no rows dropped.",
    tags={"layer": "staging"},
)

invalid_job = define_asset_job(
    name="invalid_job",
    selection=AssetSelection.groups("invalid"),
    description="Materialises invalid and duplicate records flagged in staging for investigation.",
    tags={"layer": "invalid"},
)

reference_job = define_asset_job(
    name="reference_job",
    selection=AssetSelection.groups("reference"),
    description="Builds static and derived reference / entity tables.",
    tags={"layer": "reference"},
)

silver_job = define_asset_job(
    name="silver_job",
    selection=AssetSelection.groups("silver"),
    description="Promotes valid staging records into the silver layer.",
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
    selection=AssetSelection.groups(
        "bronze", "staging", "invalid", "reference", "silver", "gold"
    ),
    description=(
        "End-to-end pipeline: "
        "bronze ingestion → staging labeling → invalid capture + reference build → "
        "silver promotion → gold aggregation."
    ),
    tags={"layer": "all"},
)
