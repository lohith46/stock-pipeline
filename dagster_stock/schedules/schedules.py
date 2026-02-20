"""
Schedules:
- daily_bronze_schedule   — runs bronze_job at midnight UTC every day
- hourly_silver_schedule  — runs silver_job at the top of every hour
"""

from dagster import ScheduleDefinition

from dagster_stock.jobs.jobs import bronze_job, silver_job

daily_bronze_schedule = ScheduleDefinition(
    name="daily_bronze_schedule",
    job=bronze_job,
    cron_schedule="0 0 * * *",   # midnight UTC
    execution_timezone="UTC",
    description="Ingests all Kafka topics into the bronze layer once per day at midnight UTC.",
)

hourly_silver_schedule = ScheduleDefinition(
    name="hourly_silver_schedule",
    job=silver_job,
    cron_schedule="0 * * * *",   # top of every hour
    execution_timezone="UTC",
    description="Cleans and validates bronze data into the silver layer every hour.",
)
