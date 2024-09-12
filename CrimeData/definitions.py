from dagster import Definitions, load_assets_from_modules, AssetSelection, define_asset_job,ScheduleDefinition
from . import assets

all_assets = load_assets_from_modules([assets])

crimeData_Job = define_asset_job("crimeData_Job", selection=AssetSelection.all())

# Defines how often the job should run using cron scheduling 
CrimeData_schedule = ScheduleDefinition(
    job=crimeData_Job,
    cron_schedule = "0 12 * * SAT" ,  # every Saturday at midday
)

defs = Definitions(
    assets=all_assets,
    jobs = [crimeData_Job],
    schedules=[CrimeData_schedule]
)
