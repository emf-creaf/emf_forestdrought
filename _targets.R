# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  # Packages that your targets need for their tasks.
  packages = c(
    "tibble", "dplyr", "stringr", "purrr", "lubridate",
    "arrow", "geoarrow", "sf", "terra",
    "medfate", "medfateland",
    "future", "furrr"
  ),
  # Default format to qs instead of rds
  format = "qs",
  # Memory to transient to avoid having all the objects of all targets loaded
  memory = "transient",
  # iteration mode default to list
  iteration = "list",
  # garbage collection
  garbage_collection = 5
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()

# debug
# debug(run_daily_medfateland)

# dates
dates_to_process <- seq(Sys.Date() - 385, Sys.Date() - 5, by = "day")

# emf_forestdrought target list
list(
  # dates input
  tar_target(dates, dates_to_process),
  # medfateland runner, dynamic branching for all the dates.
  # dates are not independent, as the state needs to be retrieved from the
  # previous date. So in this case, error should stop the pipeline.
  tar_target(
    daily_models, run_daily_medfateland(dates),
    pattern = map(dates)
  ),
  # parquet writer
  tar_target(
    parquet_files, write_medfateland_parquet(daily_models),
    pattern = map(daily_models),
    # continue on error, return NULL for those failed files
    error = "null"
  ),
  # bitmap creator and table wirter
  tar_target(
    bitmaps, create_medfateland_bitmap(parquet_files),
    pattern = map(parquet_files),
    # continue on error, return NULL for those failed files
    error = "null"
  ),
  tar_target(
    bitmaps_table, write_medfateland_bitmaps(bitmaps)
  ),
  # timeseries for regions
  tar_target(
    provincia_daily_averages,
    calculate_daily_averages(parquet_files, "provincia"),
    pattern = map(parquet_files),
    # continue on error, return NULL for those failed files
    error = "null"
  ),
  tar_target(
    comarca_daily_averages,
    calculate_daily_averages(parquet_files, "comarca"),
    pattern = map(parquet_files),
    # continue on error, return NULL for those failed files
    error = "null"
  ),
  tar_target(
    provincia_timeseries, write_medfateland_timeseries(provincia_daily_averages)
  ),
  tar_target(
    comarca_timeseries, write_medfateland_timeseries(comarca_daily_averages)
  )
)