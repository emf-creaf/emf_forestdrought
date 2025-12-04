library(medfateland)
library(dplyr)
library(sf)
library(purrr)
library(stringr)
library(tidyr)
library(terra)

# provinces except for Canary Islands
province_files <- list.files(
  Sys.getenv("MEDFATELAND_INPUTS_PATH"), ".rds", full.names = TRUE
) |>
  purrr::discard(
    .p = \(file_name) {
      stringr::str_detect(file_name, "_35_|_38_")
    }
  )

# update control object to return fire hazard variables
custom_control <- medfate::defaultControl()
custom_control$fireHazardResults <- TRUE

mirai::daemons(12)
withr::defer(mirai::daemons(0))

forests_sf <- province_files |>
  purrr::map(
    .f = purrr::in_parallel(\(input_name) {
      output_name <- input_name |>
        stringr::str_replace("_sf_500m", "_initialized") |>
        stringr::str_replace(Sys.getenv("MEDFATELAND_INPUTS_PATH"), "data-raw")
      cli::cli_alert_info(c(
        "i" = "Creating initialized forest file: {.file {output_name}}"
      ))
      readRDS(input_name) |>
        sf::st_as_sf() |>
        medfateland::initialize_landscape(
          SpParams = traits4models::SpParamsES,
          local_control = custom_control,
          model = "spwb"
        ) |>
        saveRDS(output_name)
    }, custom_control = custom_control)
  )
