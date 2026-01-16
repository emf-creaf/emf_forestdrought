.update_state <- function(ini_object, date_to_process, province_file) {
  # get the previous day date
  state_date <- as.character(as.Date(date_to_process) - 1)
  state_date_stripped <- stringr::str_remove_all(
    state_date, "-"
  )
  state_file <- stringr::str_replace(
    province_file,
    "data-raw", Sys.getenv("MEDFATELAND_OUTPUTS_PATH")
  ) |>
    stringr::str_replace(
      "initialized", state_date_stripped
    )

  # We need to check that the state file exists. If not, recursive execution
  # to check previous days until date limit (385 days from present day)
  if (!file.exists(state_file)) {
    # stop recursion if limit date is reached
    if (as.Date(date_to_process) < (Sys.Date() - 385)) {
      # return the "zero" state if not state found
      return(ini_object)
    }
    # recursive call
    return(.update_state(
      ini_object, as.character(as.Date(date_to_process) - 1), province_file
    ))
  }

  last_state <- readRDS(state_file)

  # start transformation
  ini_object[["state"]] <- last_state[["state"]]
  ini_object[["soil"]] <- purrr::map(last_state[["state"]], "soil")

  # return the updated object
  return(ini_object)
}

run_daily_medfateland <- function(date_to_process) {
  cli::cli_inform(
    c("i" = "Updating medfateland object for {.date {date_to_process}}")
  )

  # iterate by province, for that we start with the initialized forests files,
  # that are already splitted by province.
  province_files <- 
    list.files("data-raw", "initialized.rds", full.names = TRUE)[c(
      10, 6, 16, 13, 22, 19, 21, 42, 14, 36, 24, 9, 2, 25, 40, 23, 27,
      33, 8, 31, 48, 15, 43, 44, 17, 39, 18, 30, 32, 41, 12, 5, 38, 28,
      47, 11, 29, 37, 35, 7, 26, 34, 1, 4, 46, 3, 20, 45
    )]
  # date stripped
  date_stripped <- stringr::str_remove_all(date_to_process, "-")
  meteo_data_file <- list.files(
    Sys.getenv("METEOROLOGY_PATH"), date_stripped, full.names = TRUE
  )
  if (!file.exists(meteo_data_file)) {
    # return the "zero" state if not state found
    cli::cli_abort("No meteo data for {.date {date_to_process}}. Aborting...")
  }

  # meteo vars
  meteo_vars <- c(
    "MeanTemperature", "MinTemperature", "MaxTemperature",
    "MeanRelativeHumidity", "MinRelativeHumidity", "MaxRelativeHumidity",
    "Precipitation", "Radiation", "WindSpeed", "PET", "ThermalAmplitude"
  )
  # raster platon
  raster_platon_specs <- readRDS("data-raw/penbal_platon_specs.rds")
  raster_platon <- terra::rast(
    extent = raster_platon_specs$extent,
    resolution = raster_platon_specs$resolution,
    crs = raster_platon_specs$crs,
    nlyrs = length(meteo_vars)
  ) |>
    terra::wrap()

  # mirai daemons
  mirai::daemons(8, output = TRUE)
  withr::defer(mirai::daemons(0))
  mirai::everywhere(
    {
      suppressMessages({
        library(meteoland)
        library(medfate)
        library(medfateland)
        library(sf)
      })
    },
    .update_state = .update_state,
    date_stripped = date_stripped,
    meteo_data_file = meteo_data_file,
    date_to_process = date_to_process,
    raster_platon = raster_platon,
    meteo_vars = meteo_vars
  )
  # mirai::mirai_map(
  purrr::map(
    .x = province_files,
    .f = purrr::in_parallel(\(province_file) {

      # output file name
      output_file <- stringr::str_replace(
        province_file,
        "data-raw", Sys.getenv("MEDFATELAND_OUTPUTS_PATH")
      ) |>
        stringr::str_replace(
          "initialized", date_stripped
        )
      
      init_time <- Sys.time()
      
      # forests data
      raster_platon <- terra::unwrap(raster_platon)
      forests_sf <- readRDS(province_file) |>
        dplyr::mutate(
          cell_index = terra::cells(
            raster_platon, terra::vect(geometry)
          )[, "cell"]
        )
      spatial_filter <- forests_sf$geometry |>
        sf::st_union() |>
        sf::st_convex_hull() |>
        sf::st_buffer(dist = 1000) |>
        sf::st_as_text()

      # read meteo data, but only the province plus buffer
      meteo_data <- sf::st_read(
        meteo_data_file,
        quiet = TRUE, wkt_filter = spatial_filter
      ) |>
        dplyr::mutate(
          cell_index = terra::cells(raster_platon, terra::vect(geom))[, "cell"]
        ) |>
        dplyr::as_tibble() |>
        dplyr::filter(cell_index %in% forests_sf[["cell_index"]]) |>
        dplyr::select(
          -geom, -geom_hex, -geom_text,
          -elevation, -slope, -aspect, -DOY, -WindDirection,
          -TPI, -partition,
          -day, -month, -year
        ) |>
        # join meteo with the forests
        tidyr::nest(.by = cell_index, .key = "meteo")

      if (nrow(meteo_data) < nrow(forests_sf)) {
        empty_meteo <- rep(NA_real_, length(meteo_vars)) |>
          purrr::set_names(meteo_vars) |>
          as.list() |>
          dplyr::as_tibble() |>
          dplyr::mutate(dates = date_to_process)
        meteo_data <- meteo_data |>
          dplyr::bind_rows(
            dplyr::tibble(
              cell_index = forests_sf[["cell_index"]][
                which(
                  !forests_sf[["cell_index"]] %in% meteo_data[["cell_index"]]
                )
              ],
              meteo = list(empty_meteo)
            )
          )
      }

      meteo_data |>
        dplyr::right_join(forests_sf, by = "cell_index") |>
        sf::st_as_sf() |>
        .update_state(date_to_process, province_file) |>
        medfateland::spwb_spatial_day(
          date = as.character(date_to_process),
          SpParams = traits4models::SpParamsES,
          progress = FALSE
        ) |>
        dplyr::mutate(
          date = as.Date(date_to_process)
        ) |>
        saveRDS(file = output_file)

      cli::cli_inform(c(
        "v" = "Completed {.file {stringr::str_split_i(output_file, '/', -1)}}. {Sys.time() - init_time}"
      ))

      return(output_file)
    })
  )
}