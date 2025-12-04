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
    c("i" = "Running medfateland simulation for {.date {date_to_process}}")
  )

  ##############################################################################
  # WHY ITERATE BY PROVINCE AND NOT USE THE PARALLELIZE OPTION IN MEDFATELAND? #
  # I did tests. Parallelizing by province and custom state updating makes     #
  # around 25 minutes per day. Parallelizing with medfateland or any           #
  # combination of province and medfateland parallelization was considerably   #
  # slower.                                                                    #
  ##############################################################################

  # iterate by province, for that we start with the initialized forests files,
  # that are already splitted by province.
  province_files <- list.files(
    "data-raw", "initialized.rds", full.names = TRUE
  )
  # date stripped
  date_stripped <- stringr::str_remove_all(date_to_process, "-")
  meteo_data_file <- list.files(
    Sys.getenv("METEOROLOGY_PATH"), date_stripped, full.names = TRUE
  )
  if (!file.exists(meteo_data_file)) {
    # return the "zero" state if not state found
    cli::cli_abort("No meteo data for {.date {date_to_process}}. Aborting...")
  }

  # day_files <- purrr::map_chr(
  # day_files <- furrr::future_map_chr(
  # mirai daemons
  mirai::daemons(12)
  withr::defer(mirai::daemons(0))
  mirai::everywhere({}, .update_state = .update_state)
  day_files <- mirai::mirai_map(
    .x = province_files,
    .f = \(province_file) {
      # output file name
      output_file <- stringr::str_replace(
        province_file,
        "data-raw", Sys.getenv("MEDFATELAND_OUTPUTS_PATH")
      ) |>
        stringr::str_replace(
          "initialized", date_stripped
        )

      # forests data
      forests_sf <- readRDS(province_file)
      spatial_filter <- forests_sf$geometry |>
        sf::st_union() |>
        sf::st_convex_hull() |>
        sf::st_buffer(dist = 1000) |>
        sf::st_as_text()

      # meteo data
      meteo_vars <- c(
        "MeanTemperature", "MinTemperature", "MaxTemperature",
        "MeanRelativeHumidity", "MinRelativeHumidity", "MaxRelativeHumidity",
        "Precipitation", "Radiation", "WindSpeed", "PET", "ThermalAmplitude"
      )
      # read meteo data, but only the province plus buffer
      meteo_data <- sf::st_read(
        meteo_data_file,
        quiet = TRUE, wkt_filter = spatial_filter
      )

      # create meteo raster
      raster_platon_specs <- readRDS("data-raw/penbal_platon_specs.rds")
      raster_platon <- terra::rast(
        extent = raster_platon_specs$extent,
        resolution = raster_platon_specs$resolution,
        crs = raster_platon_specs$crs,
        nlyrs = length(meteo_vars)
      )
      # points to cell (pixel) indexes
      cells_index <-
        terra::cells(raster_platon, terra::vect(meteo_data))[, "cell"]
      # assign values to raster platon
      raster_platon[cells_index] <- meteo_data |>
        dplyr::as_tibble() |>
        dplyr::select(
          -geom, -geom_hex, -geom_text,
          -dates, -elevation, -slope, -aspect, -DOY, -WindDirection,
          -TPI, -partition,
          -day, -month, -year
        )
      # assign names to the raster
      names(raster_platon) <-
        names(meteo_data)[
          !names(meteo_data) %in% c(
            "geom", "geom_hex", "geom_text", "dates",
            "elevation", "slope", "aspect", "DOY", "WindDirection",
            "TPI", "partition",
            "day", "month", "year"
          )
        ]
      # extract the meteo and join it with the forests
      terra::extract(
        raster_platon, terra::vect(forests_sf$geometry)
      ) |>
        dplyr::mutate(dates = as.Date(date_to_process)) |>
        tidyr::nest(.by = ID, .key = "meteo") |>
        dplyr::bind_cols(forests_sf) |>
        dplyr::select(-ID) |>
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

      # finally, return the name of the file
      return(output_file)
    },
    date_stripped = date_stripped,
    meteo_data_file = meteo_data_file
  )[]

  return(day_files)
}