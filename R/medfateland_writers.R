.quality_control <- function(formatted_res_day) {

  medfateland_vars <- c(
    "Theta", "REW", "Psi", "PET", "Precipitation", "AET", "ELW",
    "LAI", "DDS", "LFMC", "DFMC", "SFP", "CFP"
  )
  formatted_res_day |>
    # temporal cleaning of -40 Psi values (NA to all variables when it happens)
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(medfateland_vars),
        ~ dplyr::if_else(.data$Psi == -40, NA_real_, .x)
      )
    ) |>
    # temporal cleaning of DDS and LFMC NaN values (NA to all variables when it
    # happens)
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(medfateland_vars),
        ~ dplyr::if_else(is.nan(DDS) | is.nan(LFMC), NA_real_, .x)
      )
    )
}

.format_medfateland_output <- function(res_day) {
  readRDS(res_day) |>
    dplyr::as_tibble() |>
    dplyr::filter(!purrr::map_lgl(result, rlang::is_error)) |>
    dplyr::mutate(
      # water balance vars
      Precipitation = purrr::map_dbl(result, list("WaterBalance", "Rain")) +
        purrr::map_dbl(result, list("WaterBalance", "Snow")),
      PET = purrr::map_dbl(result, list("WaterBalance", "PET")),
      Interception = purrr::map_dbl(result, list("WaterBalance", "Rain")) -
        purrr::map_dbl(result, list("WaterBalance", "NetRain")),
      Esoil = purrr::map_dbl(result, list("WaterBalance", "SoilEvaporation")),
      Eplant = purrr::map_dbl(result, list("WaterBalance", "Transpiration")),
      AET = Interception + Esoil + Eplant,
      Runoff = purrr::map_dbl(result, list("WaterBalance", "Runoff")),
      DeepDrainage = purrr::map_dbl(
        result, list("WaterBalance", "DeepDrainage")
      ),
      ELW = Runoff + DeepDrainage,
      # stand vars
      LAI = purrr::map_dbl(result, list("Stand", "LAIexpanded")),
      # soil vars (nested, we unnest later)
      soil_vars = purrr::map(
        state,
        .f = \(point_state) {
          soil_function <- point_state$control$soilFunctions
          theta_point <-
            medfate::soil_theta(point_state$soil, model = soil_function)
          psi_point <-
            medfate::soil_psi(point_state$soil, model = soil_function)
          water_wp_point <-
            medfate::soil_waterWP(point_state$soil, model = soil_function)
          water_fc_point <-
            medfate::soil_waterFC(point_state$soil, model = soil_function)
          # theta_fc_point <-
          #   medfate::soil_thetaFC(point_state$soil, model = soil_function)
          w_point <- point_state$soil$W

          res <- dplyr::tibble(
            Theta = max(
              0,
              min(1, sum(theta_point * water_fc_point) / sum(water_fc_point))
            ),
            REW = 100 * max(
              0,
              min(1, sum((w_point * water_fc_point - water_wp_point) / sum(water_fc_point - water_wp_point)))
            ),
            Psi = psi_point[1] # ??? the first???
          )
          if (any(
            sum(is.na(w_point)) > 0,
            sum(is.na(water_fc_point)) > 0,
            sum(water_fc_point) == 0
          )) {
            res[["Theta"]] <- NA
            res[["REW"]] <- NA
            res[["Psi"]] <- NA
          }
          return(res)
        }
      ),
      # plant vars
      plant_vars = purrr::map(
        result,
        .f = \(result_point) {
          plant_dds <- result_point$Plants$DDS
          lai_expanded <- result_point$Plants$LAI
          plant_lfmc <- result_point$Plants$LFMC

          res <- dplyr::tibble(
            DDS = 100 * sum(lai_expanded * plant_dds, na.rm = TRUE) /
              sum(lai_expanded, na.rm = TRUE),
            LFMC = sum(lai_expanded * plant_lfmc, na.rm = TRUE) /
              sum(lai_expanded, na.rm = TRUE)
          )
          return(res)
        }
      ),
      # fire vars (missing????)
      DFMC = purrr::map_dbl(result, list("FireHazard", "DFMC [%]")),
      SFP = purrr::map_dbl(result, list("FireHazard", "SFP")),
      CFP = purrr::map_dbl(result, list("FireHazard", "CFP")),
      # other vars
      # internalPhenology = purrr::map(state, list("internalPhenology")),
      # internalWater = purrr::map(state, list("internalWater")),
      # date vars
      day = lubridate::day(date),
      month = lubridate::month(date),
      year = lubridate::year(date)
    ) |>
    tidyr::unnest(soil_vars) |>
    tidyr::unnest(plant_vars) |>
    dplyr::select(-state, -result) |>
    .quality_control()
}

write_medfateland_parquet <- function(daily_model_files) {
  # bucket
  # s3 filesystem
  s3_fs <- S3FileSystem$create(
    access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    scheme = "https",
    endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
    region = ""
  )

  meteoland_bucket <- s3_fs$cd("forestdrought-spain-app-data")

  cli::cli_inform(
    c("i" = "Formatting medfateland simulation data for {.date {stringr::str_extract(daily_model_files[1], '[0-9]{8}')}}")
  )

  # loop for provinces
  mirai::daemons(10)
  withr::defer(mirai::daemons(0))
  mirai::everywhere({library(sf)})
  day_final_res <-
    mirai::mirai_map(
      daily_model_files, .f = .format_medfateland_output,
      .quality_control = .quality_control
    )[] |>
    purrr::list_rbind()
  # day_final_res <- daily_model_files |>
  #   purrr::map(.f = .format_medfateland_output) |>
  #   purrr::list_rbind()

  cli::cli_inform(
    c("i" = "Writing parquet files of medfateland simulation data for {.date {stringr::str_extract(daily_model_files[1], '[0-9]{8}')}}")
  )

  day_final_res |>
    write_dataset(
      path = meteoland_bucket,
      format = "parquet",
      partitioning = c("year", "month", "day"),
      existing_data_behavior = "overwrite",
      # min_rows_per_group = 50000
      max_rows_per_group = 5000,
      max_rows_per_file = 1000000
    )

  cli::cli_inform(
    c("i" = "Writing gpkg files of medfateland simulation data for {.date {stringr::str_extract(daily_model_files[1], '[0-9]{8}')}}")
  )

  # write interpolated meteo (POINTS geopackge)
  gpkg_file_name <- paste0(
    "/srv/emf_data/fileserver/gpkg/daily_modelled_forests/",
    stringr::str_extract(daily_model_files[1], '[0-9]{8}'),
    ".gpkg"
  )
  day_final_res |>
    dplyr::select(!dplyr::starts_with("internal")) |>
    sf::st_as_sf() |>
    sf::st_write(gpkg_file_name, delete_layer = TRUE, quiet = TRUE)

  date_to_process <- stringr::str_extract(daily_model_files, '[0-9]{8}') |>
    unique() |>
    lubridate::as_date()
  part_parquet_file_name <- paste0(
    "s3://forestdrought-spain-app-data/",
    paste0("year=", lubridate::year(date_to_process), "/"),
    paste0("month=", lubridate::month(date_to_process), "/"),
    paste0("day=", lubridate::day(date_to_process), "/")
  )

  cli::cli_inform(c(
    "v" = "Finished parquet creation.",
    "i" = "{.path {part_parquet_file_name}} created in bucket"
  ))

  return(part_parquet_file_name)
}