create_medfateland_bitmap <- function(parquet_files) {
  # date to process
  year_month_day <- stringr::str_extract_all(
    parquet_files, "=[0-9]*", simplify = TRUE
  ) |>
    stringr::str_remove_all("=") |>
    stringr::str_pad(2, "left", "0")
  date_to_process <-
    paste0(year_month_day[1], year_month_day[2], year_month_day[3])

  cli::cli_inform(
    c("i" = "Creting bitmap for {.date {date_to_process}}")
  )

  # rounding factor
  # The transformation from variable value to rgb must be done
  # over integers, so we need to multiply by a rounding factor & round to zero
  # decimals
  rounding_factor <- 1000

  # medfateland_vars
  medfateland_vars <- c(
    "Theta", "REW", "Psi",
    "PET", "Precipitation", "AET", "ELW",
    "LAI", "DDS", "LFMC",
    "DFMC", "SFP", "CFP"
  )

  # create platon raster
  raster_platon_specs <- readRDS("data-raw/penbal_platon_specs.rds")
  raster_platon <- terra::rast(
    extent = raster_platon_specs$extent,
    resolution = raster_platon_specs$resolution,
    crs = raster_platon_specs$crs,
    nlyrs = length(medfateland_vars)
  )

  # read day medfateland table
  # s3 filesystem
  s3_fs <- S3FileSystem$create(
    access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    scheme = "https",
    endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
    region = ""
  )
  medfateland_bucket <- s3_fs$cd(
    stringr::str_remove(parquet_files, "s3://")
  )
  # arrow dataset
  medfateland_day <- open_dataset(medfateland_bucket) |>
    dplyr::select(geometry, dplyr::all_of(medfateland_vars)) |>
    sf::st_as_sf()

  # points to cell (pixel) indexes
  cells_index <-
    terra::cells(raster_platon, terra::vect(medfateland_day))[, "cell"]
  # assign values to raster platon
  raster_platon[cells_index] <- medfateland_day |>
    dplyr::as_tibble() |>
    dplyr::select(-geometry)
  # assign names to the raster
  names(raster_platon) <- medfateland_vars

  # reduce resolution and project to lat long
  raster_platon_4326 <- raster_platon |>
    terra::aggregate(fact = 4, fun = mean, na.rm = TRUE) |>
    terra::project("epsg:4326")

  # rounding
  raster_platon_4326 <- round(raster_platon_4326 * rounding_factor, 0)

  # color tables
  color_table_gen <- function(medfateland_var) {
    reverse <- FALSE
    if (dplyr::cur_column() %in% c("DDS", "SFP", "CFP")) {
      reverse <- TRUE
    }
    scales::col_numeric(
      c(
        "#FF0D50", "#FB7C82", "#FEABAC", "#FFD7D7", "#F2EFF2",
        "#9AAABA", "#4B8AA1", "#007490", "#006584"
      ),
      c(min(medfateland_var, na.rm = TRUE), max(medfateland_var, na.rm = TRUE)),
      na.color = "#FFFFFF00", reverse = reverse, alpha = TRUE
    )(medfateland_var)
  }
  color_table_gen_psi <- function(psi_var) {
    scales::col_numeric(
      scales::gradient_n_pal(
        c(
          "#FF0D50", "#FB7C82", "#FEABAC", "#FFD7D7", "#F2EFF2",
          "#9AAABA", "#4B8AA1", "#007490", "#006584"
        ),
        c(0, 0.45, 0.65, 0.75, 0.8, 0.85, 0.9, 0.95, 1)
      ),
      c(min(psi_var, na.rm = TRUE), max(psi_var, na.rm = TRUE)),
      na.color = "#FFFFFF00", reverse = FALSE, alpha = TRUE
    )(psi_var)
  }
  color_tables <- terra::values(raster_platon_4326) |>
    dplyr::as_tibble() |>
    dplyr::mutate(dplyr::across(
      .cols = dplyr::everything(),
      .fns = color_table_gen,
      .names = "{.col}_colored"
    )) |>
    # fix Psi with exp scale
    dplyr::mutate(Psi_colored = color_table_gen_psi(Psi)) |>
    as.data.frame()
  # update raster color tables
  purrr::walk(
    .x = 1:length(medfateland_vars),
    .f = \(var_index) {
      terra::coltab(raster_platon_4326, layer = var_index) <<-
        color_tables[, c(var_index, length(medfateland_vars) + var_index)]
    }
  )

  # create temporal pngs and encode them in base64. Return a tibble with the
  # encoded png and some metadata (extent, palette, min and max values...)
  raster_platon_w <- terra::wrap(raster_platon_4326)
  # mirai daemons
  mirai::daemons(12, output = TRUE)
  withr::defer(mirai::daemons(0))
  bitmap_tibble <- mirai::mirai_map(
    .x = names(raster_platon_4326),
    .f = \(medfateland_var) {

      raster_platon_unw <- terra::unwrap(raster_platon_w)

      temp_path <- tempfile(medfateland_var, fileext = ".png")
      colorized_raster <- terra::colorize(
        raster_platon_unw[[medfateland_var]], "rgb",
        NAflag = 255,
        filename = temp_path,
        overwrite = TRUE
      )
      extent_raster <- terra::ext(colorized_raster)
      png_base64_string <- paste0(
        "data:image/png;base64,",
        base64enc::base64encode(temp_path)
      )

      cli::cli_inform(c(
        "v" = medfateland_var
      ))

      dplyr::tibble(
        date = date_to_process,
        var = medfateland_var,
        palette_selected = "ag_GrnYl",
        base64_string = png_base64_string,
        left_ext = as.numeric(extent_raster[1]),
        down_ext = as.numeric(extent_raster[3]),
        right_ext = as.numeric(extent_raster[2]),
        up_ext = as.numeric(extent_raster[4]),
        # min max value of the variable to build the palette/legend later
        min_value = min(color_tables[[medfateland_var]], na.rm = TRUE) / rounding_factor,
        max_value = max(color_tables[[medfateland_var]], na.rm = TRUE) / rounding_factor
      )
    },
    date_to_process = date_to_process,
    raster_platon_w = raster_platon_w
  )[] |>
    purrr::list_rbind()

  return(bitmap_tibble)
}

write_medfateland_bitmaps <- function(png_tibbles) {
  # s3 filesystem
  s3_fs <- S3FileSystem$create(
    access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    scheme = "https",
    endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
    region = ""
  )

  # write png tibble
  png_tibbles |>
    purrr::list_rbind() |>
    arrow::write_parquet(
      sink = s3_fs$path("forestdrought-spain-app-pngs/daily_medfateland_bitmaps.parquet"),
      chunk_size = 11
    )

  return("forestdrought-spain-app-pngs/daily_medfateland_bitmaps.parquet")
}