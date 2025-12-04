calculate_daily_averages <- function(parquet_files, admin_level) {
  stopifnot(!is.null(parquet_files))
  # get the selected admin polygons, removing those belonging to Canarias,
  # Ceuta, Melilla
  admin_polygons <- switch(
    admin_level,
    "provincia" = mapSpain::esp_get_prov_siane(epsg = 4326, resolution = 10) |>
      dplyr::filter(
        !ine.prov.name %in%
          c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
        !is.na(cpro)
      ) |>
      dplyr::select(name = ine.prov.name, geom) |>
      sf::st_transform(crs = 25830),
    "municipio" = mapSpain::esp_get_munic_siane(epsg = 4326, resolution = 10) |>
      dplyr::filter(
        !ine.prov.name %in%
          c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
        !is.na(cpro),
        !is.na(name)
      ) |>
      dplyr::select(name, geom) |>
      sf::st_transform(crs = 25830),
    "comarca" = mapSpain::esp_get_comarca(epsg = 4326, type = "INE") |>
      dplyr::filter(
        !ine.prov.name %in%
          c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
        !is.na(cpro)
      ) |>
      dplyr::select(name, geom) |>
      sf::st_transform(crs = 25830)
  )

  cli::cli_inform(
    c("i" = "Creting {admin_level} timeseries for {parquet_files}")
  )

  # mirai daemons
  mirai::daemons(12)
  mirai::everywhere({
    # db preparation
    duckdb_proxy <<- DBI::dbConnect(duckdb::duckdb())
    # withr::defer(DBI::dbDisconnect(duckdb_proxy))
    install_httpfs_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "INSTALL httpfs;"
    )
    httpfs_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "LOAD httpfs;"
    )
    install_spatial_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "INSTALL spatial;"
    )
    spatial_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "LOAD spatial;"
    )
    credentials_statement <- glue::glue(
      "CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{Sys.getenv('AWS_ACCESS_KEY_ID')}',
        SECRET '{Sys.getenv('AWS_SECRET_ACCESS_KEY')}',
        REGION '',
        ENDPOINT '{Sys.getenv('AWS_S3_ENDPOINT')}'
      );"
    )
    DBI::dbExecute(duckdb_proxy, install_httpfs_statement)
    DBI::dbExecute(duckdb_proxy, httpfs_statement)
    DBI::dbExecute(duckdb_proxy, install_spatial_statement)
    DBI::dbExecute(duckdb_proxy, spatial_statement)
    DBI::dbExecute(duckdb_proxy, credentials_statement)
  })
  # disconnect db and close daemons on exit
  withr::defer({
    mirai::everywhere({
      DBI::dbDisconnect(duckdb_proxy)
    })
    mirai::daemons(0)
  })

  mirai::mirai_map(
    admin_polygons,
    \(name, cpro, geom) {
      # row query
      ts_query <- glue::glue("
        FROM (
          FROM
            read_parquet('{parquet_files}*.parquet', hive_partitioning=true)
          SELECT
            date,
            ST_Point(geometry.x, geometry.y)::GEOMETRY AS geom,
            COLUMNS('Theta|REW|Psi|PET|Precipitation|AET|ELW|LAI|DDS|LFMC|DFMC|SFP|CFP')
        )
        SELECT
          date,
          avg(Theta) FILTER (NOT isnan(Theta)) AS Theta,
          avg(REW) FILTER (NOT isnan(REW)) AS REW,
          avg(Psi) FILTER (NOT isnan(Psi)) AS Psi,
          avg(PET) FILTER (NOT isnan(PET)) AS PET,
          avg(Precipitation) FILTER (NOT isnan(Precipitation)) AS Precipitation,
          avg(AET) FILTER (NOT isnan(AET)) AS AET,
          avg(ELW) FILTER (NOT isnan(ELW)) AS ELW,
          avg(LAI) FILTER (NOT isnan(LAI)) AS LAI,
          avg(DDS) FILTER (NOT isnan(DDS)) AS DDS,
          avg(LFMC) FILTER (NOT isnan(LFMC)) AS LFMC,
          avg(DFMC) FILTER (NOT isnan(DFMC)) AS DFMC,
          avg(SFP) FILTER (NOT isnan(SFP)) AS SFP,
          avg(CFP) FILTER (NOT isnan(CFP)) AS CFP
        WHERE ST_Intersects(geom, ST_GeomFromText('{sf::st_as_text(geom)}'))
        GROUP BY date
        ;
      ")
      DBI::dbGetQuery(duckdb_proxy, ts_query) |>
        dplyr::as_tibble() |>
        dplyr::mutate(
          name = name,
          geom = sf::st_as_text(geom)
        )
    },
    parquet_files = parquet_files
  )[] |>
    purrr::list_rbind() |>
    dplyr::mutate(admin_level = admin_level)
}

write_medfateland_timeseries <- function(daily_averages) {
  # s3 filesystem
  s3_fs <- S3FileSystem$create(
    access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    scheme = "https",
    endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
    region = ""
  )

  # write png tibble
  data_to_write <- daily_averages |>
    purrr::list_rbind()

  file_name <- switch(
    unique(data_to_write$admin_level),
    "municipio" = "forestdrought-spain-app-pngs/daily_medfateland_timeseries_municipio.parquet",
    "comarca" = "forestdrought-spain-app-pngs/daily_medfateland_timeseries_comarca.parquet",
    "provincia" = "forestdrought-spain-app-pngs/daily_medfateland_timeseries_provincia.parquet"
  )

  # write png tibble
  daily_averages |>
    purrr::list_rbind() |>
    arrow::write_parquet(
      sink = s3_fs$path(file_name)
    )

  return(paste0("s3://", file_name))
}