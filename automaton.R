## libraries
library(targets)
library(ntfy)

## check working directory
setwd(Sys.getenv("PIPELINE_PATH"))
cli::cli_alert_info("Current active pipeline: {.file {getwd()}}")

## daily make
tar_make(reporter = "timestamp")

## daily prune
if (length(tar_prune_list() > 0)) {
  cli::cli_inform(c(
    "!" = "Pruning old targets:",
    tar_prune_list()
  ))
  tar_prune()
}

## notify
pipeline_log_summary <- tar_progress_summary(fields = NULL)
ntfy_tag <- ifelse(
  pipeline_log_summary$errored > 0,
  c(tags$lady_beetle), c(tags$bell)
)
errored_pipelines <- tar_errored()
if (length(errored_pipelines) == 0) {
  errored_pipelines <- "no errors"
}

ntfy_send(
  message = glue::glue(
    "emf_forestdrought pipeline ({pipeline_log_summary$time}):    ",
    "skipped: {pipeline_log_summary$skipped} - ",
    "completed: {pipeline_log_summary$completed} - ",
    "{pipeline_log_summary$errored} errors: ",
    "{glue::glue_collapse(errored_pipelines, sep = ', ', last = ' and ')}"
  ),
  tags = ntfy_tag,
  priority = 2,
  topic = "emf_pipelines"
)
