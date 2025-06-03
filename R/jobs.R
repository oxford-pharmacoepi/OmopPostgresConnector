
#' Get ongoing Postgres jobs.
#'
#' @param src It can either be a cdm_reference, a postgres_source or a
#' PqConnection object.
#' @param user Users to filter by. If NULL no filter is applied.
#'
#' @return Tibble with the identified jobs.
#' @export
# library(DBI)
# library(RPostgres)
# library(OmopPostgresConnector)
#
# con <- dbConnect(
#   drv = Postgres(),
#   dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "omop_test"),
#   host = "localhost",
#   port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
#   user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
#   password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
#   )
# getJobs(con)
#
# cdm <- cdmFromPostgres(con = con)
# getJobs(cdm)
#
getJobs <- function(src, user) {
  UseMethod("getJobs")
}

#' @export
getJobs.cdm_reference <- function(src, user) {
  getJobs(src = omopgenerics::cdmSource(x = src))
}

#' @export
getJobs.pq_cdm <- function(src, user) {
  getJobs(src = conFromSource(x = src))
}

#' @export
getJobs.PqConnection <- function(src, user) {
  omopgenerics::assertCharacter(user, null = TRUE)

  x <- dplyr::tbl(src, I("pg_stat_activity"))

  if (!is.null(user)) {
    x <- x |>
      dplyr::filter(.data$usename %in% .env$user)
  }

  dplyr::collect(x)
}

#' Cancel a Postgres job.
#'
#' @param src It can either be a cdm_reference, a postgres_source or a
#' PqConnection object.
#' @param pid Numeric. The pid to cancel, multiple can be supplied.
#'
#' @return Invisible TRUE if the process is successful.
#' @export
#'
cancelJob <- function(src, pid) {
  UseMethod("killJob")
}

#' @export
cancelJob.cdm_reference <- function(src, pid) {
  cancelJob(src = omopgenerics::cdmSource(x = src))
}

#' @export
cancelJob.pq_cdm <- function(src, pid) {
  cancelJob(src = conFromSource(x = src))
}

#' @export
cancelJob.PqConnection <- function(src, pid) {
  omopgenerics::assertNumeric(pid, integerish = TRUE)
  pids <- unique(pid)

  for (pid in pids) {
    cli::cli_inform(c(i = "Cancelling job with `pid = {.pkg {pid}}`."))
    statment <- paste0("SELECT pg_cancel_backend(", pid,")")
    DBI::dbExecute(conn = src, statement = statment)
  }

  invisible(TRUE)
}
