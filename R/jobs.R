

getJobs <- function(con, user = NULL) {

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

}

#' @export
cancelJob.PqConnection <- function(src, pid) {
  omopgenerics::assertNumeric(pid, integerish = TRUE)
  pids <- unique(pid)

  for (pid in pids) {
    statment <- paste0("SELECT pg_cancel_backend(", pid,")")
    DBI::dbExecute(conn = src, statement = statment)
  }

  invisible(TRUE)
}
