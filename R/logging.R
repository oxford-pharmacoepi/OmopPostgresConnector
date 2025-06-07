
#' Start a postgres log
#'
#' @param path Path to a folder where to store the log files.
#' @param sql Whether to log the sql information.
#' @param explain Whether to log the explain infotmation.
#' @param analyse Whether to log the analyse information.
#'
#' @return Invisible path.
#' @export
#'
startPostgresLog <- function(path,
                             sql = TRUE,
                             explain = TRUE,
                             analyse = FALSE) {
  # input check
  path <- validatePath(path = path)
  omopgenerics::assertLogical(sql, length = 1)
  omopgenerics::assertLogical(explain, length = 1)
  omopgenerics::assertLogical(analyse, length = 1)

  if (isFALSE(sql) & isFALSE(explain) & isFALSE(analyse)) {
    cli::cli_abort(c(x = "`sql`, `explain` or `analyse` must be {.pkg TRUE} to set up a {.cls postgresLog}."))
  }

  setLogPath(path)
  setLogSql(sql)
  setLogExplain(explain)
  setLogAnalyse(analyse)

  invisible(path)
}
validatePath <- function(path, call = parent.frame()) {
  omopgenerics::assertCharacter(path, length = 1, call = call)
  if (!dir.exists(path)) {
    cli::cli_inform(c("!" = "Creating logPath {.pkg {path}}"))
    dir.create(path, showWarnings = TRUE, recursive = TRUE)
  }
  invisible(path)
}

#' Create a tibble from the log files in the logPath folder
#'
#' @return A tibble with the one row per log file in the logPath folder.
#' @export
#'
readPostgresLog <- function() {
  logPath <- getLogPath()

  if (!dir.exist(logPath)) {
    cli::cli_inform(c(
      "!" = "{.var logPath} is not defined.",
      "i" = "Please use {.pkg startPostgresLog()} to create a log path."
    ))
    x <- dplyr::tibble()
  } else {
    x <- logPath |>
      list.files(pattern = "^log_query.*\\.txt$", full.names = TRUE) |>
      purrr::map(\(x) read.dcf(file = x)) |>
      dplyr::bind_rows()
  }

  # populate missing columns and cast
  prepareLog(x)
}

prepareLog <- function(x) {
  cols = list(
    job_id = "i", job_title = "c", job_type = "c", start_time = "T",
    end_time = "T", elapsed_seconds = "i", call_function = "c",
    trace_back = "c", source_type = "c", sql = "c", explain = "c",
    analyse = "c"
  )
  presentCols <- colnames(x)
  for (col in names(cols)) {
    fun <- switch (cols[[col]],
      "i" = as.integer,
      "c" = as.character,
      "T" = as.POSIXct
    )
    if (col %in% presentCols) {
      x <- dplyr::mutate(x, dplyr::across(dplyr::all_of(col), fun))
    } else {
      x <- dplyr::mutate(x, !!col := !!fun(NA))
    }
  }

  dplyr::select(x, dplyr::all_of(names(cols)))
}
getLogName <- function(id) {
  sprintf("log_query_%05i_%s.txt", id, format(Sys.time(), "on_%Y_%m_%d_at_%H_%M_%S"))
}
startLogger <- function(logName, jobId, jobTitle, jobType, sql, explain) {
  # get trace back
  tb <- rlang::trace_back()
  ftb <- paste0(capture.output(tb), collapse = "\n")
  tb <- getCallFunction(tb, job_type)

  logInfo <- dplyr::tibble(
    job_id = jobId,
    job_title = jobTitle,
    job_type = jobType,
    start_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    end_time = "",
    elapsed_seconds = NA_integer_,
    call_function = tb,
    trace_back = ftb,
    source_type = "OmopPostgresConnector",
    sql = sql,
    explain = explain
  )

  write.dcf(x = logInfo, file = file.path(getLogPath(), logName))
}
finishLogger <- function(logName, analyse) {
  file <- file.path(getLogPath(), logName)

  read.dcf(file = file) |>
    prepareLog() |>
    dplyr::mutate(
      analyse = .env$analyse,
      end_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      elapsed_seconds = as.numeric(as.POSIXct(.data$end_time)) -
        as.numeric(as.POSIXct(.data$start_time))
    ) |>
    prepareLog() |>
    write.dcf(file = file)
}
extractSql <- function(x) {
  if (getLogSql()) {
    as.character(dplyr::show_query(x))
  } else {
    ""
  }
}
extractExplain <- function(x) {
  if (getLogExplain()) {
    as.character(dplyr::explain(x))
  } else {
    ""
  }
}
logIdCounter <- function() {
  id <- getOption("OmopPostgresConnector.log_id", 1L)
  options(OmopPostgresConnector.log_id = id + 1L)
  return(id)
}
getLogPath <- function() {
  getOption("OmopPostgresConnector.log_path", "")
}
setLogPath <- function(x) {
  options(OmopPostgresConnector.log_path = x)
}
getLogSql <- function() {
  getOption("OmopPostgresConnector.log_sql", TRUE)
}
setLogSql <- function(x) {
  options(OmopPostgresConnector.log_sql = x)
}
getLogExplain <- function() {
  getOption("OmopPostgresConnector.log_explain", TRUE)
}
setLogExplain <- function(x) {
  options(OmopPostgresConnector.log_explain = x)
}
getLogAnalyse <- function() {
  getOption("OmopPostgresConnector.log_analyse", FALSE)
}
setLogAnalyse <- function(x) {
  options(OmopPostgresConnector.log_analyse = x)
}
getCallFunction <- function(traceBack, jobType) {
  tryCatch({
    traceBack <- traceBack |>
      dplyr::as_tibble() |>
      dplyr::mutate(
        fun = stringr::str_extract(as.character(.data$call), "^[^(]+"),
        id = dplyr::row_number()
      )
    id <- max(traceBack$id[
      traceBack$namespace == pkgJobType(jobType) &
        traceBack$fun == funJobType(jobType)
    ])
    x <- traceBack |>
      dplyr::filter(.data$id < .env$id) |>
      dplyr::filter(.data$scope == "::") |>
      utils::tail(1)
    paste0(x$namespace, "::", x$fun)
  },
  error = function(e) "")
}
pkgJobType <- function(jobType) {
  dplyr::case_when(
    jobType == "compute" ~ "dplyr",
    jobType == "create_index" ~ "OmopPostgresConnector",
    jobType %in% c("drop_table", "insert_table") ~ "omopgenerics"
  )
}
funJobType <- function(jobType) {
  dplyr::case_when(
    jobType == "compute" ~ "compute",
    jobType == "create_index" ~ "createIndex",
    jobType == "drop_table" ~ "dropSourceTable",
    jobType == "insert_table" ~ "insertTable"
  )
}
