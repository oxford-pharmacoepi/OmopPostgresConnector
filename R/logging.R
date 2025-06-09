
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
postgresLog <- function(path,
                        sql = TRUE,
                        explain = TRUE,
                        analyse = FALSE) {
  # input check
  omopgenerics::assertLogical(sql, length = 1)
  omopgenerics::assertLogical(explain, length = 1)
  omopgenerics::assertLogical(analyse, length = 1)
  if (isFALSE(sql) & isFALSE(explain) & isFALSE(analyse)) {
    cli::cli_inform(c("!" = "Deactivating logging as `sql`, `explain` and `analyse` are {.pkg FALSE}."))
    path <- NULL
  } else {
    path <- validatePath(path = path)
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

  if (!dir.exists(logPath)) {
    cli::cli_inform(c(
      "!" = "{.var logPath} is not defined.",
      "i" = "Please use {.pkg postgresLog()} to create a log path."
    ))
    x <- dplyr::tibble()
  } else {
    x <- logPath |>
      list.files(pattern = "^log_query.*\\.txt$", full.names = TRUE) |>
      purrr::map(\(x) readLog(file = x)) |>
      dplyr::bind_rows()
  }

  # populate missing columns and cast
  prepareLog(x)
}

writeLog <- function(x, file) {
  x |>
    prepareLog() |>
    as.list() |>
    purrr::imap(\(xx, nm) {
      if (is.na(xx)) {
        xx <- "-"
      } else {
        xx <- stringr::str_split(string = as.character(xx), pattern = "\n")[[1]]
      }
      if (length(xx) > 1) {
        c(paste0(nm, ":"), paste0("    ", xx))
      } else {
        paste0(nm, ": ", xx)
      }
    }) |>
    purrr::flatten_chr() |>
    writeLines(con = file)
}
fillNa <- function(x) {
  for (i in seq_along(x)) {
    if (is.na(x[i]) && i > 1) {
      x[i] <- x[i - 1]
    }
  }
  x
}
readLog <- function(file) {
  x <- readLines(con = file)
  nms <- x |>
    purrr::imap_chr(\(x, nm) {
      if (startsWith(x = x, prefix = "    ")) {
        NA_character_
      } else {
        stringr::str_extract(string = x, pattern = "^[^:]+")
      }
    }) |>
    fillNa()
  unique(nms) |>
    rlang::set_names() |>
    as.list() |>
    purrr::map(\(nm) {
      cont <- x[nms == nm]
      if (length(cont) > 1) {
        cont <- cont[-1]
        cont <- substr(x = cont, start = 5, stop = nchar(cont))
        cont <- paste0(cont, collapse = "\n")
      } else {
        n <- nchar(nm) + 3
        cont <- substr(x = cont, start = nchar(nm) + 3, nchar(cont))
        if (cont == "-") {
          cont <- NA_character_
        }
      }
      cont
    }) |>
    dplyr::as_tibble() |>
    prepareLog()
}
logSql <- function() {
  any(c(getLogSql(), getLogExplain(), getLogAnalyse())) &
    !identical(getLogPath(), "")
}
prepareLog <- function(x) {
  cols = list(
    job_id = "i", job_name = "c", job_type = "c", start_time = "T",
    end_time = "T", elapsed_seconds = "i", call_function = "c",
    trace_back = "c", source_type = "c", sql = "c", explain = "c",
    analyse = "c"
  )
  presentCols <- colnames(x)
  for (col in names(cols)) {
    fun <- switch (cols[[col]],
      "i" = as.integer,
      "c" = as.character,
      "T" = asDttm
    )
    if (col %in% presentCols) {
      x <- dplyr::mutate(x, dplyr::across(dplyr::all_of(col), fun))
    } else {
      x <- dplyr::mutate(x, !!col := !!fun(NA))
    }
  }

  dplyr::select(x, dplyr::all_of(names(cols)))
}
asDttm <- function(x) {
  as.POSIXct(x = x, format = "%Y-%m-%d %H:%M:%S")
}
getLogName <- function(jobId) {
  sprintf("log_query_%05i_%s.txt", jobId, format(Sys.time(), "on_%Y_%m_%d_at_%H_%M_%S"))
}
startLogger <- function(jobName, jobType, sql, explain, callFrom = jobType) {
  # job id
  jobId <- logIdCounter()

  # logName
  logName <- getLogName(jobId = jobId)

  # get trace back
  tb <- rlang::trace_back()
  ftb <- paste0(utils::capture.output(tb), collapse = "\n")
  tb <- getCallFunction(tb, callFrom)

  logInfo <- dplyr::tibble(
    job_id = jobId,
    job_name = jobName,
    job_type = jobType,
    start_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    end_time = NA_character_,
    elapsed_seconds = NA_integer_,
    call_function = tb,
    trace_back = ftb,
    source_type = "OmopPostgresConnector",
    sql = sql,
    explain = explain
  ) |>
    prepareLog()

  writeLog(x = logInfo, file = file.path(getLogPath(), logName))

  return(logName)
}
finishLogger <- function(logName, analyse) {
  file <- file.path(getLogPath(), logName)

  logInfo <- readLog(file = file) |>
    dplyr::mutate(
      analyse = .env$analyse,
      end_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      elapsed_seconds = as.integer(as.numeric(asDttm(.data$end_time)) -
        as.numeric(asDttm(.data$start_time)))
    )

  writeLog(x = logInfo, file = file)
}
extractSql <- function(sql) {
  ifelse(getLogSql(), sql, NA_character_)
}
extractExplain <- function(src, sql) {
  if (getLogExplain()) {
    con <- getCon(src = src)
    sql <- paste0("EXPLAIN ", sql)
    DBI::dbGetQuery(conn = con, statement = sql) |>
      dplyr::pull() |>
      paste0(collapse = "\n")
  } else {
    NA_character_
  }
}
extractAnalyse <- function(src, sql) {
  if (getLogAnalyse()) {
    sql <- paste0("EXPLAIN ANALYSE ", sql)
    analyse <- DBI::dbGetQuery(conn = getCon(src = src), statement = sql) |>
      dplyr::pull() |>
      paste0(collapse = "\n")
  } else {
    # execute
    DBI::dbExecute(conn = getCon(src = src), statement = sql)
    analyse <- NA_character_
  }
  analyse
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
  getOption("OmopPostgresConnector.log_sql", FALSE)
}
setLogSql <- function(x) {
  options(OmopPostgresConnector.log_sql = x)
}
getLogExplain <- function() {
  getOption("OmopPostgresConnector.log_explain", FALSE)
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
  out <- tryCatch({
    traceBack <- traceBack |>
      dplyr::as_tibble() |>
      dplyr::mutate(
        fun = funName(.data$call),
        id = dplyr::row_number()
      )
    id <- traceBack$id[
      traceBack$namespace == pkgJobType(jobType) &
        traceBack$fun == funJobType(jobType)
    ]
    if (length(id) > 0) {
      idi <- max(id)
      x <- traceBack |>
        dplyr::filter(.data$id < .env$idi & !.data$id %in% .env$id) |>
        dplyr::filter(.data$scope == "::") |>
        utils::tail(1)
      paste0(x$namespace, "::", x$fun)
    } else {
      NA_character_
    }
  },
  error = function(e) NA_character_)

  out
}
funName <- function(call) {
  call |>
    purrr::map_chr(\(x) paste0(utils::capture.output(x), collapse = "\n")) |>
    purrr::map_chr(\(x) {
      stringr::str_extract(string = x, pattern = "(?<=\\s|::|^)[^\\s:()]+(?=\\()")
    })
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
