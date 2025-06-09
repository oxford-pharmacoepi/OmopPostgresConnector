
#' This function creates a connection to the local postgres instance
#'
#' It relays on environmental variables such as:
#' * dbname = `OMOP_POSTGRES_CONNECTOR_DB`
#' * host = `OMOP_POSTGRES_CONNECTOR_HOST`
#' * port = `OMOP_POSTGRES_CONNECTOR_PORT`
#' * user = `OMOP_POSTGRES_CONNECTOR_USER`
#' * password = `OMOP_POSTGRES_CONNECTOR_PASSWORD`
#'
#' @return A connection to your postgres local instance
#' @export
#'
#' @examples
#' \dontrun{
#' library(OmopPostgresConnector)
#'
#' localPostgres()
#' }
localPostgres <- function() {
  DBI::dbConnect(
    drv = RPostgres::Postgres(),
    dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "omop_test"),
    host = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "localhost"),
    port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
    user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
    password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
  )
}

#' Create a postgres source object
#'
#' @inheritParams pqSourceDoc
#'
#' @return A <pq_cdm> source object
#' @export
#'
postgresSource <- function(con,
                           cdmSchema = "public",
                           cdmPrefix = "",
                           writeSchema = "results",
                           writePrefix = "",
                           achillesSchema = NULL,
                           achillesPrefix = "") {
  # input checks
  con <- assertCon(con = con)
  cdmSchema <- assertSchema(con = con, schema = cdmSchema, null = FALSE)
  cdmPrefix <- assertPrefix(prefix = cdmPrefix)
  writeSchema <- assertSchema(con = con, schema = writeSchema, null = FALSE)
  writePrefix <- assertPrefix(prefix = writePrefix)
  achillesSchema <- assertSchema(con = con, schema = achillesSchema, null = TRUE)
  achillesPrefix <- assertPrefix(prefix = achillesPrefix)

  # create source
  source <- structure(
    .Data = list(),
    pq_con = con,
    cdm_schema = cdmSchema,
    cdm_prefix = cdmPrefix,
    write_schema = writeSchema,
    write_prefix = writePrefix,
    achilles_schema = achillesSchema,
    achilles_prefix = achillesPrefix,
    class = "pq_cdm"
  )

  # create source
  source <- omopgenerics::newCdmSource(src = source, sourceType = "postgres")

  return(source)
}

#' @export
insertTable.pq_cdm <- function(cdm, name, table, overwrite = TRUE, temporary = FALSE) {
  # initial checks
  omopgenerics::assertCharacter(name, length = 1)
  table <- dplyr::as_tibble(table)
  omopgenerics::assertLogical(overwrite, length = 1)
  omopgenerics::assertLogical(temporary, length = 1)

  # check overwrite
  if (overwrite & name %in% listTables(src = cdm, type = "write")) {
    dropTable(src = cdm, type = "write", name = name, callFrom = "insert_table")
  }

  # write table
  writeTable(src = cdm, name = name, value = table, type = "write")

  # indexes?

  readTable(src = cdm, name = name, type = "write")
}

#' @export
dropSourceTable.pq_cdm <- function(cdm, name) {
  dropTable(src = cdm, type = "write", name = name, callFrom = "drop_table")
}

#' @export
compute.pq_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, type = "write", logPrefix = NULL, jobName = NULL, ...) {
  # get source
  src <- attr(x, "tbl_source")
  con <- getCon(src)

  # find job name
  if (!is.null(jobName)) {
    jobName <- paste0(jobName, collapse = "; ")
  } else if (!is.null(logPrefix)) {
    jobName <- paste0(logPrefix, collapse = "; ")
  } else {
    jobName <- paste0("COMPUTE TABLE ", name)
  }

  # get rendered sql
  render <- as.character(dbplyr::sql_render(x))

  if (temporary) {
    type <- "temp"
    name <- omopgenerics::uniqueTableName()
  }

  ls <- listTables(src = src, type = type)

  formattedName <- formatName(src = src, name = name, type = type)
  if (stringr::str_detect(string = render, pattern = formattedName)) {
    # compute into intermediate table
    jn <- paste0(jobName, " into temp intermediate")
    intermediate <- omopgenerics::uniqueTableName()
    computeTable(src = src, type = "temp", name = intermediate, sql = render, jobName = jn)
    # delete blocking table
    dropTable(src = src, type = type, name = name, callFrom = "compute")
    # compute intermediate to final destination
    jn <- paste0(jobName, " from temp intermediate")
    sql <- paste0("SELECT * FROM ", formatName(src = src, name = intermediate, type = "temp"), ";")
    computeTable(src = src, type = type, name = name, sql = sql, jobName = jn)
    # delete intermediate table
    dropTable(src = src, type = "temp", name = intermediate, callFrom = "compute")
  } else {
    computeTable(src = src, type = type, name = name, sql = render, jobName = jobName)
  }

  # reference final table
  readTable(src = src, name = name, type = type)
}

#' @export
listSourceTables.pq_cdm <- function(cdm) {
  listTables(src = cdm, type = "write")
}

#' @export
cdmDisconnect.pq_cdm <- function(cdm, ...) {
  con <- getCon(cdm)
  DBI::dbDisconnect(conn = con)
  invisible(TRUE)
}

#' @export
cdmTableFromSource.pq_cdm <- function(src, value) {
  # check it is not data.frame
  if (inherits(value, "data.frame")) {
    cli::cli_abort(c(x = "To insert a local table to a cdm_reference object use insertTable function."))
  }

  # check it is lazy table
  if (!inherits(value, "tbl_PqConnection")) {
    cli::cli_abort(c(x = "Can't assign an object of class: {.cls {class(value)}} to a pq_cdm cdm_reference object."))
  }

  # check it comes from same connection
  con <- getCon(src)
  if (!identical(con, dbplyr::remote_con(value))) {
    cli::cli_abort(c(x = "The cdm object and the table have different connection sources."))
  }

  # check remote name
  remoteName <- dbplyr::remote_name(value)
  if (is.null(remoteName)) {
    name <- NA_character_
  } else if (startsWith(remoteName, "dbplyr") | startsWith(remoteName, "og_")) {
    name <- NA_character_
  } else {
    prefix <- getPrefix(src = src, type = "write")
    name <- substr(remoteName, nchar(prefix) + 1, nchar(remoteName))
  }

  omopgenerics::newCdmTable(table = value, src = src, name = name)
}

#' @export
insertCdmTo.pq_cdm <- function(cdm, to) {
  # identify table types
  tables <- cdmTableClasses(cdm)

  # insert omop tables
  for (nm in tables$omop_tables) {
    writeTable(src = to, name = nm, value = cdm[[nm]], type = "cdm")
  }

  # insert cohort tables
  for (nm in tables$cohort_tables) {
    x <- dplyr::collect(cdm[[nm]])
    writeTable(src = to, name = nm, value = x, type = "write")
    writeTable(src = to, name = paste0(nm, "_set"), value = attr(x, "cohort_set"), type = "write")
    writeTable(src = to, name = paste0(nm, "_attrition"), value = attr(x, "cohort_attrition"), type = "write")
    writeTable(src = to, name = paste0(nm, "_codelist"), value = attr(x, "cohort_codelist"), type = "write")
  }

  # insert achilles tables
  for (nm in tables$achilles_tables) {
    writeTable(src = to, name = nm, value = cdm[[nm]], type = "achilles")
  }

  # insert other tables
  for (nm in tables$other_tables) {
    writeTable(src = to, name = nm, value = cdm[[nm]], type = "write")
  }

  cdm <- cdmFromPostgres(
    con <- getCon(to),
    cdmName = omopgenerics::cdmName(cdm),
    cdmVersion = omopgenerics::cdmVersion(cdm),
    cdmSchema = getSchema(to, "cdm"),
    cdmPrefix = getPrefix(to, "cdm"),
    writeSchema = getSchema(to, "write"),
    writePrefix = getPrefix(to, "write"),
    achillesSchema = getSchema(to, "achilles"),
    achillesPrefix = getPrefix(to, "achilles"),
    cohortTables = tables$cohort_tables
  )

  for (nm in tables$other_tables) {
    cdm[[nm]] <- readTable(src = to, name = nm, type = "write")
  }

  # do we want to add indexes?

  return(cdm)
}

#' @export
readSourceTable.pq_cdm <- function(cdm, name) {
  readTable(src = cdm, name = name, type = "write")
}

computeTable <- function(src, type, name, sql, jobName) {
  # create sql
  name <- formatName(src = src, name = name, type = type)
  temp <- ifelse(type == "temp", " TEMP", "")
  sql <- paste0("CREATE", temp, " TABLE ", name, " AS ", sql, ";")

  # whether to log
  toLog <- logSql()

  # create log file
  if (toLog) {
    logName <- startLogger(
      jobName = jobName,
      jobType = "compute",
      sql = extractSql(sql = sql),
      explain = extractExplain(src = src, sql = sql)
    )
  }

  # finish logger
  if (toLog) {
    # analyse will also run the query
    analyse <- extractAnalyse(src = src, sql)
    finishLogger(logName = logName, analyse = analyse)
  } else {
    DBI::dbExecute(conn = getCon(src = src), statement = sql)
  }

  invisible(TRUE)
}
dropTable <- function(src, type, name, callFrom = "drop_table") {
  for (nm in name) {
    # create sql
    nm <- formatName(src = src, name = nm, type = type)
    st <- paste0("DROP TABLE IF EXISTS ", nm, ";")

    # whether to log
    toLog <- logSql()

    # create log file
    if (toLog) {
      logName <- startLogger(
        jobName = paste0("DROP TABLE ", nm, " (", type, ")"),
        jobType = "drop_table",
        callFrom = callFrom,
        sql = extractSql(sql = st),
        explain = NA_character_
      )
    }

    # drop table
    DBI::dbExecute(conn = getCon(src = src), statement = st)

    # finish logger
    if (toLog) {
      finishLogger(logName = logName, analyse = NA_character_)
    }
  }

  invisible(TRUE)
}
listTables <- function(src, type) {
  schema <- getSchema(src, type)
  if (schema == "") {
    st <- "SELECT tablename FROM pg_tables WHERE schemaname LIKE 'pg_temp%';"
  } else {
    st <- paste0("SELECT tablename FROM pg_tables WHERE schemaname = '", schema, "';")
  }
  x <- DBI::dbGetQuery(conn = getCon(src), statement = st)$tablename
  prefix <- getPrefix(src, type)
  if (prefix != "") {
    x <- x |>
      purrr::keep(\(x) startsWith(x = x, prefix = prefix)) |>
      stringr::str_replace(pattern = paste0("^", prefix), replacement = "") |>
      purrr::keep(\(x) nchar(x) > 0)
  }
  return(x)
}
writeTable <- function(src, name, value, type) {
  # whether to log
  toLog <- logSql()

  # start logger
  if (toLog) {
    con <- getCon(src = src)
    cols <- value |>
      purrr::imap(\(x, nm) paste0(nm, " ", DBI::dbDataType(dbObj = con, x))) |>
      paste0(collapse = ", ")
    nm <- formatName(src = src, name = name, type = type)
    sql <- paste0(
      "CREATE", ifelse(type == "temp", " TEMP", ""), " TABLE ", nm, " (", cols,
      ");"
    )
    logName <- startLogger(
      jobName = paste("INSERT IN", nm, "A TIBBLE WITH", nrow(value), "ROWS"),
      jobType = "insert_table",
      sql = extractSql(sql = sql),
      explain = NA_character_
    )
  }

  # insert table
  DBI::dbWriteTable(
    conn = getCon(src = src),
    name = IdName(src = src, name = name, type = type),
    value = dplyr::as_tibble(x = value),
    temporary = type == "temp"
  )

  # finish logger
  if (toLog) {
    finishLogger(logName = logName, analyse = NA_character_)
  }

  invisible(TRUE)
}
readTable <- function(src, name, type) {
  dplyr::tbl(src = getCon(src), I(formatName(src, name, type))) |>
    omopgenerics::newCdmTable(src = src, name = name)
}

getCon <- function(src) {
  attr(src, "pq_con")
}
getSchema <- function(src, type) {
  if (type == "cdm") {
    attr(src, "cdm_schema")
  } else if (type == "write") {
    attr(src, "write_schema")
  } else if (type == "achilles") {
    attr(src, "achilles_schema")
  } else if (type == "temp") {
    ""
  }
}
getPrefix <- function(src, type) {
  if (type == "cdm") {
    attr(src, "cdm_prefix")
  } else if (type == "write") {
    attr(src, "write_prefix")
  } else if (type == "achilles") {
    attr(src, "achilles_prefix")
  } else if (type == "temp") {
    ""
  }
}
formatName <- function(src, name, type) {
  schema <- getSchema(src, type)
  if (schema == "") {
    paste0(getPrefix(src, type), name)
  } else {
    paste0(schema, ".", getPrefix(src, type), name)
  }
}
IdName <- function(src, name, type) {
  schema <- getSchema(src, type)
  name <- paste0(getPrefix(src, type), name)
  if (schema == "") {
    DBI::Id(name = name)
  } else {
    DBI::Id(schema = schema, name = name)
  }
}
assertCon <- function(con, call = parent.frame()) {
  if (!inherits(con, "PqConnection")) {
    c(x = "`con` is not a {.cls pqConnection} object.") |>
      cli::cli_abort(call = call)
  }
  if (!DBI::dbIsValid(con)) {
    cli::cli_abort(c(x = "Connection is no longer valid."), call = call)
  }
  invisible(con)
}
assertSchema <- function(con, schema, null, call = parent.frame()) {
  omopgenerics::assertCharacter(schema, length = 1, null = null, call = call)
  emptySchema <- is.null(schema) | identical(schema, "")
  if (emptySchema) {
    if (null) {
      schema <- ""
    } else {
      cli::cli_abort(c(x = "Schema must be defined."))
    }
  } else {
    if (is.null(schema))
    if (!schemaExists(con, schema)) {
      if (question("Schema {.pkg {schema}} does not exist. Do you want to create it? Y/n")) {
        cli::cli_inform(c("i" = "Creating schema: {.pkg {schema}}."))
        createSchema(con, schema)
      } else {
        cli::cli_abort(c(x = "schema: {.pkg {schema}} does not exist."))
      }
    }
  }
  invisible(schema)
}
assertPrefix <- function(prefix, call = parent.frame()) {
  if (is.null(prefix)) {
    prefix <- ""
  } else {
    omopgenerics::assertCharacter(prefix, length = 1)
  }
  invisible(prefix)
}
