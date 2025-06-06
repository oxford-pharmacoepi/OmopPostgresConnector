
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
    dropTable(src = cdm, type = "write", name = name)
  }

  # write table
  writeTable(src = cdm, name = name, value = table, type = "write")

  # indexes?

  readTable(src = cdm, name = name, type = "write")
}

#' @export
dropSourceTable.pq_cdm <- function(cdm, name) {
  for (nm in name) {
    dropTable(src = cdm, type = "write", name = nm)
  }
  return(cdm)
}

#' @importFrom dplyr compute
compute.pq_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, type = "write", ...) {
  # get source
  src <- attr(table, "tbl_source")
  con <- getCon(src)

  # get rendered sql
  render <- as.character(dbplyr::sql_render(x))

  # check if table must be temporary or permanent
  if (temporary) {
    name <- omopgenerics::uniqueTableName()
    temp <- " TEMP"
  } else {
    name <- formatName(src = src, name = name, type = type)
    temp <- ""
  }

  # check if intermediate table is needed
  if (stringr::str_detect(string = render, pattern = name)) {
    intermediate <- omopgenerics::uniqueTableName()
    sql <- paste0("CREATE TEMP TABLE ", intermediate, " AS ", render, ";")
    DBI::dbExecute(conn = con, statement = sql)
    sql <- paste0("CREATE TABLE ", name, " AS SELECT * FROM ", intermediate, ";")
    DBI::dbExecute(conn = con, statement = sql)
    sql <- paste0("DROP TABLE IF EXISTS ", intermediate, ";")
    DBI::dbExecute(conn = con, statement = sql)
  } else {
    sql <- paste0("CREATE", temp, " TABLE ", name, " AS ", render, ";")
    DBI::dbExecute(conn = con, statement = sql)
  }

  # reference final table
  dplyr::tbl(con, I(name))
}

#' @export
listSourceTables.pq_cdm <- function(cdm) {
  listTables(src = cdm, type = "write")
}

#' @export
cdmDisconnect.pq_cdm <- function(cdm, ...) {
  src <- omopgenerics::cdmSource(cdm)
  con <- getCon(src)
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
  if (!inherits(value, "tbl_lazy")) {
    cli::cli_abort(c(x = "Can't assign an object of class: {.cls {class(value)}} to a db_con cdm_reference object."))
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

dropTable <- function(src, type, name) {
  name <- formatName(src = src, name = name, type = type)
  st <- paste0("DROP TABLE IF EXISTS ", name, ";")
  DBI::dbExecute(conn = getCon(src = src), statement = st)
}
listTables <- function(src, type) {
  schema <- getSchema(src, type)
  prefix <- getPrefix(src, type)
  st <- paste0(
    "SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '", schema, "'",
    ifelse(prefix == "", ";", paste0(" AND table_name LIKE '", prefix, "%';"))
  )
  x <- DBI::dbGetQuery(conn = getCon(src), statement = st)$table_name
  if (prefix != "") {
    x <- x |>
      stringr::str_replace(pattern = paste0("^", prefix), replacement = "") |>
      purrr::keep(\(x) nchar(x) > 0)
  }
  return(x)
}
writeTable <- function(src, name, value, type) {
  DBI::dbWriteTable(
    conn = getCon(src),
    name = formatName(src, name, type),
    value = dplyr::as_tibble(value)
  )
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
  }
}
getPrefix <- function(src, type) {
  if (type == "cdm") {
    attr(src, "cdm_prefix")
  } else if (type == "write") {
    attr(src, "write_prefix")
  } else if (type == "achilles") {
    attr(src, "achilles_prefix")
  }
}
formatName <- function(src, name, type) {
  if (type == "cdm") {
    paste0(getSchema(src, "cdm"), ".", getPrefix(src, "cdm"), name)
  } else if (type == "write") {
    paste0(getSchema(src, "write"), ".", getPrefix(src, "write"), name)
  } else if (type == "achilles") {
    paste0(getSchema(src, "achilles"), ".", getPrefix(src, "achilles"), name)
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
  if (!is.null(schema)) {
    if (!schemaExists(con, schema)) {
      if (question("Schema {.pkg {schema}} does not exist. Do you wnat to create it? Y/n")) {
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
conFromSource <- function(x) {
  attr(x, "pq_con")
}
