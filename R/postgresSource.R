
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

# methods
insertTable.pq_cdm <- function(cdm, name, table, overwrite = TRUE, temporary = FALSE) {
  table <- dplyr::as_tibble(table)

}
# dropSourceTable.pq_cdm
# listSourceTables.pq_cdm
# compute.pq_cdm

listTables <- function(src, type) {
  schema <- getSchema(src, type)
  prefix <- getPrefix(src, type)
  st <- paste0(
    "SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '", schema, "'",
    ifelse(prefix == "", ";", paste0(" AND table_name LIKE '", prefix, "%';"))
  )
  DBI::dbGetQuery(conn = getCon(src), statement = st)
}
writeTable <- function(src, name, value, type) {
  DBI::dbWriteTable(
    conn = getCon(src),
    name = formatName(src, name, type),
    value = value
  )
  readTable(src = src, name = name, type = type)
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
      cli::cli_abort(c(x = "schema: {.pkg {schema}} does not exist."))
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
