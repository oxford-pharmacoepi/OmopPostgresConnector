
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
# compute.pq_cdm

#' @export
listSourceTables.pq_cdm <- function(cdm) {
  listTables(src = cdm, type = "write")
}

# cdmDisconnect.pq_cdm
# cdmTableFromSource.pq_cdm

#' @export
insertCdmTo.pq_cdm <- function(cdm, to) {
  # identify table types
  tables <- names(cdm)
  tableType <- purrr::map(tables, \(nm) {
    cl <- class(cdm[[nm]])
    dplyr::case_when(
      "omop_table" %in% class(x) ~ "omop_table",
      "cohort_table" %in% class(x) ~ "cohort_table",
      "achilles_table" %in% class(x) ~ "achilles_table",
      .default = "other_table"
    )
  })

  # insert cdm tables
  omopTables <- tables[tableType == "omop_table"]
  for (nm in omopTables) {
    writeTable(src = to, name = nm, value = dplyr::collect(cdm[[nm]]), type = "cdm")
  }

  # insert cohort tables
  cohortTables <- tables[tableType == "cohort_table"]
  for (nm in cohortTables) {
    x <- dplyr::collect(cdm[[nm]])
    writeTable(src = to, name = nm, value = x, type = "write")
    writeTable(src = to, name = paste0(nm, "_set"), value = attr(x, "cohort_set"), type = "write")
    writeTable(src = to, name = paste0(nm, "_attrition"), value = attr(x, "cohort_attrition"), type = "write")
    writeTable(src = to, name = paste0(nm, "_codelist"), value = attr(x, "cohort_codelist"), type = "write")
  }

  # insert achilles tables
  achillesTables <- tables[tableType == "achilles_table"]
  for (nm in achillesTables) {
    writeTable(src = to, name = nm, value = dplyr::collect(cdm[[nm]]), type = "achilles")
  }

  # insert other tables
  otherTables <- tables[tableType == "other_table"]
  for (nm in otherTables) {
    writeTable(src = to, name = nm, value = dplyr::collect(cdm[[nm]]), type = "write")
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
    cohortTables = cohortTables
  )

  for (nm in otherTables) {
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
  st <- paste0("DROP TABLE ", name, ";")
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
    value = value
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
