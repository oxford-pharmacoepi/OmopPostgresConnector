
#' Create the cdm
#'
#' @inheritParams cdmDoc
#'
#' @return The cdm object.
#' @export
#'
createCdmIndexes <- function(cdm) {

}

#' Title
#'
#' @inheritParams tableDoc
#'
#' @return The same <cdm_table> object.
#' @export
#'
createTableIndexes <- function(table) {

}

#' Title
#'
#' @param x A PqConnection, cdm_reference, cdm_table or pq_cdm object.
#' @param schema schema
#' @param tableName tableName
#' @param index index to create
#'
#' @return The same x object.
#' @export
#'
createIndexes <- function(x, schema, tableName, index) {

}

#' Identify the existing indexes in the tables of a <cdm_reference> object.
#'
#' @inheritParams cdmDoc
#'
#' @return A tibble object with the existing indexes.
#' @export
#'
existingCdmIndexes <- function(cdm) {
  # initial check
  omopgenerics::validateCdmArgument(cdm = cdm)

  # get src
  src <- omopgenerics::cdmSource(cdm)
  con <- getCon(src)
  x <- cdmTableClasses(cdm = cdm)

  # get cdm_schema indexes
  schema <- getSchema(src, "cdm")
  nms <- paste0(getPrefix(src, "cdm"), x$omop_tables)
  idx_cdm <- getIndexes(con = con, schema = schema, table = nms)

  # get write_schema indexes
  schema <- getSchema(src, "write")
  nms <- paste0(getPrefix(src, "write"), c(x$cohort_tables, x$other_tables))
  idx_write <- getIndexes(con = con, schema = schema, table = nms)

  # get achilles_schema indexes
  schema <- getSchema(src, "achilles")
  nms <- paste0(getPrefix(src, "write"), x$achilles_tables)
  idx_achilles <- getIndexes(con = con, schema = schema, table = nms)

  dplyr::bind_rows(idx_cdm, idx_write, idx_achilles)
}

#' Title
#'
#' @inheritParams tableDoc
#'
#' @return A tibble object with the existing indexes.
#' @export
#'
existingTableIndexes <- function(table) {
  omopgenerics::validateCdmTable(table = table)
  rnm <- dbplyr::remote_name(table)

  # check table is not a query
  if (is.null(rnm)) {
    cli::cli_inform(c("!" = "Table is query and not a reference to a table."))
    return(emptyIndexesMatrix())
  }

  # check is not temp table
  if (is.na(omopgenerics::tableName(table))) {
    cli::cli_inform(c("!" = "Temp tables do not have indexes."))
    return(emptyIndexesMatrix())
  }

  src <- omopgenerics::cdmSource(table)
  con <- getCon(src)
  if (stringr::str_detect(string = rnm, pattern = "\\.")) {
    rnm <- stringr::str_split_1(string = rnm, pattern = "\\.")
    schema <- rnm[1]
    name <- rnm[2]
  } else {
    schema <- DBI::dbGetQuery(con, "SELECT current_schema();")$current_schema
    name <- rnm
  }
  return(getIndexes(con = con, schema = schema, table = name))
}

#' Retrieve the indexes that exist in database.
#'
#' @param x A PqConnection, cdm_reference, cdm_table or pq_cdm object.
#' @param schema schema
#' @param tableName tableName
#'
#' @return A tibble object with the existing indexes.
#' @export
#'
existingIndexes <- function(x, schema = NULL, tableName = NULL) {
  # input check
  if (inherits(x, "cdm_reference") | inherits(x, "cdm_table")) {
    x <- omopgenerics::cdmSource(x)
  }
  if (inherits(x, "pq_cdm")) {
    x <- getCon(x)
  }
  if (!inherits(x, "PqConnection")) {
    cli::cli_abort(c(x = "{.cls PqConnection} not found."))
  }
  con <- assertCon(x)
  omopgenerics::assertCharacter(schema, null = TRUE)
  omopgenerics::assertCharacter(tableName, null = TRUE)

  # get indexes
  getIndexes(con = con, schema = schema, table = tableName)
}

#' Create the cdm
#'
#' @inheritParams cdmDoc
#'
#' @return The cdm object.
#' @export
#'
checkCdmIndexes <- function(cdm) {
  # initial checks
  cdm <- omopgenerics::validateCdmArgument(cdm = cdm)

  # get existing indexes
  src <- omopgenerics::cdmSource(x = cdm)
  con <- getCon(src =)
  idx <- getIndexes(con = con, schema = schema, table = tableName)


}

checkTableIndexes <- function(table) {

}

checkIndexes <- function(x, schema = NULL, tableName = NULL) {

}

expectedCdmIndexes <- function(cdm) {
  # input check
  cdm <- omopgenerics::validateCdmArgument(cdm = cdm)

  x <- cdmTableClasses(cdm = cdm)

  expectedIndex(tableName = x$omop_tables, tableClass = "omop_table")
}

expectedTableIndexes <- function(table) {

}

expectedIndexes <- function(x, schema, tableName) {

}

expectedIndex <- function(tableName = NULL, tableClass, columns = NULL) {
  expIdx <- expectedIdx |>
    dplyr::filter(.data$table_class %in% .env$tableClass)
  if (tableClass %in% c("omop_table", "achilles_table")) {
    expIdx <- expIdx |>
      dplyr::filter(.data$index_table == .env$tableName)
  } else if (tableClass == "cohort_table") {

  } else if (tableClass == "other_table") {

  }
  expIdx |>
    dplyr::select("index_name", "index_table", "index_column" = "index")
}
emptyIndexesMatrix <- function() {
  c("schemaname", "tablename", "indexname", "tablespace", "indexdef", "index") |>
    rlang::set_names() |>
    as.list() |>
    dplyr::as_tibble() |>
    utils::head(0)
}
createIndex <- function(con, schema, table, index) {
  if (indexExists(con, schema, table, index)) {
    cli::cli_inform("Index already existing so no new index added.")
  } else {
    cli::cli_inform("Adding indexes to table")
    st <- paste0("CREATE INDEX ON ", schema, ".", table, " (", index, ")")
    DBI::dbExecute(conn = con, statement = st)
  }
  invisible(con)
}
getIndexes <- function(con, schema = NULL, table = NULL) {
  x <- dplyr::tbl(con, I("pg_indexes"))
  if (!is.null(schema)) {
    x <- x |>
      dplyr::filter(.data$schemaname %in% .env$schema)
  }
  if (!is.null(table)) {
    x <- x |>
      dplyr::filter(.data$tablename %in% .env$table)
  }
  x |>
    dplyr::collect() |>
    dplyr::mutate(index = stringr::str_extract(
      string = .data$indexdef, pattern = "(?<=\\()[^)]*(?=\\))"
    ))
}
indexExists <- function(con, schema, table, index) {
  index %in% getIndexes(con, schema, table)$index
}
