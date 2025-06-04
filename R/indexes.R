
#' Title
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
#' @param table table
#' @param index index to create
#'
#' @return The same x object.
#' @export
#'
createIndexes <- function(x, schema, table, index) {

}

#' Identify the existing indexes in the tables of a <cdm_reference> object.
#'
#' @inheritParams cdmDoc
#'
#' @return A tibble object with the existing indexes.
#' @export
#'
#' @examples
existingCdmIndexes <- function(cdm) {
  # initial check
  omopgenerics::validateCdmArgument(cdm = cdm)

  # get src
  src <- omopgenerics::cdmSource(cdm)
  con <- getCon(src)

  # get cdm_schema indexes
  schema <- getSchema(src, "cdm")
  nms <- formatName(src, names(cdm), "cdm")
  idx_cdm <- getIndexes(con = con, schema = schema, table = nms)

  # get write_schema indexes
  schema <- getSchema(src, "write")
  nms <- formatName(src, names(cdm), "write")
  idx_write <- getIndexes(con = con, schema = schema, table = nms)

  # get achilles_schema indexes
  schema <- getSchema(src, "achilles")
  nms <- formatName(src, names(cdm), "achilles")
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
  # if (!is.null(rnm)) {
  #
  #   rnm <- stringr::str_split_1(string = rnm, pattern = "\\.")
  #   src <- omopgenerics::cdmSource(table)
  #   con <- getCon(src)
  #   idx <- getIndexes(con = con, schema = rnm[1], name = rnm[2])
  # } else {
  #   cli::cli_inform(c("!" = "Table is query and not a reference to a table."))
  #   # missing indexes
  #   idx <- c("schemaname", "tablename", "indexname", "tablespace", "indexdef", "index") |>
  #     rlang::set_names() |>
  #     as.list() |>
  #     dplyr::as_tibble() |>
  #     utils::head(0)
  # }
  #
  # return(idx)
}

#' Retrieve the indexes that exist in database.
#'
#' @param x A PqConnection, cdm_reference, cdm_table or pq_cdm object.
#' @param schema schema
#' @param table table
#'
#' @return A tibble object with the existing indexes.
#' @export
#'
existingIndexes <- function(x, schema = NULL, table = NULL) {
  # input check
  if (inherits(x, "cdm_reference") | inherits(x, "cdm_table")) {
    x <- omopgenerics::cdmSource(x)
  }
  if (inherits("pq_cdm")) {
    x <- getCon(x)
  }
  if (!inherits(x, "PqConnection")) {
    cli::cli_abort(c(x = "{.cls PqConnection} not found."))
  }
  con <- assertCon(x)
  omopgenerics::assertCharacter(schema, null = TRUE)
  omopgenerics::assertCharacter(table, null = TRUE)

  # get indexes
  getIndexes(con = con, schema = schema, table = table)
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
