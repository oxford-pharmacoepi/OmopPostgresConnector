
createCdmIndexes <- function(cdm) {

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
