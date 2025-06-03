
createIndex <- function(table, column) {
  # existingIndex <- DBI::dbGetQuery(con,
  #                                  paste0("SELECT * FROM pg_indexes WHERE",
  #                                         " schemaname = '",
  #                                         schema,
  #                                         "' AND tablename = '",
  #                                         paste0(prefix, name),
  #                                         "';"))
  # if(nrow(existingIndex) > 0){
  #   cli::cli_inform("Index already existing so no new index added.")
  #   return(invisible(NULL))
  # } else {
  #   cli::cli_inform("Adding indexes to table")
  # }
  #
  # cols <- paste0(cols, collapse = ",")
  #
  # query <- paste0(
  #   "CREATE INDEX ON ",
  #   paste0(schema, ".", prefix, name),
  #   " (",
  #   cols,
  #   ");"
  # )
}

createCdmIndexes <- function(cdm) {

}

getIndexes <- function(con, schema = NULL, table = NULL, column = NULL) {
  x <- dplyr::tbl(con, dplyr::sql("SELECT * FROM pg_indexes"))
  if (!is.null(schema)) {
    x <- x |>
      dplyr::filter(.data$schemaname %in% .env$schema)
  }
  if (!is.null(table)) {
    x <- x |>
      dplyr::filter(.data$tablename %in% .env$table)
  }

}
indexExists <- function(con, schema, table, column) {
}
