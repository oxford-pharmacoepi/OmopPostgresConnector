
currentDatabase <- function(con) {
  DBI::dbGetQuery(conn = con, statement = "SELECT current_database();") |>
    dplyr::pull("current_database")
}
createSchema <- function(con, schema) {
  DBI::dbExecute(conn = con, statement = paste0("CREATE SCHEMA ", schema))
  invisible(con)
}
deleteSchema <- function(con, schema) {
  DBI::dbExecute(conn = con, statement = paste0("DROP SCHEMA ", schema))
  invisible(con)
}
schemaExists <- function(con, schema) {
  x <- dplyr::tbl(con, I("information_schema.schemata")) |>
    dplyr::filter(.data$schema_name %in% .env$schema) |>
    dplyr::collect()
  nrow(x) > 0
}
