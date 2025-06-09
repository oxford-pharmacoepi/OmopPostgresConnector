
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
question <- function(message, .envir = parent.frame()) {
  if (!rlang::is_interactive()) return(TRUE)
  res <- ""
  while (!res %in% c("yes", "no")) {
    cli::cli_inform(message = message, .envir = .envir)
    res <- tolower(readline())
    res[res == "y"] <- "yes"
    res[res == "n"] <- "no"
  }
  res == "yes"
}
