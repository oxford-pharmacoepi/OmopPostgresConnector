
testCon <- function() {
  DBI::dbConnect(
    drv = RPostgres::Postgres(),
    dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "omop_test"),
    host = "localhost",
    port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
    user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
    password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
  )
}
