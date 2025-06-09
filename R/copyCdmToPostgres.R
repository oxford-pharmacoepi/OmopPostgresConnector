
#' Copy a cdm (independently of its source) to a
#'
#' @inheritParams cdmDoc
#' @inheritParams pqSourceDoc
#'
#' @return A cdm reference inserted in the new pq_cdm source.
#' @export
#'
#' @examples
#' #library(omock)
#' #library(OmopPostgresConnector)
#' #
#' #pq <- localPostgres()
#' #cdm <- mockCdmFromDataset(datasetName = "GiBleed")
#' #
#' #pq_cdm <- copyCdmToPostgres(cdm = cdm, con = pq)
#' #pq_cdm
#'
copyCdmToPostgres <- function(cdm,
                              con,
                              cdmSchema = "public",
                              cdmPrefix = "",
                              writeSchema = "results",
                              writePrefix = "",
                              achillesSchema = NULL,
                              achillesPrefix = "") {
  # initial checks
  cdm <- omopgenerics::validateCdmArgument(cdm = cdm)
  tables <- cdmTableClasses(cdm = cdm)
  con <- assertCon(con = con)
  cdmSchema <- assertSchema(con = con, schema = cdmSchema)
  cdmPrefix <- assertPrefix(prefix = cdmPrefix)
  nullWrite <- length(c(tables$cohort_tables, tables$other_tables)) == 0
  writeSchema <- assertSchema(con = con, schema = writeSchema, null = nullWrite)
  writeSchema <- assertPrefix(prefix = writeSchema)
  nullAch <- length(tables$achilles_tables) == 0
  achillesSchema <- assertSchema(con = con, schema = achillesSchema, null = nullAch)
  achillesPrefix <- assertPrefix(prefix = achillesPrefix)

  # create postgres source
  src <- postgresSource(
    con = con, cdmSchema = cdmSchema, cdmPrefix = cdmPrefix,
    writeSchema = writeSchema, writePrefix = writePrefix,
    achillesSchema = achillesSchema, achillesPrefix = achillesPrefix
  )

  # insert cdm
  insertCdmTo(cdm = cdm, to = src)
}
cdmTableClasses <- function(cdm) {
  x <- dplyr::tibble(
    table = names(cdm),
    table_class = purrr::map_chr(names(cdm), \(nm) {
      cl <- class(cdm[[nm]])
      dplyr::case_when(
        "omop_table" %in% cl ~ "omop_table",
        "cohort_table" %in% cl ~ "cohort_table",
        "achilles_table" %in% cl ~ "achilles_table",
        .default = "other_table"
      )
    })
  )
  list(
    omop_tables = x$table[x$table_class == "omop_table"],
    cohort_tables = x$table[x$table_class == "cohort_table"],
    achilles_tables = x$table[x$table_class == "achilles_table"],
    other_tables = x$table[x$table_class == "other_table"]
  )
}
