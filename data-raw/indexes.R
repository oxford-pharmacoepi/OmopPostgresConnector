
cdmIndexes <- readLines("https://raw.githubusercontent.com/OHDSI/CommonDataModel/refs/heads/main/inst/ddl/5.4/postgresql/OMOPCDM_postgresql_5.4_indices.sql") |>
  purrr::keep(\(x) startsWith(x, "CREATE INDEX")) |>
  stringr::str_replace_all(pattern = " ASC", replacement = "") |>
  purrr::map(\(x) {
    dplyr::tibble(
      index_name = stringr::str_match(x, "CREATE INDEX ([^ ]+)")[,2],
      index_table = stringr::str_match(x, "\\.([^ ]+)")[,2],
      index = stringr::str_match(x, "\\(([^)]+)\\)")[,2]
    )
  }) |>
  dplyr::bind_rows() |>
  dplyr::mutate(index_schema = "cdm")

cohortIndexes <- dplyr::tribble(
  ~index_name, ~index_table, ~index_schema, ~index,
  "{index_table}_cdi_si_csd", "cohort", "write", "cohort_definition_id, subject_id, cohort_start_date"
)

achillesIndexes <- dplyr::tribble(
  ~index_name, ~index_table, ~index_schema, ~index,
  "idx_achilles_results_analysis_id", "achilles_results", "achilles", "analysis_id",
  "idx_achilles_results_dist_analysis_id", "achilles_results_dist", "achilles", "analysis_id"
)

usethis::use_data(cdmIndexes, cohortIndexes, achillesIndexes, internal = TRUE, overwrite = TRUE)
