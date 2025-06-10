
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
  dplyr::mutate(index_schema = "cdm", table_class = "omop_table")

cohortIndexes <- dplyr::tribble(
  ~index_name, ~index_table, ~index_schema, ~index, ~table_class,
  "{index_table}_cdi_si_csd", "-", "write", "cohort_definition_id, subject_id, cohort_start_date", "cohort_table"
)

achillesIndexes <- dplyr::tribble(
  ~index_name, ~index_table, ~index_schema, ~index, ~table_class,
  "idx_achilles_results_analysis_id", "achilles_results", "achilles", "analysis_id", "achilles_table",
  "idx_achilles_results_dist_analysis_id", "achilles_results_dist", "achilles", "analysis_id", "achilles_table"
)

otherIndexes <- dplyr::tribble(
  ~index_name, ~index_table, ~index_schema, ~index, ~table_class,
  "{index_table}_subject_id", "-", "write", "subject_id", "other_table",
  "{index_table}_person_id", "-", "write", "person_id", "other_table",
  "{index_table}_<prefix>_concept_id", "-", "write", "<prefix>_concept_id", "other_table"
)

expectedIdx <- cdmIndexes |>
  dplyr::union_all(cohortIndexes) |>
  dplyr::union_all(achillesIndexes) |>
  dplyr::union_all(otherIndexes)

usethis::use_data(expectedIdx, internal = TRUE, overwrite = TRUE)
