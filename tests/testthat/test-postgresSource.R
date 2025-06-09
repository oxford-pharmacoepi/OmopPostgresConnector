test_that("check postgres source", {
  skip_on_cran()

  # local connection works
  expect_no_error(con <- localPostgres())
  expect_true(inherits(con, "PqConnection"))

  # create postgres source
  expect_no_error(src <- postgresSource(con = con))

  # copy cdm to postgres
  cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed") |>
    omock::mockCohort() |>
    omopgenerics::emptyAchillesTable(name = "achilles_analysis") |>
    omopgenerics::emptyAchillesTable(name = "achilles_results") |>
    omopgenerics::emptyAchillesTable(name = "achilles_results_dist")
  cdm$my_random_table <- dplyr::tibble(person_id = 1L, value = "xyz")
  expect_no_error(pq_cdm <- insertCdmTo(cdm = cdm, to = src))

  # disconnect
  expect_no_error(dropCdm(cdm = pq_cdm))
})
