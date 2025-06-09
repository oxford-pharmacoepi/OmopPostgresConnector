
#' Create a <cdm_reference> object from a postgres connection
#'
#' @inheritParams pqSourceDoc
#' @param cdmName String, name of the cdm object.
#' @param cdmVersion String, OMOP CDM version of the cdm object.
#' @param cohortTables Character vector, names of the cohort tables.
#' @param validation Logical, wether to validate the cdm object or not.
#'
#' @return A <cdm_reference> object.
#' @export
#'
cdmFromPostgres <- function(con,
                            cdmName = NULL,
                            cdmSchema = "public",
                            cdmPrefix = "",
                            writeSchema = "results",
                            writePrefix = "",
                            achillesSchema = NULL,
                            achillesPrefix = "",
                            cdmVersion = NULL,
                            cohortTables = character(),
                            validation = TRUE) {
  # initial checks
  omopgenerics::assertCharacter(cdmName, length = 1, null = TRUE)
  omopgenerics::assertCharacter(cdmVersion, length = 1, null = TRUE)
  omopgenerics::assertCharacter(cohortTables, null = TRUE)

  # create source
  src <- postgresSource(
    con = con,
    cdmSchema = cdmSchema,
    cdmPrefix = cdmPrefix,
    writeSchema = writeSchema,
    writePrefix = writePrefix,
    achillesSchema = achillesSchema,
    achillesPrefix = achillesPrefix
  )

  # read omop tables
  omopTables <- listTables(src = src, type = "cdm")
  omopTables <- omopTables[omopTables %in% omopgenerics::omopTables(version = cdmVersion %||% "5.3")]
  tables <- omopTables |>
    rlang::set_names() |>
    as.list() |>
    purrr::map(\(x) readTable(src = src, name = x, type = "cdm"))
  if (is.null(cdmName)) {
    if ("cdm_source" %in% names(cdm)) {
      if ("cdm_source_name" %in% colnames(cdm$cdm_source)) {
        cdmSourceName <- cdm[["source_name"]] |>
          dplyr::pull("cdm_source_name")
        if (length(cdmSourceName) == 1) {
          cdmName <- cdmSourceName
        }
      }
    }
  }
  cdm <- omopgenerics::newCdmReference(
    tables = tables,
    cdmName = cdmName,
    cdmVersion = cdmVersion,
    .softValidation = !validation
  )

  # read cohort tables
  lt <- listTables(src = src, type = "write")
  sp <- getSchemaPrefix(src = src, type = "write")
  for (ct in cohortTables) {
    if (!ct %in% lt) {
      cli::cli_inform(c("!" = "{.cls cohort_table} {.strong {ct}} not present in `{.emph {sp}}`."))
    } else {
      nms <- paste0(ct, c("", "_set", "_attrition", "_codelist"))
      x <- nms |>
        rlang::set_names() |>
        as.list() |>
        purrr::map(\(nm) {
          if (nm %in% lt) {
            readTable(src = src, name = nm, type = "write")
          } else {
            NULL
          }
        })
      # need to separate in two steps so it gets the cdm attribute
      cdm[[ct]] <- x[[1]]
      cdm[[ct]] <- omopgenerics::newCohortTable(
        table = cdm[[ct]],
        cohortSetRef = x[[2]],
        cohortAttritionRef = x[[3]],
        cohortCodelistRef = x[[4]],
        .softValidation = !validation
      )
    }
  }

  # read achilles tables
  achillesTables <- listTables(src = src, type = "achilles")
  achillesTables <- achillesTables[achillesTables %in% omopgenerics::achillesTables(version = omopgenerics::cdmVersion(cdm))]
  if (length(achillesTables) > 0) {
    cli::cli_inform(c("i" = "Reading `achilles tables`: {.pkg achillesTables}"))
    for (nm in achillesTables) {
      cdm[[nm]] <- readTable(src = src, name = nm, type = "achilles") |>
        omopgenerics::newAchillesTable(version = omopgenerics::cdmVersion(cdm))
    }
  }

  cdm
}
getSchemaPrefix <- function(src, type) {
  prefix <- getPrefix(src = src, type = type)
  schema <- getSchema(src = src, type = type)
  if (prefix == "") {
    return(schema)
  } else {
    return(paste0(schema, ".", prefix))
  }
}
