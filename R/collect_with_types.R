#' @importFrom magrittr "%>%"
.collect_meta <- function(con, tbl) {
  if (!DBI::dbExistsTable(con, '__types')) {
    warning('__types does not exist, running collect() instead.')
    return(dplyr::collect(tbl))
  }

  table_filter <- as.character(dbplyr::remote_name(tbl))
  dplyr::tbl(con, '__types') %>%
    dplyr::filter(table == table_filter) %>%
    dplyr::select(-table) %>%
    dplyr::collect()
}

#' @importFrom magrittr "%>%"
.get_logical_meta <- function(meta) {
  dplyr::filter(meta, type == 'logical') %>%
    dplyr::select(var) %>%
    dplyr::pull()
}

#' @importFrom magrittr "%>%"
.get_factor_meta <- function(meta) {
  dplyr::filter(meta, type == 'factor') %>%
    dplyr::select(var, levels) %>%
    purrr::transpose() %>%
    purrr::map(
      function(el) {
        el$levels <-  jsonlite::parse_json(el$levels)
        el
      }
    )
}

#' @importFrom magrittr "%>%"
.restore_types <- function(tbl, meta_logical, meta_factor) {
  collection <- tbl %>%
    dplyr::collect() %>%
    dplyr::mutate_at(meta_logical, as.logical)

  for (el in meta_factor) {
    collection[[el$var]] <- forcats::fct_relevel(
      collection[[el$var]],
      unlist(el$levels$levels)
    )
    if (el$level$ordered) {
      collection[[el$var]] <- as.ordered(collection[[el$var]])
    }
  }
  collection
}

#' Force computation of a database query, restoring R types
#'
#' collect_with_types() works exactly like [dplyr::collect()] with the
#' exception that it restores the original R types not saved by the database,
#' specifically logicals (booleans) and factor levels. It does this by looking
#' up this information generated from the [dbWriteTable2()] function, which
#' stores a *__types* table in the database for this information.
#'
#' @param tbl An SQL table pointer (generated via [dplyr::tbl()]).
#'
#' @seealso [dplyr::collect()]
#' @export
collect_with_types <- function(tbl) {
  con <- dbplyr::remote_con(tbl)
  meta <- .collect_meta(con, tbl)
  meta_logical <- .get_logical_meta(meta)
  meta_factor <- .get_factor_meta(meta)
  .restore_types(tbl, meta_logical, meta_factor)
}
