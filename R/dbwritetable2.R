#' @importFrom magrittr "%>%"
.process_logicals <- function(df, name) {
  df %>%
    purrr::map_lgl(is.logical) %>%
    purrr::keep(~ .x) %>%
    names() %>%
    purrr::map(~ c(name, .x, 'logical', '')) %>%
    unlist()
}

#' @importFrom magrittr "%>%"
.process_factors <- function(df, name) {
  df %>%
    dplyr::select_if(is.factor) %>%
    purrr::imap(
      function(col, col_name) {
        ordered = is.ordered(col)
        levels = attributes(col)$levels
        c(
          name,
          col_name,
          'factor',
          jsonlite::toJSON(
            list(ordered = ordered, levels = levels), auto_unbox = TRUE
          )
        )
      }
    )
}

.fix_meta <- function(meta){
  dplyr::bind_rows(
    tibble::tibble(
      table = character(),
      var = character(),
      type = character(),
      levels = character()
    ),
    meta
  )
}

.create_meta <- function(logicals, factors) {
  tibble::tribble(
    ~table, ~var, ~type, ~levels,
    !!!c(logicals, factors) %>%
      purrr::flatten()
  ) %>% .fix_meta
}

#' @importFrom magrittr "%>%"
.create_or_update_rtypes <- function(con, meta) {
  if (!DBI::dbExistsTable(con, '__types')) {
    DBI::dbCreateTable(con, '__types', meta)
    invisible(DBI::dbAppendTable(con, '__types', meta))
  } else {
    new_meta <- dplyr::tbl(con, '__types') %>%
      dplyr::collect() %>%
      dplyr::full_join(
        meta %>%
          dplyr::rename(levels_update = levels, type_update = type),
        by = c('table', 'var')
      ) %>%
      dplyr::mutate(
        type = ifelse(is.na(type_update), type, type_update),
        levels = ifelse(is.na(levels_update), levels, levels_update)
      ) %>%
      dplyr::select(-dplyr::ends_with('_update')) %>%
      .fix_meta
    DBI::dbWriteTable(con, '__types', new_meta, overwrite = TRUE)
  }
}

#' Copy data frames to database tables, saving R types
#'
#' dbWriteTable2() is a wrapper around [DBI::dbWriteTable()]; the key exception
#' is that in addition to saving the relevant database table, it also saves
#' a special *__types* table which stores R types which SQL databases do not
#' (necessarily) have, most notably logicals (booleans) in case of sqlite, and
#' factors more generally. Of course, *__types* becomes a **reserved** table,
#' and *should not be used for anything else nor changed manually*.
#'
#' @param con A [DBIConnection][DBI::DBIConnection-class] object,
#' as returned by [DBI::dbConnect()].
#' @param name A character string specifying the unqouted DBMS table name.
#' @param df a [data.frame][base::data.frame()] (or coercible to data.frame).
#' @param ... Other parameters passed on to methods.
#'
#' @seealso [DBI::dbWriteTable()]
#' @export
dbWriteTable2 <- function(con, name, df, ...) {
  logicals <- .process_logicals(df, name)
  factors <- .process_factors(df, name)
  meta <- .create_meta(logicals, factors)
  DBI::dbWriteTable(con, name, df, ...)
  .create_or_update_rtypes(con, meta)
}

