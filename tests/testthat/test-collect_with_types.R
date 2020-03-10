create_test_db <- function() {
  con <- DBI::dbConnect(RSQLite::SQLite(), ':memory:')
  con
}

create_test_table <- function(con) {
  dt <- tibble::tibble(
    a = c(1:3),
    b = c(TRUE, FALSE, TRUE),
    c = letters[1:3],
    d = c(FALSE, FALSE, TRUE),
    e = factor(letters[1:3]),
    f = ordered(letters[4:6])
  )
  suppressWarnings({
    DBI::dbCreateTable(con, 'test_table', dt)
    DBI::dbAppendTable(con, 'test_table', dt)
  })
}

create_test_types <- function(con) {
  types_tbl <- tibble::tribble(
    ~table, ~var, ~type, ~levels,
    'test_table', 'b', 'logical', '',
    'test_table', 'd', 'logical', '',
    'test_table', 'e', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = letters[1:3]
    ), auto_unbox = TRUE),
    'test_table', 'f', 'factor', jsonlite::toJSON(list(
      ordered = TRUE, levels = ordered(letters[4:6])
    ), auto_unbox = TRUE),
    'test_nontable', 'g', 'logical', '',
    'test_nontibble', 'h', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = c('l1', 'l2')
    ))
  )

  DBI::dbCreateTable(con, '__types', types_tbl)
  DBI::dbAppendTable(con, '__types', types_tbl)
}

test_that('.collect_meta warns if __types is not present.', {
  con <- create_test_db()
  create_test_table(con)
  tbl <- dplyr::tbl(con, 'test_table')
  expect_warning(.collect_meta(con, tbl), '__types does not exist')
  DBI::dbDisconnect(con)
})

test_that('.collect_meta filters the __types table appropriately.', {
  con <- create_test_db()
  create_test_table(con)
  create_test_types(con)
  tbl <- dplyr::tbl(con, 'test_table')
  meta <- .collect_meta(con, tbl)
  expect_equal(nrow(meta), 4)
  expect_equal(ncol(meta), 3)
  DBI::dbDisconnect(con)
})

test_that('.get_logical_meta filters the __types logicals appropriately.', {
  con <- create_test_db()
  create_test_table(con)
  create_test_types(con)
  tbl <- dplyr::tbl(con, 'test_table')
  meta <- .collect_meta(con, tbl)
  logicals <- .get_logical_meta(meta)
  expect_identical(logicals, c('b', 'd'))
  DBI::dbDisconnect(con)
})

test_that('.get_factor_meta filters the __types factors appropriately.', {
  con <- create_test_db()
  create_test_table(con)
  create_test_types(con)
  tbl <- dplyr::tbl(con, 'test_table')
  meta <- .collect_meta(con, tbl)
  factors <- .get_factor_meta(meta)

  expect_type(factors, 'list')
  expect_identical(
    unlist(purrr::transpose(factors)$var),
    c('e', 'f')
  )
  expect_identical(
    unlist(purrr::transpose(purrr::transpose(factors)$levels)$ordered),
    c(FALSE, TRUE)
  )
  expect_identical(
    unlist(factors[[1]]$levels$levels),
    c('a', 'b', 'c')
  )
  expect_identical(
    unlist(factors[[2]]$levels$levels),
    c('d', 'e', 'f')
  )
  DBI::dbDisconnect(con)
})

test_that('.restore_types creates an identical tbl to original.', {
  dt <- tibble::tibble(
    a = c(1:3),
    b = c(TRUE, FALSE, TRUE),
    c = letters[1:3],
    d = c(FALSE, FALSE, TRUE),
    e = factor(letters[1:3]),
    f = ordered(letters[4:6])
  )

  con <- create_test_db()
  create_test_table(con)
  create_test_types(con)
  tbl <- dplyr::tbl(con, 'test_table')
  meta <- .collect_meta(con, tbl)
  logicals <- .get_logical_meta(meta)
  factors <- .get_factor_meta(meta)
  dt_restore <- .restore_types(tbl, logicals, factors)

  expect_identical(dt, dt_restore)
  DBI::dbDisconnect(con)
})

test_that('collect_with_types creates an identical tbl to original.', {
  dt <- tibble::tibble(
    a = c(1:3),
    b = c(TRUE, FALSE, TRUE),
    c = letters[1:3],
    d = c(FALSE, FALSE, TRUE),
    e = factor(letters[1:3]),
    f = ordered(letters[4:6])
  )

  con <- create_test_db()
  create_test_table(con)
  create_test_types(con)
  tbl <- dplyr::tbl(con, 'test_table')
  expect_identical(dt, collect_with_types(tbl))
  DBI::dbDisconnect(con)
})
