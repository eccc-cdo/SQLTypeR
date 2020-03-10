dt <- tibble::tibble(
  a = c(1:3),
  b = c(TRUE, FALSE, TRUE),
  c = letters[1:3],
  d = c(FALSE, FALSE, TRUE),
  e = factor(letters[1:3]),
  f = ordered(letters[4:6])
)

test_that('.process_logicals is character.', {
  expect_type(.process_logicals(dt, 'test-name'), 'character')
})

test_that('.process_logicals length is multiple of 4.', {
  object <- .process_logicals(dt, 'test-name')
  remainder <- length(object) %% 4
  expect_equal(remainder, 0)
})

test_that('.process_factors is a list', {
  expect_type(.process_factors(dt, 'test-name'), 'list')
})

test_that('.process_factors length is equal to factor elems of df.', {
  object <- .process_factors(dt, 'test-name')
  ix <- 0
  for (col in dt) {
    if (is.factor(col)) ix <- ix + 1
  }
  expect_length(object, ix)
})

test_that('.create_meta returns the correctly formatted tibble.', {
  tbl <- .create_meta(
    .process_logicals(dt, 'test'), .process_factors(dt, 'test')
  )
  tbl_check <- tibble::tribble(
    ~table, ~var, ~type, ~levels,
    'test', 'b', 'logical', '',
    'test', 'd', 'logical', '',
    'test', 'e', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = levels(dt$e)
    ), auto_unbox = TRUE),
    'test', 'f', 'factor', jsonlite::toJSON(list(
      ordered = TRUE, levels = levels(dt$f)
    ), auto_unbox = TRUE)
  )
  expect_identical(tbl, tbl_check)
})

test_that('.create_or_update_rtypes creates __types table if not exist.', {
  meta <- .create_meta(
    .process_logicals(dt, 'test'), .process_factors(dt, 'test')
  )

  db_table <- tibble::tribble(
    ~table, ~var, ~type, ~levels,
    'test', 'b', 'logical', '',
    'test', 'd', 'logical', '',
    'test', 'e', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = levels(dt$e)
    ), auto_unbox = TRUE),
    'test', 'f', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = levels(dt$f)
    ), auto_unbox = TRUE),
    'test2', 'ab', 'logical', ''
  )

  con <- DBI::dbConnect(RSQLite::SQLite(), ':memory:')
  .create_or_update_rtypes(con, meta)
  expect_true(DBI::dbExistsTable(con, '__types'))
  DBI::dbDisconnect(con)
})

test_that('.create_or_update_rtypes updates correctly.', {
  meta <- .create_meta(
    .process_logicals(dt, 'test'), .process_factors(dt, 'test')
  )

  db_table <- tibble::tribble(
    ~table, ~var, ~type, ~levels,
    'test', 'b', 'logical', '',
    'test', 'd', 'logical', '',
    'test', 'e', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = levels(dt$e)
    ), auto_unbox = TRUE),
    'test', 'f', 'factor', jsonlite::toJSON(list(
      ordered = FALSE, levels = levels(dt$f)
    ), auto_unbox = TRUE),
    'test2', 'ab', 'logical', ''
  )

  con <- DBI::dbConnect(RSQLite::SQLite(), ':memory:')
  DBI::dbCreateTable(con, '__types', db_table)
  DBI::dbAppendTable(con, '__types', db_table)
  .create_or_update_rtypes(con, meta)

  rtypes <- dplyr::tbl(con, '__types') %>%
    dplyr::collect() %>%
    dplyr::arrange(table, var)

  # Update should now render factor as ordered. The earilier entries should
  # also be retained.
  db_table_expect <- db_table %>%
    dplyr::arrange(table, var) %>%
    dplyr::mutate(
      levels = ifelse(
        table == 'test' & var == 'f',
        jsonlite::toJSON(
          list(ordered = TRUE, levels = levels(dt$f)), auto_unbox = TRUE
        ),
        levels
      )
    )

  expect_identical(rtypes, db_table_expect)
  DBI::dbDisconnect(con)
})

test_that('dbWriteTable2 produces __types with proper roundtrip info.', {
  con <- DBI::dbConnect(RSQLite::SQLite(), ':memory:')
  dbWriteTable2(con, 'test_table', dt)
  tbl <- collect_with_types(dplyr::tbl(con, 'test_table'))
  expect_identical(dt, tbl)
  DBI::dbDisconnect(con)
})
