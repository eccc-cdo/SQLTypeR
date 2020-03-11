library(hedgehog)

# ====================================
# Generators

# Generate 1 to 26 factor levels as unique single characters.
gen_factor_levels <- gen.with(
  gen.int(26),
  function(m) letters[1:m]
)

# Generate a random-leveled ordered/unordered factor column with `n` values.
gen_factor <- function(n) gen.and_then(
  gen_factor_levels,
  function (levels) gen.map(
    function(args) do.call(factor, args),
    list(
      x = gen.c(gen.element(levels), of = n),
      levels = levels,
      ordered = gen.element(c(FALSE, TRUE))
    )
  )
)

# Generate a random-typed column with `n` values.
gen_col <- function(n) gen.and_then(
  gen.element(c("factor","logical","integer","character")),
  function(type) {
    if (type == "factor") {gen_factor(n)}
    else if (type == "logical") {gen.c(gen.element(c(FALSE, TRUE)), of = n)}
    else if (type == "character") {gen.c(gen.element(letters), of = n)}
    else if (type == "integer") {gen.c(gen.int(10), of = n)}
  }
)

# Generate a tibble with 5 rows, with between 1 and 5 random columns.
gen_df <- gen.with(
  gen.list(gen_col(5), from = 1, to = 5),
  function(cols) do.call(dplyr::tibble, rlang::set_names(cols, letters[1:(length(cols))]))
)

# ====================================
# Generator Tests
test_that("All generated tibbles have length 5.",
  forall(gen_df, tests = 100, function(df) {
    expect_equal(nrow(df), 5)
}))

# ====================================
# dbwritetable2 tests
test_that('.process_logicals is character.',
  forall(gen_df, function(df) {
    expect_type(.process_logicals(df, 'test-name'), 'character')
}))

test_that('.process_logicals length is multiple of 4.',
  forall(gen_df, function(df) {
    object <- .process_logicals(df, 'test-name')
    remainder <- length(object) %% 4
    expect_equal(remainder, 0)
}))

test_that('.process_factors is a list',
  forall(gen_df, function(df) {
    expect_type(.process_factors(df, 'test-name'), 'list')
}))

test_that('.process_factors length is equal to factor elems of df.',
  forall(gen_df, function(df) {
    object <- .process_factors(dt, 'test-name')
    ix <- 0
    for (col in df) {
      if (is.factor(col)) ix <- ix + 1
    }
    expect_length(object, ix)
}))

# ====================================
# dbwritetable2 tests

test_that('.create_meta returns the correctly formatted tibble.',
  forall(gen_df, function(df) {
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
}))

test_that('.create_or_update_rtypes creates __types table if not exist.',
  forall(gen_df, function(df) {
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
}))

test_that('.create_or_update_rtypes updates correctly.',
  forall(gen_df, function(df) {
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
}))

test_that('dbWriteTable2 produces __types with proper roundtrip info.',
  forall(gen_df, function(df) {
    con <- DBI::dbConnect(RSQLite::SQLite(), ':memory:')
    dbWriteTable2(con, 'test_table', dt)
    tbl <- collect_with_types(dplyr::tbl(con, 'test_table'))
    expect_identical(dt, tbl)
    DBI::dbDisconnect(con)
}))
