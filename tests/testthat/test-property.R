library(hedgehog)

# Generate a random-leveled ordered/unordered factor column with `n` values.
gen_factor <- function(n) gen.and_then(
  gen.and_then(gen.int(26), function(m) gen.pure(letters[1:m])),
  function (levels) gen.map(
    function(args) do.call(factor,args),
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

test_that("All generated tibbles have length 5.",
  forall(gen_df, function(df) {expect_equal(nrow(df), 5)}, tests = 100)
)
