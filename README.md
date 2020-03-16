# SQLTypeR

[![Build Status](https://travis-ci.org/eccc-cdo/SQLTypeR.svg?branch=master)](https://travis-ci.org/eccc-cdo/SQLTypeR)
[![Coverage Status](https://coveralls.io/repos/github/eccc-cdo/SQLTypeR/badge.svg?branch=master)](https://coveralls.io/github/eccc-cdo/SQLTypeR?branch=master)

## Overview
This package allows you to write dataframes with logical and factor columns into an SQLite file, and then read those dataframes from SQLite with the logical and factors columns intact.

## Motivation
SQLite does not have native data types to handle factors or logicals. When R writes a dataframe to SQLite, it coerces those to characters and integers (1|0), respectively.

This means that on a round-trip of writing to then loading dataframes from SQLite, logical and factor fields are lost.

## Implementation
This package creates a `__types` metadata table in the SQLite database to store additional information for logical and factor fields in saved dataframes.

When loading a dataframe from the SQLite database, we recombine the SQL data table with the metadata, restoring the original dataframe.
