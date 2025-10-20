""" @bruin
name: r_asset_with_renv
type: r
@bruin """

# R script that uses jsonlite package from renv
library(jsonlite)

cat("Testing renv dependency management\n")

# Create a simple JSON object
data <- list(
  name = "R Asset",
  version = "1.0",
  has_renv = TRUE
)

# Convert to JSON
json_str <- toJSON(data, auto_unbox = TRUE, pretty = TRUE)
cat("Generated JSON:\n")
cat(json_str)
cat("\n")

cat("renv test completed successfully!\n")
