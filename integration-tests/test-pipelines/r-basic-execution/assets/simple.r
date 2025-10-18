# @bruin.name: r_simple_asset
# @bruin.type: r

# Simple R script that prints output
cat("Hello from R!\n")
cat("This is a basic R asset execution test\n")

# Basic calculation
result <- 2 + 2
cat(sprintf("2 + 2 = %d\n", result))

# Verify we're running from the correct directory
cat(sprintf("Working directory: %s\n", getwd()))
