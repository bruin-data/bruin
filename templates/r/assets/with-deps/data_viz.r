"@bruin
name: data_visualization
type: r
depends:
    - basic_stats
description: Data visualization example using ggplot2 and dplyr
@bruin"

# This example uses packages managed by renv
# The renv.lock file in this directory specifies the exact versions
library(dplyr)
library(jsonlite)

cat("Running data analysis with packages\n")

# Create sample dataset
set.seed(123)
dates <- seq(as.Date("2024-01-01"), as.Date("2024-12-31"), by = "day")
n_days <- length(dates)

sales_data <- data.frame(
  date = dates,
  revenue = rnorm(n_days, mean = 10000, sd = 2000),
  region = sample(c("North", "South", "East", "West"), n_days, replace = TRUE)
)

# Perform analysis using dplyr
summary_stats <- sales_data %>%
  group_by(region) %>%
  summarise(
    total_revenue = sum(revenue),
    avg_revenue = mean(revenue),
    max_revenue = max(revenue),
    days = n()
  ) %>%
  arrange(desc(total_revenue))

cat("\nRevenue Summary by Region:\n")
print(summary_stats)

# Export results as JSON
results <- list(
  total_days = nrow(sales_data),
  total_revenue = sum(sales_data$revenue),
  by_region = summary_stats
)

json_output <- toJSON(results, pretty = TRUE, auto_unbox = TRUE)
cat("\nJSON Export:\n")
cat(json_output)
cat("\n")

cat("\nData analysis with dependencies complete!\n")
