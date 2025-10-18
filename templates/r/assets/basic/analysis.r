# @bruin.name: basic_stats
# @bruin.type: r
# @bruin.description: Basic statistical analysis example showing R's capabilities

# Generate sample data
cat("Running basic statistical analysis\n")

# Create sample dataset
set.seed(42)
data <- data.frame(
  id = 1:100,
  value = rnorm(100, mean = 50, sd = 10),
  category = sample(c("A", "B", "C"), 100, replace = TRUE)
)

# Calculate statistics
cat(sprintf("Dataset size: %d observations\n", nrow(data)))
cat(sprintf("Mean value: %.2f\n", mean(data$value)))
cat(sprintf("Median value: %.2f\n", median(data$value)))
cat(sprintf("Standard deviation: %.2f\n", sd(data$value)))

# Group by category
cat("\nStatistics by category:\n")
for (cat_name in unique(data$category)) {
  cat_data <- data[data$category == cat_name, ]
  cat(sprintf("  %s: mean=%.2f, n=%d\n",
              cat_name,
              mean(cat_data$value),
              nrow(cat_data)))
}

cat("\nAnalysis complete!\n")
