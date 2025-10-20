""" @bruin
name: r_asset_with_secrets
type: r
secrets:
    - key: KEY1
      inject_as: INJECTED1
    - key: chess-default
@bruin """

# R script that tests environment variable injection (secrets/connections)
cat("Testing environment variable injection\n")

# Check for BRUIN_ASSET variable
bruin_asset <- Sys.getenv("BRUIN_ASSET")
if (nchar(bruin_asset) == 0) {
  stop("BRUIN_ASSET environment variable not set")
}
cat(sprintf("BRUIN_ASSET: %s\n", bruin_asset))

# Check for INJECTED1 (from KEY1 secret)
injected1 <- Sys.getenv("INJECTED1")
if (nchar(injected1) == 0) {
  stop("INJECTED1 environment variable not set")
}
if (injected1 != "value1") {
  stop(sprintf("INJECTED1 has wrong value: expected 'value1', got '%s'", injected1))
}
cat(sprintf("INJECTED1: %s (correct!)\n", injected1))

# Check for chess-default connection
chess_default <- Sys.getenv("chess-default")
expected_chess <- '{"name":"chess-default","players":["erik","vadimer2"]}'
if (nchar(chess_default) == 0) {
  stop("chess-default environment variable not set")
}
if (chess_default != expected_chess) {
  stop(sprintf("chess-default has wrong value:\nExpected: %s\nGot: %s", expected_chess, chess_default))
}
cat(sprintf("chess-default: %s (correct!)\n", chess_default))

cat("\nAll environment variable tests passed!\n")
