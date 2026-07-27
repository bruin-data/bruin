#!/usr/bin/env bash

set -euo pipefail

merge_base_ref="${LINT_MERGE_BASE:-origin/main}"
module_dir="${LINT_MODULE_DIR:-.}"
build_tags="${LINT_BUILD_TAGS:-}"
concurrency="${LINT_CONCURRENCY:-4}"
timeout="${LINT_TIMEOUT:-10m}"
parallel_flags="${LINT_PARALLEL_FLAGS:---allow-parallel-runners}"
enable_only="${LINT_ENABLE_ONLY:-}"

merge_base="$(git merge-base "$merge_base_ref" HEAD)"

changed_files=()
while IFS= read -r file; do
  changed_files+=("$file")
done < <(
  {
    git diff --name-only --diff-filter=ACMRD "$merge_base" --
    git ls-files --others --exclude-standard
  } | sed '/^$/d' | LC_ALL=C sort -u
)

if [[ ${#changed_files[@]} -eq 0 ]]; then
  echo "No changes found."
  exit 0
fi

module_files=()
while IFS= read -r file; do
  module_files+=("$file")
done < <(git ls-files '*go.mod' | LC_ALL=C sort)

module_for_file() {
  local file="$1"
  local matched_module="."
  local module_file candidate

  for module_file in "${module_files[@]}"; do
    [[ "$module_file" == "go.mod" ]] && continue
    candidate="${module_file%/go.mod}"
    if [[ "$file" == "$candidate" || "$file" == "$candidate/"* ]]; then
      if [[ "$matched_module" == "." || ${#candidate} -gt ${#matched_module} ]]; then
        matched_module="$candidate"
      fi
    fi
  done

  printf '%s\n' "$matched_module"
}

lint_all=false
packages=()

for file in "${changed_files[@]}"; do
  case "$file" in
    .golangci.yml)
      lint_all=true
      ;;
    go.mod|go.sum)
      if [[ "$module_dir" == "." ]]; then
        lint_all=true
      fi
      ;;
    "$module_dir"/go.mod|"$module_dir"/go.sum)
      if [[ "$module_dir" != "." ]]; then
        lint_all=true
      fi
      ;;
    *.go)
      if [[ "$(module_for_file "$file")" != "$module_dir" ]]; then
        continue
      fi

      if [[ ! -e "$file" ]]; then
        lint_all=true
        continue
      fi

      relative_file="$file"
      if [[ "$module_dir" != "." ]]; then
        relative_file="${file#"$module_dir"/}"
      fi

      directory="$(dirname "$relative_file")"
      if [[ "$directory" == "." ]]; then
        packages+=("./")
      else
        packages+=("./$directory")
      fi
      ;;
  esac
done

if [[ "$lint_all" == true ]]; then
  packages=("./...")
elif [[ ${#packages[@]} -eq 0 ]]; then
  echo "No changed Go packages to lint in $module_dir."
  exit 0
else
  unique_packages=()
  while IFS= read -r package; do
    unique_packages+=("$package")
  done < <(printf '%s\n' "${packages[@]}" | LC_ALL=C sort -u)
  packages=("${unique_packages[@]}")
fi

args=(run --timeout "$timeout" --concurrency "$concurrency")

if [[ -n "$parallel_flags" ]]; then
  read -r -a parsed_parallel_flags <<< "$parallel_flags"
  args+=("${parsed_parallel_flags[@]}")
fi

if [[ -n "$build_tags" ]]; then
  args+=(--build-tags "$build_tags")
fi

if [[ -n "$enable_only" ]]; then
  args+=(--enable-only "$enable_only")
fi

echo "Module: $module_dir"
echo "Packages: ${packages[*]}"

if [[ "$module_dir" == "." ]]; then
  exec golangci-lint "${args[@]}" "${packages[@]}"
fi

cd "$module_dir"
exec golangci-lint "${args[@]}" "${packages[@]}"
