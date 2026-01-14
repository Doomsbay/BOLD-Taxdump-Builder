#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

default_input="${root_dir}/BOLD_Public.26-Sep-2025/BOLD_Public.26-Sep-2025.tsv"
default_taxonkit="${root_dir}/taxonkit_input.tsv"
default_taxdump="${root_dir}/bolddb-taxdump"
default_marker="${root_dir}/marker_fastas"
default_releases="${root_dir}/releases"

input_tsv=""
taxonkit_input=""
taxdump_dir=""
marker_dir=""
release_dir=""
skip_manifest="false"
skip_checksums="false"
package_artifacts="false"

positional=()
while [[ $# -gt 0 ]]; do
  case "${1}" in
    -h|--help)
      cat <<'EOF'
Usage: bolddb-taxdump.sh [options] [input_tsv] [taxonkit_input] [taxdump_dir] [marker_dir] [releases_dir]

Options:
  --input PATH           BOLD TSV input (default: BOLD_Public.26-Sep-2025/BOLD_Public.26-Sep-2025.tsv)
  --taxonkit-input PATH  Output taxonkit_input.tsv path
  --taxdump-dir PATH     Output taxdump directory (default: bolddb-taxdump)
  --marker-dir PATH      Output marker FASTA directory (default: marker_fastas)
  --releases-dir PATH    Output releases directory (default: releases)
  --artifacts-dir PATH   Alias for --releases-dir (legacy)
  --package              Create release artifacts (zips, manifest, checksums)
  --skip-manifest        Do not generate releases/manifest.json
  --skip-checksums       Do not generate releases/SHA256SUMS.txt
  -h, --help             Show this help
EOF
      exit 0
      ;;
    --package)
      package_artifacts="true"
      shift
      ;;
    --input)
      input_tsv="${2:-}"
      shift 2
      ;;
    --taxonkit-input)
      taxonkit_input="${2:-}"
      shift 2
      ;;
    --taxdump-dir)
      taxdump_dir="${2:-}"
      shift 2
      ;;
    --marker-dir)
      marker_dir="${2:-}"
      shift 2
      ;;
    --releases-dir)
      release_dir="${2:-}"
      shift 2
      ;;
    --artifacts-dir)
      release_dir="${2:-}"
      shift 2
      ;;
    --skip-manifest)
      skip_manifest="true"
      shift
      ;;
    --skip-checksums)
      skip_checksums="true"
      shift
      ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do
        positional+=("${1}")
        shift
      done
      ;;
    -*)
      echo "Unknown option: ${1}" >&2
      exit 1
      ;;
    *)
      positional+=("${1}")
      shift
      ;;
  esac
done

if [[ ${#positional[@]} -gt 0 ]]; then
  input_tsv="${input_tsv:-${positional[0]}}"
  taxonkit_input="${taxonkit_input:-${positional[1]:-}}"
  taxdump_dir="${taxdump_dir:-${positional[2]:-}}"
  marker_dir="${marker_dir:-${positional[3]:-}}"
  release_dir="${release_dir:-${positional[4]:-}}"
fi

input_tsv="${input_tsv:-${default_input}}"
taxonkit_input="${taxonkit_input:-${default_taxonkit}}"
taxdump_dir="${taxdump_dir:-${default_taxdump}}"
marker_dir="${marker_dir:-${default_marker}}"
release_dir="${release_dir:-${default_releases}}"

input_base="$(basename "${input_tsv}")"
if [[ "${input_base}" == *.tsv.gz ]]; then
  dataset_tag="${input_base%.tsv.gz}"
else
  dataset_tag="${input_base%.*}"
fi

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

run_step() {
  local name="$1"
  shift
  step=$((step + 1))
  log "[$step/$total_steps] ${name}..."
  local t0=$SECONDS
  "$@"
  log "[$step/$total_steps] ${name} done in $((SECONDS - t0))s"
}

total_steps=3
if [[ "${package_artifacts}" == "true" ]]; then
  total_steps=$((total_steps + 1))
  if [[ "${skip_manifest}" != "true" ]]; then
    total_steps=$((total_steps + 1))
  fi
  if [[ "${skip_checksums}" != "true" ]]; then
    total_steps=$((total_steps + 1))
  fi
fi
step=0

run_step "Extract taxonomy" \
  "${root_dir}/scripts/01_extract_taxonomy_from_bold.sh" "${input_tsv}" "${taxonkit_input}"
run_step "Build taxdump" \
  "${root_dir}/scripts/02_build_ncbi_taxdump.sh" "${taxonkit_input}" "${taxdump_dir}"
run_step "Build marker FASTAs" \
  "${root_dir}/scripts/03_build_marker_fastas.sh" "${input_tsv}" "${marker_dir}"
if [[ "${package_artifacts}" == "true" ]]; then
  run_step "Package release artifacts" \
    "${root_dir}/scripts/04_package_reference_db.sh" "${release_dir}" "${taxdump_dir}" "${marker_dir}" "${dataset_tag}"
  if [[ "${skip_manifest}" != "true" ]]; then
    run_step "Generate manifest" \
      "${root_dir}/scripts/06_generate_manifest.sh" "${release_dir}" "${taxdump_dir}" "${marker_dir}" "${dataset_tag}" "${release_dir}/manifest.json"
  fi
  if [[ "${skip_checksums}" != "true" ]]; then
    run_step "Generate checksums" \
      "${root_dir}/scripts/05_generate_checksums.sh" "${release_dir}" "${release_dir}/SHA256SUMS.txt"
  fi
fi
