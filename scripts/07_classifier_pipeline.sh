#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

input_fasta="${1:-${root_dir}/marker_fastas/COI-5P.fasta.gz}"
output_dir="${2:-${root_dir}/classifier_outputs}"
taxdump_dir="${3:-${root_dir}/bold-taxdump}"
classifiers="${4:-blast}"

compress_output="${COMPRESS_OUTPUT:-false}"

if [[ ! -f "${input_fasta}" ]]; then
  echo "Input FASTA not found: ${input_fasta}" >&2
  exit 1
fi

boldkit_bin="${BOLDKIT_BIN:-${root_dir}/boldkit/boldkit}"
if [[ ! -x "${boldkit_bin}" ]]; then
  echo "boldkit binary not found or not executable: ${boldkit_bin}" >&2
  exit 1
fi

base="$(basename "${input_fasta}")"
if [[ "${base}" == *.gz ]]; then
  base="${base%.gz}"
fi
base="${base%.*}"
if [[ -z "${base}" ]]; then
  base="qc_output"
fi

qc_out="${output_dir}/qc/${base}.fasta"

mkdir -p "${output_dir}/qc"

"${boldkit_bin}" qc \
  -input "${input_fasta}" \
  -output "${qc_out}" \
  -taxdump-dir "${taxdump_dir}" \
  -min-length 200 \
  -max-length 700 \
  -max-n 0 \
  -max-ambig 0 \
  -max-invalid 0 \
  -dedupe=true \
  -dedupe-ids=true

IFS=',' read -r -a classifier_list <<< "${classifiers}"
for classifier in "${classifier_list[@]}"; do
  classifier="$(echo "${classifier}" | tr '[:upper:]' '[:lower:]')"
  if [[ -z "${classifier}" ]]; then
    continue
  fi
  out_dir="${output_dir}/${classifier}"
  "${boldkit_bin}" format \
    -input "${qc_out}" \
    -outdir "${out_dir}" \
    -classifier "${classifier}" \
    -taxdump-dir "${taxdump_dir}"

  if [[ "${compress_output}" == "true" ]]; then
    tar -czf "${output_dir}/${classifier}.tar.gz" -C "${output_dir}" "${classifier}"
  fi
done
