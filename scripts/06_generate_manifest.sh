#!/usr/bin/env bash
set -euo pipefail

releases_dir="${1:-releases}"
taxdump_dir="${2:-bold-taxdump}"
marker_dir="${3:-marker_fastas}"
snapshot_id="${4:-}"
output_file="${5:-${releases_dir}/manifest.json}"

if [[ -z "${snapshot_id}" ]]; then
  echo "Snapshot ID is required" >&2
  exit 1
fi

if [[ ! -d "${releases_dir}" ]]; then
  echo "Directory not found: ${releases_dir}" >&2
  exit 1
fi

if [[ -s "${output_file}" ]]; then
  echo "Manifest exists, skipping: ${output_file}" >&2
  exit 0
fi

if [[ ! -d "${taxdump_dir}" ]]; then
  echo "Directory not found: ${taxdump_dir}" >&2
  exit 1
fi

if [[ ! -d "${marker_dir}" ]]; then
  echo "Directory not found: ${marker_dir}" >&2
  exit 1
fi

commit_hash="unknown"
repo_root="$(cd "$(dirname "${releases_dir}")" && pwd)"
if command -v git >/dev/null 2>&1; then
  if git -C "${repo_root}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    if git -C "${repo_root}" rev-parse --verify HEAD >/dev/null 2>&1; then
      commit_hash="$(git -C "${repo_root}" rev-parse HEAD)"
    fi
  fi
fi

nodes_count="$(wc -l < "${taxdump_dir}/nodes.dmp" | tr -d ' ')"
names_count="$(wc -l < "${taxdump_dir}/names.dmp" | tr -d ' ')"
taxid_count="$(wc -l < "${taxdump_dir}/taxid.map" | tr -d ' ')"

marker_seq_count="0"
mapfile -d '' marker_files < <(find "${marker_dir}" -maxdepth 1 -type f \( -name '*.fasta' -o -name '*.fasta.gz' \) -print0)
marker_file_count="${#marker_files[@]}"

if (( marker_file_count > 0 )); then
  if ! command -v gzip >/dev/null 2>&1; then
    for f in "${marker_files[@]}"; do
      if [[ "${f}" == *.gz ]]; then
        echo "gzip not found in PATH (needed for ${f})" >&2
        exit 1
      fi
    done
  fi
  for f in "${marker_files[@]}"; do
    if [[ "${f}" == *.gz ]]; then
      count="$(gzip -dc "${f}" | awk '/^>/{c++} END{print c+0}')"
    else
      count="$(awk '/^>/{c++} END{print c+0}' "${f}")"
    fi
    marker_seq_count=$((marker_seq_count + count))
  done
fi

cat <<EOF > "${output_file}"
{
  "snapshot_id": "${snapshot_id}",
  "commit_hash": "${commit_hash}",
  "counts": {
    "nodes": ${nodes_count},
    "names": ${names_count},
    "taxid_map": ${taxid_count},
    "marker_fasta_files": ${marker_file_count},
    "marker_fasta_sequences": ${marker_seq_count}
  }
}
EOF
