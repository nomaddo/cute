#!/usr/bin/env bash
set -euo pipefail

root="${1:-.}"
procs="${2:-$(nproc)}"

# 1行だけのファイルを並列削除
find "$root" -type f -print0 \
  | xargs -0 -n 1 -P "$procs" bash -c '
      f="$1"
      # wc -lは末尾改行がなくても正しく「行数」を返す
      if [ "$(wc -l < "$f")" -eq 1 ]; then
        echo "Removing: $f"
        rm -f -- "$f"
      fi
    ' _
