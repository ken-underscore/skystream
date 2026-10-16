#!/usr/bin/env bash
# Downloads embeddinggemma-300m (ONNX) for semantic search into models/embeddinggemma-300m, or the directory given.
# The files are pinned to one revision, which the embedding regression test was recorded against. ONNX Runtime needs
# model.onnx_data as a real file next to model.onnx, so this writes plain files rather than a Hugging Face cache.
# The model is released under the Gemma Terms of Use: https://ai.google.dev/gemma/terms
set -euo pipefail

REPO="onnx-community/embeddinggemma-300m-ONNX"
REVISION="5090578d9565bb06545b4552f76e6bc2c93e4a66"
TARGET="${1:-$(dirname "$0")/../models/embeddinggemma-300m}"
# Path and expected size in bytes, to catch truncated downloads.
FILES=(
  "tokenizer.json 20323312"
  "onnx/model.onnx 479932"
  "onnx/model.onnx_data 1234521088"
)

for entry in "${FILES[@]}"; do
  read -r path size <<<"$entry"
  file="$TARGET/$path"
  if [[ -f "$file" && $(stat -c %s "$file") == "$size" ]]; then
    echo "have $path"
    continue
  fi
  mkdir -p "$(dirname "$file")"
  echo "fetching $path"
  curl -fL --retry 3 -o "$file.part" "https://huggingface.co/$REPO/resolve/$REVISION/$path"
  [[ $(stat -c %s "$file.part") == "$size" ]] || { echo "unexpected size for $path" >&2; exit 1; }
  mv "$file.part" "$file"
done
echo "model ready in $TARGET"
