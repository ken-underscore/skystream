#!/usr/bin/env bash
# Downloads embeddinggemma-300m (ONNX) for semantic search into models/embeddinggemma-300m, or the directory given.
# The files are pinned to one revision, which the embedding regression test was recorded against, and checked by
# SHA-256. ONNX Runtime needs model.onnx_data as a real file next to model.onnx, so this writes plain files rather
# than a Hugging Face cache.
# The model is released under the Gemma Terms of Use: https://ai.google.dev/gemma/terms
set -euo pipefail

REPO="onnx-community/embeddinggemma-300m-ONNX"
REVISION="5090578d9565bb06545b4552f76e6bc2c93e4a66"
TARGET="${1:-$(dirname "$0")/../models/embeddinggemma-300m}"
FILES=(
  "tokenizer.json 4dda02faaf32bc91031dc8c88457ac272b00c1016cc679757d1c441b248b9c47"
  "tokenizer_config.json 3ca953eea6c3c9fcda9cf3df22949ff18b216f7c74bd6459230f3f1013953f3a"
  "onnx/model.onnx ea91fd315a7c152d427d231746f0f811a1ac93beaba656abfdf2b24e091265e4"
  "onnx/model.onnx_data ef835ae565d8695236652475903078e8ed794c7c35faf1164d78ec3238e8a88d"
)

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

for entry in "${FILES[@]}"; do
  read -r path hash <<<"$entry"
  file="$TARGET/$path"
  if [[ -f "$file" && $(sha256 "$file") == "$hash" ]]; then
    echo "have $path"
    continue
  fi
  mkdir -p "$(dirname "$file")"
  echo "fetching $path"
  curl -fL --retry 3 -o "$file.part" "https://huggingface.co/$REPO/resolve/$REVISION/$path"
  [[ $(sha256 "$file.part") == "$hash" ]] || { echo "checksum mismatch for $path" >&2; exit 1; }
  mv "$file.part" "$file"
done
echo "model ready in $TARGET"
