#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/update-homebrew-formula.sh <version> <path-to-SHA256SUMS> [output-file]

VERSION="${1:-}"
SUMS_FILE="${2:-}"
OUTPUT_FILE="${3:-Formula/noida-db.rb}"

if [ -z "$VERSION" ] || [ -z "$SUMS_FILE" ]; then
  echo "Usage: $0 <version> <path-to-SHA256SUMS> [output-file]"
  exit 1
fi

get_sha() {
  local target="$1"
  grep "noida-db-${VERSION}-${target}" "$SUMS_FILE" | awk '{print $1}' | head -n1
}

SHA_DARWIN_ARM64=$(get_sha "aarch64-apple-darwin")
SHA_DARWIN_X64=$(get_sha "x86_64-apple-darwin")
SHA_LINUX_ARM64=$(get_sha "aarch64-unknown-linux-gnu")
SHA_LINUX_X64=$(get_sha "x86_64-unknown-linux-gnu")

mkdir -p "$(dirname "$OUTPUT_FILE")"

cat <<EOF > "$OUTPUT_FILE"
class NoidaDb < Formula
  desc "One tiny local binary that speaks Postgres, MySQL, Redis, Kafka, Elasticsearch and ClickHouse"
  homepage "https://github.com/its-banana-coder/noida-db"
  version "${VERSION}"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v${VERSION}/noida-db-${VERSION}-aarch64-apple-darwin.tar.gz"
      sha256 "${SHA_DARWIN_ARM64}"
    end
    on_intel do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v${VERSION}/noida-db-${VERSION}-x86_64-apple-darwin.tar.gz"
      sha256 "${SHA_DARWIN_X64}"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v${VERSION}/noida-db-${VERSION}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "${SHA_LINUX_ARM64}"
    end
    on_intel do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v${VERSION}/noida-db-${VERSION}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "${SHA_LINUX_X64}"
    end
  end

  def install
    bin.install "noida-db"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/noida-db --version")
  end
end
EOF

echo "Generated $OUTPUT_FILE for version $VERSION"
