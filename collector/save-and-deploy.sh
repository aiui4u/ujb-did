#!/usr/bin/env bash
set -euo pipefail
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add web/data.json web/news.json
if [ -f collector/state.json ]; then
  git add collector/state.json
fi
if git diff --cached --quiet; then
  echo "저장할 변경사항 없음"
  exit 0
fi
git commit -m "data: 통합 DID 자료 갱신"
git pull --rebase origin main
git push origin HEAD:main
gh workflow run deploy.yml --ref main
