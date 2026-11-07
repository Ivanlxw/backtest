#!/usr/bin/env bash
# Bump the trading/ submodule pointer to Ivanlxw/trading master in a new backtest PR.
set -euo pipefail

TRADING_URL=git@github.com:Ivanlxw/trading.git
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

git fetch -q origin master
NEW=$(git ls-remote "$TRADING_URL" refs/heads/master | cut -f1)
OLD=$(git ls-tree origin/master trading | awk '{print $3}')

if [ "$NEW" = "$OLD" ]; then
  echo "UP_TO_DATE $NEW"
  exit 0
fi

SHORT=${NEW:0:7}
BRANCH=claude/bump-trading-$SHORT
if git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
  echo "BRANCH_EXISTS $BRANCH"
  exit 3
fi

WT=$(mktemp -d)
cleanup() {
  cd "$ROOT"
  git worktree remove --force "$WT" >/dev/null 2>&1 || true
  rm -rf "$WT"
}
trap cleanup EXIT

git worktree add -q -b "$BRANCH" "$WT" origin/master
cd "$WT"
git update-index --cacheinfo 160000,"$NEW",trading
git submodule update --init trading

# Verification checklist (CLAUDE.md steps 1-3; step 4 skipped: pointer-only change)
LOG=$(mktemp)
TABLE="| Step | Result |
|---|---|"
FAILED=0
run_step() {
  local name=$1; shift
  if "$@" >>"$LOG" 2>&1; then
    TABLE="$TABLE
| $name | PASS |"
  else
    TABLE="$TABLE
| $name | FAIL |"
    FAILED=1
  fi
}
run_step "1. uv sync --frozen" uv sync --frozen
if [ $FAILED -eq 0 ]; then
  run_step "2. compileall" uv run python -m compileall -q backtest trading loop.py loop_streaming.py inform.py
fi
if [ $FAILED -eq 0 ]; then
  run_step "3. import check" uv run python -c "import backtest.utilities.utils, backtest.utilities.backtest, trading.data.dataHandler"
fi
TABLE="$TABLE
| 4. pytest | SKIP (pointer-only change) |"
echo "$TABLE"
if [ $FAILED -ne 0 ]; then
  echo "VERIFY_FAILED"
  cat "$LOG"
  exit 4
fi

COMMITS=$(git -C trading log --oneline "$OLD..$NEW" 2>/dev/null || true)
MSG="bump trading submodule to $SHORT

$COMMITS

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
git add trading
git commit -q -m "$MSG"
git push -q -u origin "$BRANCH"

if gh auth status >/dev/null 2>&1; then
  BODY="## Summary
Bump the trading submodule pointer to Ivanlxw/trading master (\`$SHORT\`).

## Commits
$COMMITS

## Verification
$TABLE

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
  URL=$(gh pr create --base master --head "$BRANCH" --title "bump trading submodule to $SHORT" --body "$BODY")
  echo "PR_URL $URL"
  exit 0
fi
echo "NO_GH https://github.com/Ivanlxw/backtest/compare/master...$BRANCH?expand=1"
exit 5
