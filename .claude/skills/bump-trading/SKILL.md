---
name: bump-trading
description: Bump the trading/ submodule pointer to Ivanlxw/trading origin/master in a separate backtest PR. Use when asked to update/bump the trading submodule.
---

# bump-trading

Run exactly one command from the repo root: `bash .claude/skills/bump-trading/bump.sh`

Do not edit any files. Do not run any other git commands. The script's built-in
checklist replaces the Haiku verification subagent for this pointer-only change.

Reply according to the script's output:

| Output line / exit code | Exact reply |
|---|---|
| `PR_URL <url>` (exit 0) | Give the URL. |
| `UP_TO_DATE <sha>` (exit 0) | Say the trading pointer is already up to date. |
| `BRANCH_EXISTS` (exit 3) | Say the bump branch already exists on origin and ask the user what to do. |
| `VERIFY_FAILED` (exit 4) | Paste the output verbatim and stop. |
| `NO_GH <url>` (exit 5) | Give the compare URL and tell the user to install gh / run `gh auth login`, or open that link. |
| Any other failure | Paste the output and stop. |
