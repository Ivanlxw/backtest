# backtest

Python 3.13 / uv backtester. Market data comes from the IBKR bars/ticks in the Postgres `ibkr`
schema (`DB_URL` env var). `trading/` is a git submodule
([Ivanlxw/trading](https://github.com/Ivanlxw/trading)).

## Mandatory workflow after ANY code change

1. Work on a branch `claude/<short-kebab-desc>` cut from up-to-date `origin/master`. Never commit
   to `master`.
2. Make the change.
3. **Verify immediately, in a subagent**: spawn an `Agent` with `model: haiku`. Tell it to run the
   checklist below exactly as written and not edit any code. Give it the list of changed files. It
   reports PASS/FAIL/SKIP per step and the verbatim output of any failure.
4. If anything fails, fix it and verify again. Never commit failing code. After 2 failed rounds,
   stop and report the failure to the user.
5. Commit, push and open a PR by following "Git / PR workflow" below.
6. Reply with the PR URL(s) and the verification table.

Docs-only changes (`*.md`) skip step 3 but still get a branch and a PR. The user approves and
merges PRs; never merge or approve them yourself.

### Verification checklist

No step may need the database, IBKR, or hidden files (see below).

1. `uv sync --frozen`
2. `uv run python -m compileall -q backtest trading loop.py loop_streaming.py inform.py`
3. `uv run python -c "import backtest.utilities.utils, backtest.utilities.backtest, trading.data.dataHandler"`
4. If a changed file has tests: `uv run pytest -q -x <path>`. Otherwise SKIP.

### Git / PR workflow

Run this after verification has passed.

1. **Branch** (if not already on one): `git fetch origin && git switch -c claude/<short-kebab-desc> origin/master`.
2. **Stage only the relevant paths**: `git add <path> ...`. Never `git add -A` or `git add .`.
   Never commit: `credentials*.json` / `real_credentials*.json`, hidden env files, `.venv/`,
   `__pycache__/`, `*.egg-info`, `experiments/` notebooks, notebook outputs.
3. **Commit** with a clear message that says what and why. End the message with the attribution
   trailer given in the session's system reminder.
4. **Push**: `git push -u origin <branch>`.
5. **Open the PR**: `gh pr create --base master --title "<title>" --body "<body>"`. The body has: a
   short summary, the verification table from the Haiku subagent (including SKIPped steps and
   why), and the attribution line from the session's system reminder.
6. **Never** merge, approve, or enable auto-merge. The user approves.
7. If `gh` is missing or not authenticated (`gh auth status` fails), stop after the push and print
   `https://github.com/Ivanlxw/backtest/compare/master...<branch>?expand=1` and tell the user to
   install gh / run `gh auth login` or open that link.

### `trading/` submodule pointer

- Changes inside `trading/` go in their own PR to Ivanlxw/trading, never alongside a backtest PR.
- Never bump the `trading` pointer to an unmerged branch commit, and never in the same PR as the
  submodule PR: a squash or rebase merge leaves that commit off the submodule's `master`, so the
  pointer goes stale. After the trading PR merges, bump the pointer in a separate backtest PR.

## Hidden files: NEVER access without explicit permission

- **Never read hidden files** (any file or directory whose name starts with `.`, e.g. `.gitignore`,
  `.envrc`, `.env`, `.git/`, `.venv/`, `.config/`).
- **Always ask the user for permission first** before doing *any* operation that involves a hidden
  file: reading, writing, editing, deleting, moving, copying, listing its contents, grepping,
  sourcing, or diffing it.
- This applies to **every access path, including indirect ones**. Do not sidestep this rule by, for
  example:
  - writing or running Python (or any other language) code that opens/loads the file (e.g.
    `open(".envrc")`, `dotenv.load_dotenv()`, `Path(".env").read_text()`);
  - using shell commands (`cat`, `head`, `grep`, `source`, `cp`, `git show`, etc.) on it;
  - running scripts, tests, or tools whose purpose or side effect is to read it and print its
    contents;
  - delegating the access to a subagent.
- If a task seems to require a hidden file, stop, explain why it's needed, and wait for explicit
  approval for that specific operation. Approval for one operation does not extend to others.
