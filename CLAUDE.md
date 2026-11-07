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

## Quiver Quant API (api.quiverquant.com)

Reference parsed from the live OpenAPI schema at `https://api.quiverquant.com/docs/schema.json`
(the human docs at `/docs/` are a Stoplight Elements SPA that renders this same schema).

- **Base URL**: `https://api.quiverquant.com`
- **Auth**: HTTP Bearer token (`Authorization: Bearer <token>`), scheme name
  `WebPlatformTokenAuth`. Obtain/refresh via `POST /beta/token/login/` and `POST
  /beta/token/logout/`.
- **Access tiers**: every endpoint is tagged `public`, `enterprise`, `internal`, or `mobile`, plus
  a rate/plan tag `Tier 1`, `Tier 2`, or `New Constructs Ratings`. `public` endpoints are available
  on a standard paid API key; `enterprise` ones need an enterprise plan; `internal`/`mobile` are not
  meant for external use (may 403/require special auth) — check for these before recommending one.
- **Pagination**: most list endpoints accept `page` and `page_size` query params.
- **Date params**: `YYYYMMDD` string format (e.g. `date`, `date_from`, `date_to`).
- **Ticker params**: either a path param (`/{ticker}`) on "historical" endpoints, or an optional
  `ticker` query param on "live"/"bulk" endpoints to filter.

### Congressional trading & holdings (Tier 1, public)

- `GET /beta/live/congresstrading` — most recent transactions by all of Congress.
  Filters: `representative`, `normalized` (normalize name spelling).
- `GET /beta/live/housetrading` — most recent U.S. House transactions. Filters: `name`, `options`
  (include options trades).
- `GET /beta/live/senatetrading` — most recent U.S. Senate transactions. Same filters as above.
- `GET /beta/bulk/congresstrading` — full transaction history, paginated. Filters: `ticker`,
  `representative`, `bioguide_id`, `date`, `nonstock`, `normalized`, `version` (`V1`|`V2`).
- `GET /beta/historical/congresstrading/{ticker}` — full history for one ticker.
- `GET /beta/historical/housetrading/{ticker}` / `GET /beta/historical/senatetrading/{ticker}` —
  chamber-specific history for one ticker.
- `GET /beta/live/congress/politicians` / `GET /beta/bulk/congress/politicians` — roster with
  trading stats (trade count/volume) and net worth. Filters: `chamber`, `party`, `state`,
  `is_active`, `include_candidates` (bulk only), `sort_by`, `volume_method`
  (`range_start`|`average`|`range_end`).
- `GET /beta/live/congress_stock_holdings` — estimated current stock holdings per member. Filters:
  `bioguide_id`, `ticker`, `sort_by_holding`.
- `GET /beta/live/congressholdings` (enterprise) — live congress holdings data.

### Corporate donations & lobbying (Tier 1, public)

- `GET /beta/bulk/corporatedonors` — PAC donations to politicians. Filters: `ticker`,
  `bioguide_id`, `cycle` (election year), `transaction_tp` (FEC transaction type code, e.g. `24K`
  contribution to candidate, `24E` independent expenditure).
- `GET /beta/historical/corporatedonors/{ticker}` — same, scoped to one ticker's PAC donations
  (default 500/page).
- `GET /beta/live/lobbying` — most recent lobbying spend across all companies. Filters:
  `date_from`, `date_to`, `all`.
- `GET /beta/historical/lobbying/{ticker}` — lobbying spend history for one ticker. Filters:
  `query` (match issue/summary text), `queryTicker`.
- `GET /beta/live/bill_summaries` (enterprise) — recent bill summaries. Filters: `query`,
  `summary_limit`.
- `GET /beta/live/legislation` (enterprise) — recent legislation.

### Government contracts (Tier 1, public)

- `GET /beta/live/govcontracts` — last quarter's gov contract amounts, all companies.
- `GET /beta/live/govcontractsall` — recently announced contracts, all companies. Filters: `date`.
- `GET /beta/historical/govcontracts/{ticker}` — historical quarterly contract amounts for one
  ticker.
- `GET /beta/historical/govcontractsall/{ticker}` — full historical contracts for one ticker.

### Off-exchange / dark-pool activity (Tier 1, public)

- `GET /beta/live/offexchange` — yesterday's off-exchange activity, all companies.
- `GET /beta/historical/offexchange/{ticker}` — daily off-exchange history for one ticker.
  Filters: `date`.

### Insider trading & SEC 13F (Tier 2, public)

- `GET /beta/live/insiders` — recent Form 4 insider transactions. Filters: `ticker`, `date`,
  `uploaded`, `limit_codes`.
- `GET /beta/live/sec13f` — 13F institutional holdings snapshot at a filing period. Filters:
  `ticker`, `owner`, `period`, `date`, `today`.
- `GET /beta/live/sec13fchanges` — period-over-period 13F holding changes. Filters: same as above
  plus `most_recent`, `show_new_funds`, `mobile`.
- `GET /beta/live/topshareholders/{ticker}` — top direct + derivative shareholders for a ticker.

### Patents (Tier 2, public)

- `GET /beta/live/allpatents` — recently granted patents, all companies (max 30-day range).
  Filters: `date_from`, `date_to`.
- `GET /beta/historical/allpatents/{ticker}` — patents granted to one ticker, paginated. Filters:
  `date_from`, `date_to`.
- `GET /beta/live/patentmomentum` — historical patent-momentum score, all tickers. Filters:
  `ticker`, `date_from`, `date_to`, `latest`.
- `GET /beta/live/patentdrift` — historical patent-drift score, all tickers. Same filters.

### Misc data feeds

- `GET /beta/bulk/trumpstocktrades` (Tier 1, public) — Donald Trump's recent stock transactions
  (default 200/page).
- `GET /beta/historical/executivecompensation/{ticker}` (Tier 2, public) — exec comp history
  (default 200/page).
- `GET /beta/live/quivernews` (Tier 2, public) — recent Quiver News articles. Filters: `ticker`
  (default 30/page).
- `GET /beta/live/appratings` (Tier 2, public) — last week of app store ratings data.
- `GET /beta/live/earningsdistortionscores/{ticker}` (New Constructs Ratings, public) — New
  Constructs' proprietary ratings (Overall Risk/Reward, Economic vs Reported EPS, ROIC, FCF Yield,
  Price-to-Economic-Book-Value, GAP, etc.) derived from SEC filings.
- `GET /beta/live/rss` (Tier 1, enterprise) — RSS feed data.
- `GET /beta/strategies/holdings` / `GET /beta/strategies/holdings_infrequent` (Tier 1,
  enterprise) — Quiver's own strategy portfolios' holdings. Filter: `strategy`.

### Internal-only (not for external use — listed for completeness)

`GET /beta/auth/premium`, `/beta/coins` (Reddit coin data), `/beta/companies`, `/beta/funds`
(SEC 13F fund info), `/beta/historical/eventsbeta/{ticker}`, `/beta/today/eventsbeta`,
`/beta/ignoredtickers`, `/beta/live/cnbc` (CNBC trade calls), `/beta/live/flights`,
`/beta/personalities` (CNBC personalities), `/beta/tickerdata`.

### Notes for this repo

- No API key is currently configured/observed in this repo for Quiver; ask the user (do not read
  `.env` or other hidden files yourself) before assuming live access works.
