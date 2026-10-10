# moviehouse-lib-index

Hourly job that keeps the MovieHouse Library index up to date (`cinefreak.py cycle`, run by
`.github/workflows/sync.yml`). Public so that its Actions minutes are free; the crawler state is
stored encrypted and every key lives in repository secrets. Logs print counts only.

Manual backfill: Actions → Library sync → Run workflow → minutes = 330.
