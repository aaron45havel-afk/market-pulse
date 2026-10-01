# Maintenance scripts

## refresh_zillow.py

Refreshes state-level home values and rents for `data_providers`.

```bash
python scripts/refresh_zillow.py            # download + write overrides
python scripts/refresh_zillow.py --dry-run  # download + print, don't write
```

Downloads Zillow Research's state ZHVI (home value, with year-over-year
change) and state ZORI (rent) and writes them to
`data/zillow_overrides.json` under `state_overrides`; `data_providers`
applies them to `CHOROPLETH_STATES`. If the state ZORI file is missing,
last month's state rents are carried. ZIP-level Zillow figures are built
into `data/zip_profile.db` and `data/zips.db`, not here (the per-ZIP
section went with the hand-curated metro maps in map rebuild phase 5).

## mfops_bootstrap.py

Creates the first organization, division and platform administrator for
the multifamily ops platform. Run once, by whoever holds `DATABASE_URL`:

    DATABASE_URL=... python scripts/mfops_bootstrap.py \
        --org "Havel Property Holdings LLC" \
        --division "California" \
        --email you@example.com --migrate

Prints a TOTP secret once — `platform_admin` requires MFA, so without
enrolling it the account is created and immediately unusable. Refuses to
run a second time if any platform administrator already exists; create
further ones by signing in and granting the role, which leaves an audit
trail.
