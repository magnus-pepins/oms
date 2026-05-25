# Account tax residency sync (CA treaty withholding)

**Scope:** Keep `oms_account_tax_residency` aligned with the customer's authoritative tax country so `CorporateActionWithholdingResolver` can pick treaty rates from broker CA payloads.

## Source of truth (target)

| Priority | System | Signal |
|----------|--------|--------|
| 1 | **KYC Gateway** | Tier-3 / address verification outcome → ISO-3166 alpha-2 tax country |
| 2 | **Onboarding BFF** | Customer-declared tax residency at account open (until KYC confirms) |
| 3 | **Ops override** | Beard Admin / internal HTTP for corrections |

Pop manual SQL was a one-off smoke path only.

## OMS surface (landed)

| Method | Path | Purpose |
|--------|------|---------|
| `PUT` | `/internal/v1/account-tax-residency/{accountId}` | Upsert `{ "taxCountry": "SE", "source": "kyc.tier3" }` |
| `GET` | `/internal/v1/account-tax-residency/{accountId}` | Read current row |

Requires `X-OMS-Internal-Key` like other internal routes.

## Upstream wiring (TODO)

1. **KYC Gateway** — on successful Tier-3 with tax country, `PUT` to OMS ingress with service account key. Idempotent on `account_id`.
2. **Customer Frontend BFF** — optional bootstrap at invest-account creation when KYC not yet complete; KYC overwrites on confirmation.
3. **Mid-year change** — defer to ISK plan `tax_residency_events`; withholding uses point-in-time row until event stream lands.

## Withholding behaviour

CA event payload carries `withholdingRates` map (broker file). OMS resolves:

1. `oms_account_tax_residency.tax_country` for holder account
2. Match key in map, else `default`, else flat `withholdingRate`

No W-8BEN verification in OMS v1 — broker/custodian remains authoritative on whether treaty rate applies.
