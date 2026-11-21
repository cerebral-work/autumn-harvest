## Feature — add contract-driven `llms.txt` discovery across Harvest API and Vantage UI

`autumn-harvest-plugin` now renders deterministic `llms.txt` markdown (`text/markdown; charset=utf-8`) directly from the canonical `api-contract.json` (`ApiContract`) via `openapi::llms_txt_from_contract()`, keeping agent progressive-discovery output synchronized with OpenAPI 3.1.0 without maintaining a separate route table.

### Shipped surfaces

1. **Harvest API router (`api::api_router()` + standalone `HarvestPlugin`):**
   - `GET /llms.txt` (mounted at `/api/harvest/llms.txt` under `HarvestPlugin` and `/llms.txt` when mounted standalone) serves the full contract-generated `llms.txt` index covering all 181 canonical routes grouped by tag, RBAC permission, rate-limit class, audit classification, Authentication (`Bearer` / `X-Harvest-Key` / `X-Harvest-User`), and Error Envelope (`401`/`403`/`404`/`405`/`409`/`422`/`429`).
   - Non-`GET` methods on `/llms.txt` return `405 Method Not Allowed` with `Allow: GET` and JSON `ErrorResponse`.
   - Treated as an unauthenticated discovery route alongside `/health`, `/ready`, and `/openapi.json` (`Permission::WorkflowRead` public pass-through, `AuditAction::DiscoveryRead`, `Cache-Control: public, max-age=300`, `X-Content-Type-Options: nosniff`).
   - `GET /openapi.json` now emits `Link: <{api_base}/llms.txt>; rel="service-doc"; type="text/markdown"`.
2. **Vantage HTMX operator UI (`ui::ui_router()`):**
   - `GET /llms.txt` (mounted at `/harvest/llms.txt` under `HarvestPlugin`) serves a dedicated operator UI discovery document describing Vantage console routes, HTMX partial fragment architecture (`HX-Request: true`), CSRF boundary (`harvest_csrf` + `X-Harvest-Csrf`), CSP posture, and cross-links to `{api_base}/llms.txt` and `{api_base}/openapi.json`.
   - Non-`GET` methods return `405 Method Not Allowed` with `Allow: GET`.
   - All 7 HTML page shells emit `<link rel="describedby" type="text/markdown" href="{ui_base}/llms.txt">` in `<head>`, and `GET {ui_base}/` emits `Link: <{ui_base}/llms.txt>; rel="describedby"; type="text/markdown"`.
3. **Checked-in contract artifacts & regeneration:**
   - `emit_openapi` and `scripts/regenerate-openapi.sh` emit `docs/llms.txt` and `autumn-harvest-plugin/llms.txt` alongside `openapi.json` and `api-contract.json`.

### Invariants & migrations

- No database schema migration.
- No `WorkflowEvent` variant change; adds read-only `AuditAction::DiscoveryRead` (`discovery.read`, `AuditOutcome::Read`).
- Zero internal hostnames, private IPs, or secret material in generated `llms.txt` artifacts.

### Test evidence

- `cargo test -j 2 --workspace --lib` (1,535 unit tests passed).
- `openapi_spec` (27 passed, including artifact byte-identity parity across all 181 contract routes) and `contract_regression` (44 passed, including API + UI `/llms.txt` headers, `405 Allow: GET`, `<link rel="describedby">`, and `Link` headers).
- `security` (125 passed), `standalone_admin_auth` (12 passed), and `standalone_mount` (4 passed).
