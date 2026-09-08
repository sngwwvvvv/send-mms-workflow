# Selectable MMS templates

User approved terminal-number selection and `--template NAME` on 2026-09-08. This supersedes the old fixed body, fixed image names/count and split-message rules. Implementation only; no live delivery authorized.

## Contract

- One direct child directory of `input/` is one template. It contains UTF-8 `message.txt` and one or two JPEG files. Images are ordered by filename (recommend `01_`, `02_` prefixes). One-to-two is this release's project limit, not a claimed SENS maximum.
- Send all of `message.txt` as one MMS content, including its title line, whitespace and newlines. Do not synthesize a subject, split the body or insert text. Decode UTF-8 BOM as a file marker if present; retain remaining bytes/newlines exactly. Reject blank content, unsupported EUC-KR characters, or content exceeding 2000 EUC-KR bytes. Each JPEG must be within 300*1024 bytes and 1500x1440 pixels, with positive dimensions.
- `preflight` without `--template` shows a sorted, numbered list with JPEG counts and accepts a number on a terminal. Invalid input can be retried; EOF/cancel stops safely. No templates gives an actionable setup error. Noninteractive stdin requires `--template`; never silently choose a default, even for a single template. `live` always requires `--template` and never prompts.
- `preflight --template NAME` skips selection and preserves JSON output. Interactive menu/prompts go to stderr, report JSON to stdout. Expose selected template in the report. Validate selection before loading credentials or reading delivery inputs.
- Only direct safe folder names under `input` are allowed. Reject traversal, absolute paths, symlinks/junction escapes for folders and files, and control characters in displayed filenames. Known local template errors may give fixed explanatory messages; never echo untrusted contents or exception text.
- Final preflight retains immutable original message bytes (including an optional UTF-8 BOM) and image bytes. Approval token binds template name, those original message bytes, image names/order/bytes and existing approval inputs. Only decoded send content excludes the BOM file marker. Workflow passes the final report's content and images to actual API calls and never reopens them after final token validation. Send APIs require explicit content; no production hardcoded message fallback remains.
- Upload JPEGs using a stable content-derived ASCII filename of at most 40 characters, so SENS's documented same-name/same-size reuse cannot confuse different templates. Public preview still shows original local filenames.
- Keep existing results/result.csv schema, snapshots, logs, lock, retry, pending-first, SENT/ambiguous protection, settings and explicit live-approval requirements. Template selection is not a new campaign or a reset of recipient history. A permitted retry after an explicitly confirmed failure uses the newly approved selected content; preview already describes retry permission. No new campaign manager or persistence schema.
- COMM remains the supported runtime content type; this change does not add advertising support. No network from preflight, no live SENS tests, no real recipients/credentials. No root .env inspection.
- Current hardcoded message and local image are copied into a local `input/개업안내/` starter template once during development; production never falls back to them. Personal input files are ignored by Git. Missing local images are reported without generating or substituting them.

## Files and verification

Reuse inputs.py for template loading/validation; preflight.py for approval binding; cli.py for selection; workflow.py and pipeline.py for report-to-request propagation; api.py removes defaults. Update existing tests to use synthetic templates and add targeted selection, byte equality, stale token, changed-after-validation, upload name, path validation and failure scenarios. Update AGENTS.md and README to the same contract.

Official send/list/get/upload/overview pages were reviewed 2026-09-08. Send content uses EUC-KR with a 2000-byte MMS limit. Upload allows JPEG, 300 KB and 1500x1440; same-name/same-size files may be reused. Sources: https://api.ncloud-docs.com/docs/sens-sms-send and https://api.ncloud-docs.com/docs/sens-sms-attachment-create (and the project-required list/get/overview pages).
