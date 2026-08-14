-- Copyright 2024 Blnk Finance Authors.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- +migrate Up

-- SCRUB EMBEDDED CREDENTIALS, AND LITERAL SPACES, OUT OF ALREADY-STORED WEBHOOK URLS.
--
-- model.ValidateWebhookURL now refuses userinfo and literal spaces, so no NEW row can
-- carry either. This migration deals with the rows already written, because a rule that
-- only applies going forward leaves the disclosure it was written to stop sitting in the
-- table.
--
-- WHAT WAS WRONG. The policy checked scheme, host, length, surrounding whitespace and
-- internal destinations, and nothing else. So
--
--     https://user:secret@hooks.example.com/blnk
--
-- was accepted, written verbatim to blnk.event_subscribers.webhook_url, and returned
-- unchanged by GET /subscribers/{id}. Every spelling got through, including a
-- percent-encoded password. A credential in that column is a credential in every API
-- response that reads the row, every support export, and every database backup.
--
-- A literal space got through for a different reason: url.Parse ACCEPTS one and folds it
-- into the path, so "https://hooks.example.com/a b" was stored with the space intact and
-- two spellings of one endpoint became two subscribers pointing at the same place.
--
-- WHY THIS IS SAFE TO REWRITE, and the reason is specific rather than general: NOTHING
-- DIALS THIS COLUMN. Legacy HTTP delivery goes to the single configured
-- BLNK_NOTIFICATION_WEBHOOK_URL; webhook_url on a subscriber is the migration record —
-- "this is the endpoint this subscriber used to be pushed to" — read by
-- RecordSubscriberWebhookURL, ClearSubscriberWebhookURL, the migrated_at stamp and
-- PurgeMigratedWebhookURLs. Rewriting it therefore changes no delivery, retries nothing,
-- and reorders nothing. It removes a stored secret and canonicalises a stored spelling.
--
-- STATEMENT 1 — USERINFO. The pattern matches from the scheme separator to the LAST '@'
-- before the authority ends, which is exactly where Go's url.Parse ends userinfo, so this
-- removes precisely what the parser would have read as credentials and nothing else:
--
--     https://user@host/p            -> https://host/p
--     https://user:secret@host/p     -> https://host/p
--     https://user:p%40ss@host/p     -> https://host/p
--     https://@host/p                -> https://host/p
--     https://user@@host/p           -> https://host/p     (greedy to the last '@')
--     https://host/hook@v2           -> unchanged          ('[^/?#]*' cannot cross the '/')
--     https://host/?to=a@b.com       -> unchanged
--
-- The host, port, path, query and fragment are untouched, so the destination the operator
-- recorded is still the destination recorded.
--
-- STATEMENT 2 — LITERAL SPACES, and only after the authority. url.Parse REFUSES a space in
-- a host ("invalid character \" \" in host name") and in a port, so a stored value can only
-- carry one in the path, query or fragment — and there '%20' is not an approximation of a
-- space, it IS a space per RFC 3986, so the rewrite is an exact re-spelling. The authority
-- is split off and left alone deliberately: percent-encoding a space inside a host would
-- manufacture a syntactically valid hostname that never existed, which is worse than
-- leaving a row that was never a usable destination looking like what it is.
--
-- Control characters need no statement. url.Parse has always refused them
-- ("net/url: invalid control character in URL"), so none can be present; the new check
-- exists to give a caller a message that names the byte, not to close a storage gap.
--
-- BOTH STATEMENTS ARE IDEMPOTENT. Each WHERE selects only rows still exhibiting the
-- pattern, and after the update no row does, so a re-run is a no-op — which matters
-- because sql-migrate records this file once but an operator may replay a range.

UPDATE blnk.event_subscribers
SET webhook_url = regexp_replace(webhook_url, '^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]*@', '\1'),
    updated_at  = now()
WHERE webhook_url IS NOT NULL
  AND webhook_url ~ '^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*@';

UPDATE blnk.event_subscribers
SET webhook_url = substring(webhook_url from '^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*')
                  || replace(
                       substring(webhook_url from '^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*(.*)$'),
                       ' ', '%20'
                     ),
    updated_at  = now()
WHERE webhook_url IS NOT NULL
  AND webhook_url ~ '^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*[/?#].* ';

-- +migrate Down

-- THERE IS NO DOWN FOR THIS, and saying so is more honest than a statement that pretends.
--
-- The credential this migration removed was not copied anywhere before it was removed —
-- deliberately, because a rollback table holding the plaintext passwords would recreate
-- the disclosure in a place nobody is watching. Reversing the space canonicalisation
-- alone would be possible and pointless: '%20' and ' ' denote the same URL, so it would
-- change bytes and nothing else.
--
-- Rolling back past this migration therefore leaves webhook_url as the scrubbed form. That
-- is the correct outcome: the schema is unchanged by this file, so an older binary reads
-- and writes the column exactly as before, and the only difference it sees is that a
-- destination it once recorded with credentials is now recorded without them. A subscriber
-- that genuinely needs authenticated pushes must be re-registered with the credential sent
-- as a header on the endpoint rather than embedded in the URL.

SELECT 1;
