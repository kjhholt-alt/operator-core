-- Portfolio Growth OS: canonical lead ledger table for Supabase.
--
-- Apply this in the shared Operator Supabase project when the local queue is
-- ready to become a browser dashboard or cross-device command center.
-- The CLI can mirror into this table with:
--
--   operator leads sync --mirror-supabase

create table if not exists public.operator_leads (
  id text primary key,
  event_key text not null unique,
  product text not null,
  event_type text not null,
  source_table text not null,
  source_row_id text not null,
  email text,
  company text,
  event_created_at timestamptz,
  first_seen_at timestamptz not null,
  last_seen_at timestamptz not null,
  status text not null default 'NEW',
  intent_score integer not null default 0,
  next_action text not null default '',
  notes_md text not null default '',
  metadata jsonb not null default '{}'::jsonb,
  last_contacted_at timestamptz,
  follow_up_at timestamptz,
  updated_at timestamptz not null
);

create index if not exists operator_leads_status_score_idx
  on public.operator_leads (status, intent_score desc);

create index if not exists operator_leads_email_idx
  on public.operator_leads (email);

alter table public.operator_leads enable row level security;

-- Service-role writes only for now. Add authenticated read policies when the
-- dashboard ships.
