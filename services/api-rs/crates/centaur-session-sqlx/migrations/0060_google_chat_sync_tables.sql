-- Google Chat ETL tables. Rows are keyed by owner_email so a shared space
-- (owner_email = '') and each DWD-synced direct-message mailbox stay separate.

create or replace function centaur_current_google_chat_space_id()
returns text
language sql
stable
as $$
    select nullif(current_setting('centaur.google_chat_space_id', true), '')
$$;

grant usage on schema public to centaur_slack_reader;
grant execute on function centaur_current_google_chat_space_id() to centaur_slack_reader;
grant execute on function centaur_current_slack_user_email() to centaur_slack_reader;

create table if not exists google_chat_sync_runs (
    run_id text primary key,
    workflow_run_id text,
    mode text not null default 'incremental',
    status text not null,
    scopes_requested jsonb not null default '[]'::jsonb,
    scopes_synced jsonb not null default '[]'::jsonb,
    scopes_failed jsonb not null default '[]'::jsonb,
    spaces_seen integer not null default 0,
    spaces_synced integer not null default 0,
    messages_seen integer not null default 0,
    messages_upserted integer not null default 0,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    error_text text not null default '',
    metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_google_chat_sync_runs_started
    on google_chat_sync_runs (started_at desc);

create table if not exists google_chat_sync_spaces (
    owner_email text not null default '',
    space_id text not null,
    space_name text not null default '',
    display_name text not null default '',
    space_type text not null default '',
    participant_emails text[] not null default array[]::text[],
    raw_payload jsonb not null default '{}'::jsonb,
    source_run_id text references google_chat_sync_runs(run_id) on delete set null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_error text not null default '',
    updated_at timestamptz not null default now(),
    primary key (owner_email, space_id)
);

create table if not exists google_chat_sync_messages (
    owner_email text not null default '',
    space_id text not null,
    message_id text not null,
    message_name text not null default '',
    thread_id text not null default '',
    sender_id text not null default '',
    sender_name text not null default '',
    sender_type text not null default '',
    text_content text not null default '',
    source_create_time timestamptz,
    source_last_update_time timestamptz,
    raw_payload jsonb not null default '{}'::jsonb,
    source_run_id text references google_chat_sync_runs(run_id) on delete set null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_error text not null default '',
    updated_at timestamptz not null default now(),
    primary key (owner_email, space_id, message_id),
    foreign key (owner_email, space_id)
        references google_chat_sync_spaces(owner_email, space_id) on delete cascade
);

create index if not exists idx_google_chat_sync_messages_thread
    on google_chat_sync_messages (space_id, thread_id);

create index if not exists idx_google_chat_sync_messages_create
    on google_chat_sync_messages (source_create_time desc);

create index if not exists idx_google_chat_sync_messages_text
    on google_chat_sync_messages
    using gin (to_tsvector('english', coalesce(text_content, '')));

create table if not exists google_chat_sync_checkpoints (
    owner_email text not null default '',
    space_id text not null,
    watermark_time timestamptz,
    continuation_token text not null default '',
    continuation_filter text not null default '',
    continuation_started_at timestamptz,
    continuation_updated_at timestamptz,
    last_run_id text references google_chat_sync_runs(run_id) on delete set null,
    last_success_at timestamptz,
    last_error text not null default '',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (owner_email, space_id),
    foreign key (owner_email, space_id)
        references google_chat_sync_spaces(owner_email, space_id) on delete cascade
);

create table if not exists google_chat_sync_attachments (
    owner_email text not null default '',
    space_id text not null,
    message_id text not null,
    attachment_id text not null,
    attachment_name text not null default '',
    content_name text not null default '',
    content_type text not null default '',
    source_uri text not null default '',
    download_uri text not null default '',
    size_bytes bigint,
    content_text text not null default '',
    raw_payload jsonb not null default '{}'::jsonb,
    source_run_id text references google_chat_sync_runs(run_id) on delete set null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (owner_email, space_id, message_id, attachment_id),
    foreign key (owner_email, space_id, message_id)
        references google_chat_sync_messages(owner_email, space_id, message_id)
        on delete cascade
);

create index if not exists idx_google_chat_sync_attachments_updated
    on google_chat_sync_attachments (updated_at, space_id, message_id, attachment_id);

create table if not exists google_chat_sync_reactions (
    owner_email text not null default '',
    space_id text not null,
    message_id text not null,
    reaction_id text not null,
    user_id text not null default '',
    emoji_unicode text not null default '',
    custom_emoji_uid text not null default '',
    raw_payload jsonb not null default '{}'::jsonb,
    source_run_id text references google_chat_sync_runs(run_id) on delete set null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (owner_email, space_id, message_id, reaction_id),
    foreign key (owner_email, space_id, message_id)
        references google_chat_sync_messages(owner_email, space_id, message_id)
        on delete cascade
);

create index if not exists idx_google_chat_sync_reactions_message
    on google_chat_sync_reactions (space_id, message_id);

grant select on
    google_chat_sync_runs,
    google_chat_sync_spaces,
    google_chat_sync_messages,
    google_chat_sync_checkpoints,
    google_chat_sync_attachments,
    google_chat_sync_reactions
to centaur_slack_reader;

alter table google_chat_sync_runs enable row level security;
alter table google_chat_sync_spaces enable row level security;
alter table google_chat_sync_messages enable row level security;
alter table google_chat_sync_checkpoints enable row level security;
alter table google_chat_sync_attachments enable row level security;
alter table google_chat_sync_reactions enable row level security;

-- Runs and checkpoints carry no reader policy: row level security denies every
-- row to a role no policy applies to.

create policy centaur_google_chat_spaces_reader_select
    on google_chat_sync_spaces for select to centaur_slack_reader
    using (
        (owner_email = '' and space_id = centaur_current_google_chat_space_id())
        or lower(owner_email) = centaur_current_slack_user_email()
    );

create policy centaur_google_chat_messages_reader_select
    on google_chat_sync_messages for select to centaur_slack_reader
    using (
        (owner_email = '' and space_id = centaur_current_google_chat_space_id())
        or lower(owner_email) = centaur_current_slack_user_email()
    );

create policy centaur_google_chat_attachments_reader_select
    on google_chat_sync_attachments for select to centaur_slack_reader
    using (
        (owner_email = '' and space_id = centaur_current_google_chat_space_id())
        or lower(owner_email) = centaur_current_slack_user_email()
    );

create policy centaur_google_chat_reactions_reader_select
    on google_chat_sync_reactions for select to centaur_slack_reader
    using (
        (owner_email = '' and space_id = centaur_current_google_chat_space_id())
        or lower(owner_email) = centaur_current_slack_user_email()
    );

drop policy if exists centaur_context_docs_reader_select on company_context_documents;
create policy centaur_context_docs_reader_select
    on company_context_documents for select to centaur_slack_reader
    using (
        (
            source = 'slack'
            and metadata ->> 'channel_id' = centaur_current_slack_channel_id()
        )
        or (
            source = 'google_chat'
            and metadata ->> 'space_id' = centaur_current_google_chat_space_id()
        )
    );
