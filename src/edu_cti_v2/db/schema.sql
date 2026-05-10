-- EduThreat-CTI v2 Postgres schema draft
-- Fresh-start design: canonical incidents + lineage retained
-- Updated: 2026-05-09

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE IF NOT EXISTS source_incidents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name text NOT NULL,
    source_group text NOT NULL,
    source_event_key text NOT NULL,
    collector_version text,
    collected_at timestamptz NOT NULL DEFAULT now(),
    source_published_at timestamptz,
    raw_title text,
    raw_subtitle text,
    raw_victim_name text,
    raw_institution_name text,
    raw_institution_type text,
    raw_country text,
    raw_region text,
    raw_city text,
    raw_incident_date text,
    raw_date_precision text,
    raw_status text,
    raw_attack_hint text,
    raw_threat_actor text,
    raw_notes text,
    source_confidence text,
    ingest_hash text NOT NULL,
    raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_deleted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_source_incident UNIQUE (source_name, source_event_key),
    CONSTRAINT ck_source_group CHECK (source_group IN ('curated', 'news', 'rss', 'api'))
);

CREATE INDEX IF NOT EXISTS idx_source_incidents_collected_at
    ON source_incidents (collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_source_incidents_source_name
    ON source_incidents (source_name);
CREATE INDEX IF NOT EXISTS idx_source_incidents_ingest_hash
    ON source_incidents (ingest_hash);

CREATE TABLE IF NOT EXISTS source_incident_urls (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_incident_id uuid NOT NULL REFERENCES source_incidents(id) ON DELETE CASCADE,
    url text NOT NULL,
    normalized_url text NOT NULL,
    resolved_url text,
    url_kind text NOT NULL DEFAULT 'article',
    is_wrapper boolean NOT NULL DEFAULT false,
    is_primary_from_source boolean NOT NULL DEFAULT false,
    is_resolved_primary boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_source_incident_normalized_url UNIQUE (source_incident_id, normalized_url),
    CONSTRAINT ck_source_incident_url_kind
        CHECK (url_kind IN ('article', 'detail', 'leak_site', 'screenshot', 'rss_wrapper', 'search_result', 'other'))
);

CREATE INDEX IF NOT EXISTS idx_source_incident_urls_normalized
    ON source_incident_urls (normalized_url);
CREATE INDEX IF NOT EXISTS idx_source_incident_urls_resolved
    ON source_incident_urls (resolved_url)
    WHERE resolved_url IS NOT NULL;

CREATE TABLE IF NOT EXISTS source_state (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name text NOT NULL,
    state_scope text NOT NULL DEFAULT 'default',
    cursor_key text NOT NULL DEFAULT 'default',
    state_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_seen_published_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_source_state UNIQUE (source_name, state_scope, cursor_key)
);

CREATE TABLE IF NOT EXISTS article_documents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_incident_id uuid NOT NULL REFERENCES source_incidents(id) ON DELETE CASCADE,
    source_incident_url_id uuid REFERENCES source_incident_urls(id) ON DELETE SET NULL,
    title text,
    author text,
    publish_date date,
    content_text text NOT NULL,
    content_hash text NOT NULL,
    content_language text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_selected_for_enrichment boolean NOT NULL DEFAULT false,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_article_documents_source_incident
    ON article_documents (source_incident_id);
CREATE INDEX IF NOT EXISTS idx_article_documents_content_hash
    ON article_documents (content_hash);
CREATE INDEX IF NOT EXISTS idx_article_documents_selected
    ON article_documents (source_incident_id, is_selected_for_enrichment);

CREATE TABLE IF NOT EXISTS article_fetch_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_incident_id uuid NOT NULL REFERENCES source_incidents(id) ON DELETE CASCADE,
    source_incident_url_id uuid REFERENCES source_incident_urls(id) ON DELETE SET NULL,
    fetch_tier text NOT NULL,
    attempted_at timestamptz NOT NULL DEFAULT now(),
    worker_id text,
    success boolean NOT NULL,
    http_status integer,
    latency_ms integer,
    content_length integer,
    error_code text,
    error_message text,
    response_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_article_fetch_attempts_source_incident
    ON article_fetch_attempts (source_incident_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_article_fetch_attempts_success
    ON article_fetch_attempts (success, attempted_at DESC);

CREATE TABLE IF NOT EXISTS source_enrichments (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_incident_id uuid NOT NULL UNIQUE REFERENCES source_incidents(id) ON DELETE CASCADE,
    article_document_id uuid REFERENCES article_documents(id) ON DELETE SET NULL,
    llm_provider text,
    llm_model text,
    prompt_version text,
    schema_version text,
    mapper_version text,
    post_processing_version text,
    raw_response jsonb,
    raw_extraction jsonb,
    typed_enrichment jsonb,
    enrichment_confidence numeric(5,2),
    is_education_related boolean,
    failed_reason text,
    re_enrich_attempts integer NOT NULL DEFAULT 0,
    re_enrich_reason text,
    manual_review_required boolean NOT NULL DEFAULT false,
    manual_review_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_source_enrichments_is_education_related
    ON source_enrichments (is_education_related);
CREATE INDEX IF NOT EXISTS idx_source_enrichments_manual_review_required
    ON source_enrichments (manual_review_required);

CREATE TABLE IF NOT EXISTS canonical_incidents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_key text UNIQUE,
    status text NOT NULL DEFAULT 'open',
    institution_name text,
    institution_type text,
    vendor_name text,
    country text,
    country_code text,
    region text,
    city text,
    incident_date date,
    date_precision text,
    source_published_at timestamptz,
    attack_category text,
    attack_vector text,
    threat_actor_name text,
    ransomware_family text,
    is_education_related boolean,
    severity text,
    canonical_summary text,
    primary_source_incident_id uuid REFERENCES source_incidents(id) ON DELETE SET NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    resolution_version text NOT NULL DEFAULT 'v2',
    resolution_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_canonical_status CHECK (status IN ('open', 'excluded', 'merged', 'superseded')),
    CONSTRAINT ck_canonical_date_precision CHECK (
        date_precision IS NULL OR date_precision IN ('day', 'week', 'month', 'year', 'approximate', 'unknown')
    )
);

CREATE INDEX IF NOT EXISTS idx_canonical_incidents_incident_date
    ON canonical_incidents (incident_date DESC);
CREATE INDEX IF NOT EXISTS idx_canonical_incidents_country_code
    ON canonical_incidents (country_code);
CREATE INDEX IF NOT EXISTS idx_canonical_incidents_attack_category
    ON canonical_incidents (attack_category);
CREATE INDEX IF NOT EXISTS idx_canonical_incidents_education_related
    ON canonical_incidents (is_education_related);
CREATE INDEX IF NOT EXISTS idx_canonical_incidents_institution_name_trgm
    ON canonical_incidents USING gin (institution_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_canonical_incidents_vendor_name_trgm
    ON canonical_incidents USING gin (vendor_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS canonical_memberships (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_incident_id uuid NOT NULL REFERENCES canonical_incidents(id) ON DELETE CASCADE,
    source_incident_id uuid NOT NULL UNIQUE REFERENCES source_incidents(id) ON DELETE CASCADE,
    match_type text NOT NULL,
    match_score numeric(7,2),
    survivor_score numeric(7,2),
    is_primary_member boolean NOT NULL DEFAULT false,
    field_contribution jsonb NOT NULL DEFAULT '{}'::jsonb,
    matcher_version text NOT NULL,
    matched_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_membership_match_type CHECK (
        match_type IN ('url_exact', 'url_resolved', 'name_date', 'vendor_platform', 'vendor_date', 'vendor_followup', 'manual', 'seed')
    )
);

CREATE INDEX IF NOT EXISTS idx_canonical_memberships_canonical
    ON canonical_memberships (canonical_incident_id);
CREATE INDEX IF NOT EXISTS idx_canonical_memberships_primary
    ON canonical_memberships (canonical_incident_id, is_primary_member);

CREATE TABLE IF NOT EXISTS canonical_enrichments (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_incident_id uuid NOT NULL UNIQUE REFERENCES canonical_incidents(id) ON DELETE CASCADE,
    selected_source_enrichment_id uuid REFERENCES source_enrichments(id) ON DELETE SET NULL,
    merged_from_source_enrichment_ids uuid[] NOT NULL DEFAULT '{}',
    canonical_projection jsonb NOT NULL DEFAULT '{}'::jsonb,
    analytics_projection jsonb NOT NULL DEFAULT '{}'::jsonb,
    field_provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    completeness_score numeric(5,2),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_canonical_enrichments_projection_gin
    ON canonical_enrichments USING gin (analytics_projection);

CREATE TABLE IF NOT EXISTS canonical_timeline_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_incident_id uuid NOT NULL REFERENCES canonical_incidents(id) ON DELETE CASCADE,
    seq_order integer NOT NULL,
    event_date date,
    date_precision text,
    event_type text NOT NULL,
    event_description text,
    actor_attribution text,
    source_enrichment_id uuid REFERENCES source_enrichments(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_canonical_timeline_event UNIQUE (canonical_incident_id, seq_order)
);

CREATE INDEX IF NOT EXISTS idx_canonical_timeline_events_date
    ON canonical_timeline_events (canonical_incident_id, event_date);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    service_name text NOT NULL,
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    error text,
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_pipeline_run_type CHECK (
        run_type IN ('collect', 'fetch', 'enrich', 'canonicalize', 'analytics_refresh', 'reenrich', 'maintenance')
    ),
    CONSTRAINT ck_pipeline_run_status CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'paused')
    )
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_created
    ON pipeline_runs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_tasks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    task_type text NOT NULL,
    target_table text NOT NULL,
    target_id uuid,
    status text NOT NULL DEFAULT 'queued',
    priority integer NOT NULL DEFAULT 100,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    error text,
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_owner text,
    lease_token uuid,
    lease_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_pipeline_task_type CHECK (
        task_type IN (
            'collect',
            'resolve_url',
            'fetch_article',
            'enrich_source',
            'canonicalize',
            'refresh_analytics',
            'reenrich',
            'orchestrate_plan'
        )
    ),
    CONSTRAINT ck_pipeline_task_status CHECK (
        status IN ('queued', 'leased', 'completed', 'failed', 'dead_letter', 'cancelled')
    )
);

CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_lease_queue
    ON pipeline_tasks (status, available_at, priority, created_at)
    WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_lease_expiry
    ON pipeline_tasks (lease_expires_at)
    WHERE status = 'leased';
CREATE INDEX IF NOT EXISTS idx_pipeline_tasks_run_id
    ON pipeline_tasks (run_id);

CREATE TABLE IF NOT EXISTS analytics_refresh_state (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    refresh_key text NOT NULL UNIQUE,
    refresh_scope text NOT NULL DEFAULT 'global',
    needs_refresh boolean NOT NULL DEFAULT true,
    last_refreshed_at timestamptz,
    state_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS research_metric_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_key text NOT NULL,
    snapshot_scope text NOT NULL DEFAULT 'global',
    run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_metric_snapshots_key_captured
    ON research_metric_snapshots (snapshot_key, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_metric_snapshots_run_id
    ON research_metric_snapshots (run_id);
