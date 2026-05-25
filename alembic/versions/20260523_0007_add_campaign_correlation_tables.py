"""add production campaign correlation tables"""

from __future__ import annotations

from alembic import op

revision = "20260523_0007"
down_revision = "20260521_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS campaigns (
            id text PRIMARY KEY,
            campaign_name text NOT NULL,
            campaign_type text NOT NULL,
            status text NOT NULL DEFAULT 'candidate',
            first_seen_date date,
            last_seen_date date,
            actors jsonb NOT NULL DEFAULT '[]'::jsonb,
            vendors jsonb NOT NULL DEFAULT '[]'::jsonb,
            platforms jsonb NOT NULL DEFAULT '[]'::jsonb,
            cves jsonb NOT NULL DEFAULT '[]'::jsonb,
            campaign_names jsonb NOT NULL DEFAULT '[]'::jsonb,
            attack_categories jsonb NOT NULL DEFAULT '[]'::jsonb,
            member_count integer NOT NULL DEFAULT 0,
            confirmed_member_count integer NOT NULL DEFAULT 0,
            evidence_only_member_count integer NOT NULL DEFAULT 0,
            confidence numeric(5,3),
            analyst_summary text,
            analyst_notes text,
            is_name_pinned boolean NOT NULL DEFAULT false,
            correlation_version text NOT NULL,
            last_correlated_at timestamptz,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_campaigns_campaign_type CHECK (
                campaign_type IN ('same_campaign', 'shared_vendor_incident', 'mass_exploitation', 'actor_activity_wave', 'roundup_not_campaign', 'unrelated')
            ),
            CONSTRAINT ck_campaigns_status CHECK (
                status IN ('candidate', 'analyst_reviewed', 'suppressed')
            )
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaigns_status_confidence ON campaigns (status, confidence DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaigns_type ON campaigns (campaign_type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaigns_metadata_gin ON campaigns USING gin (metadata)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_memberships (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            campaign_id text NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            canonical_incident_id uuid NOT NULL REFERENCES canonical_incidents(id) ON DELETE CASCADE,
            role text NOT NULL,
            confidence numeric(5,3),
            evidence_article_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
            evidence_source_incident_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
            evidence_quotes jsonb NOT NULL DEFAULT '[]'::jsonb,
            review_status text NOT NULL DEFAULT 'candidate_unreviewed',
            victim_name text,
            canonical_status text,
            reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT uq_campaign_membership_incident UNIQUE (campaign_id, canonical_incident_id),
            CONSTRAINT ck_campaign_memberships_role CHECK (
                role IN ('direct_victim', 'affected_via_vendor', 'vendor_operator', 'mentioned_only', 'needs_review')
            ),
            CONSTRAINT ck_campaign_memberships_review_status CHECK (
                review_status IN ('candidate_unreviewed', 'true_positive', 'false_positive', 'uncertain', 'excluded_evidence_only', 'manual_review_required')
            )
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaign_memberships_campaign ON campaign_memberships (campaign_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaign_memberships_canonical ON campaign_memberships (canonical_incident_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaign_memberships_review ON campaign_memberships (review_status)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_evidence_items (
            id text PRIMARY KEY,
            campaign_id text NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            canonical_incident_id uuid NOT NULL REFERENCES canonical_incidents(id) ON DELETE CASCADE,
            source_incident_id uuid,
            article_document_id uuid,
            source_url text,
            source_title text,
            article_title text,
            evidence_quotes jsonb NOT NULL DEFAULT '[]'::jsonb,
            vendors jsonb NOT NULL DEFAULT '[]'::jsonb,
            platforms jsonb NOT NULL DEFAULT '[]'::jsonb,
            actors jsonb NOT NULL DEFAULT '[]'::jsonb,
            cves jsonb NOT NULL DEFAULT '[]'::jsonb,
            evidence_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaign_evidence_campaign ON campaign_evidence_items (campaign_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaign_evidence_canonical ON campaign_evidence_items (canonical_incident_id)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_signatures (
            id text PRIMARY KEY,
            campaign_id text NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            status text NOT NULL DEFAULT 'candidate',
            signature_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            correlation_version text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_campaign_signatures_status CHECK (
                status IN ('candidate', 'analyst_reviewed', 'suppressed')
            )
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_campaign_signatures_campaign ON campaign_signatures (campaign_id)")

    op.execute("ALTER TABLE IF EXISTS pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_run_type")
    op.execute("ALTER TABLE IF EXISTS pipeline_runs DROP CONSTRAINT IF EXISTS ck_pipeline_run_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_runs
        ADD CONSTRAINT ck_pipeline_run_type
        CHECK (
            run_type IN ('collect', 'fetch', 'enrich', 'canonicalize', 'analytics_refresh', 'campaign_correlation', 'reenrich', 'maintenance')
        )
        """
    )
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS pipeline_tasks_task_type")
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS ck_pipeline_task_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_tasks
        ADD CONSTRAINT ck_pipeline_task_type
        CHECK (
            task_type IN (
                'collect',
                'resolve_url',
                'fetch_article',
                'enrich_source',
                'canonicalize',
                'refresh_analytics',
                'campaign_correlate',
                'reenrich',
                'orchestrate_plan'
            )
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS ck_pipeline_task_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_tasks
        ADD CONSTRAINT ck_pipeline_task_type
        CHECK (
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
        )
        """
    )
    op.execute("ALTER TABLE IF EXISTS pipeline_runs DROP CONSTRAINT IF EXISTS ck_pipeline_run_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_runs
        ADD CONSTRAINT ck_pipeline_run_type
        CHECK (
            run_type IN ('collect', 'fetch', 'enrich', 'canonicalize', 'analytics_refresh', 'reenrich', 'maintenance')
        )
        """
    )
    op.execute("DROP TABLE IF EXISTS campaign_signatures CASCADE")
    op.execute("DROP TABLE IF EXISTS campaign_evidence_items CASCADE")
    op.execute("DROP TABLE IF EXISTS campaign_memberships CASCADE")
    op.execute("DROP TABLE IF EXISTS campaigns CASCADE")
