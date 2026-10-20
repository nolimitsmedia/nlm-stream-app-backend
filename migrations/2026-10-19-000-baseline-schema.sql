-- NLM Streaming App — BASELINE SCHEMA (structure only, no data)
--
-- Source: pg_dump --schema-only --no-owner --no-privileges of the production
--         database (PostgreSQL 18.6), taken 2026-10-19, BEFORE Phase 2.
-- Current schema = this file + every later file in migrations/, in name order
--         (2026-10-20-phase2-playback-id.sql adds channels.playback_id).
--
-- PURPOSE: build a NEW, EMPTY database (staging, test, disaster recovery).
-- NEVER run this against the production database or any database that
-- already has tables — CREATE TABLE statements will fail (and are meant to).
--
-- Usage (empty database):
--   psql -v ON_ERROR_STOP=1 --single-transaction -d <empty_db> \
--        -f migrations/2026-10-19-000-baseline-schema.sql
--   psql -v ON_ERROR_STOP=1 -d <empty_db> \
--        -f migrations/2026-10-20-phase2-playback-id.sql
--
-- Portability edits vs. the raw dump (nothing else changed): removed the
-- psql-18-only "\restrict"/"\unrestrict" meta-commands and
-- "SET transaction_timeout" (PostgreSQL 17+), so the file also loads on
-- PostgreSQL 13–16 (gen_random_uuid, used by later migrations, needs 13+).
--
-- Tables (38): created by this file. The application ALSO runs
-- CREATE TABLE/COLUMN IF NOT EXISTS at startup; those statements are no-ops
-- on a database built from this baseline.

--
-- PostgreSQL database dump
--


-- Dumped from database version 18.6 (Debian 18.6-1.pgdg13+2)
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS '';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admins; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admins (
    id integer NOT NULL,
    name character varying(150) NOT NULL,
    email character varying(150) NOT NULL,
    password_hash text NOT NULL,
    role character varying(50) DEFAULT 'admin'::character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: admins_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.admins_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: admins_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.admins_id_seq OWNED BY public.admins.id;


--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_keys (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    key_hash character varying(64) NOT NULL,
    key_prefix character varying(20) NOT NULL,
    label character varying(255),
    created_by_admin_id integer,
    created_at timestamp with time zone DEFAULT now(),
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone
);


--
-- Name: api_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.api_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: api_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.api_keys_id_seq OWNED BY public.api_keys.id;


--
-- Name: channel_ha_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_ha_events (
    id bigint NOT NULL,
    organization_id integer NOT NULL,
    channel_id integer NOT NULL,
    event_type character varying(64) NOT NULL,
    source_id integer,
    previous_source_id integer,
    new_source_id integer,
    reason character varying(128),
    status character varying(32) DEFAULT 'completed'::character varying NOT NULL,
    duration_ms integer,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: channel_ha_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channel_ha_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_ha_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channel_ha_events_id_seq OWNED BY public.channel_ha_events.id;


--
-- Name: channel_media_node_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_media_node_migrations (
    id bigint NOT NULL,
    channel_id integer NOT NULL,
    organization_id integer NOT NULL,
    source_media_node_id bigint,
    destination_media_node_id bigint,
    requested_by_admin_id integer,
    status character varying(32) DEFAULT 'requested'::character varying NOT NULL,
    reason text,
    started_at timestamp with time zone,
    prepared_at timestamp with time zone,
    ownership_committed_at timestamp with time zone,
    completed_at timestamp with time zone,
    failed_at timestamp with time zone,
    failure_code character varying(64),
    failure_message text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_media_node_migrations_distinct_nodes_check CHECK (((source_media_node_id IS NULL) OR (destination_media_node_id IS NULL) OR (source_media_node_id <> destination_media_node_id))),
    CONSTRAINT channel_media_node_migrations_status_check CHECK (((status)::text = ANY ((ARRAY['requested'::character varying, 'validating'::character varying, 'prepared'::character varying, 'stopping_source'::character varying, 'ownership_committing'::character varying, 'completed'::character varying, 'failed'::character varying, 'cancelled'::character varying])::text[])))
);


--
-- Name: channel_media_node_migrations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channel_media_node_migrations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_media_node_migrations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channel_media_node_migrations_id_seq OWNED BY public.channel_media_node_migrations.id;


--
-- Name: channel_pull_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_pull_sources (
    id integer NOT NULL,
    channel_id integer NOT NULL,
    organization_id integer NOT NULL,
    name character varying(255) DEFAULT 'Pull Source'::character varying NOT NULL,
    protocol character varying(20) NOT NULL,
    source_url text NOT NULL,
    source_url_display text,
    enabled boolean DEFAULT true NOT NULL,
    auto_start boolean DEFAULT false NOT NULL,
    auto_reconnect boolean DEFAULT true NOT NULL,
    status character varying(32) DEFAULT 'stopped'::character varying NOT NULL,
    is_running boolean DEFAULT false NOT NULL,
    started_at timestamp with time zone,
    stopped_at timestamp with time zone,
    last_error text,
    last_error_code character varying(64),
    reconnect_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    role character varying(16) DEFAULT 'primary'::character varying NOT NULL,
    priority integer DEFAULT 1 NOT NULL,
    is_active_source boolean DEFAULT false NOT NULL,
    health_status character varying(24) DEFAULT 'unknown'::character varying NOT NULL,
    last_health_check_at timestamp with time zone
);


--
-- Name: channel_pull_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channel_pull_sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_pull_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channel_pull_sources_id_seq OWNED BY public.channel_pull_sources.id;


--
-- Name: channel_source_failover; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_source_failover (
    channel_id integer NOT NULL,
    organization_id integer NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    failure_threshold_seconds integer DEFAULT 5 NOT NULL,
    failback_enabled boolean DEFAULT true NOT NULL,
    failback_stability_seconds integer DEFAULT 15 NOT NULL,
    active_source_id integer,
    last_switch_at timestamp with time zone,
    last_switch_reason character varying(64),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: channels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channels (
    id integer NOT NULL,
    name character varying(150) NOT NULL,
    stream_key character varying(150) NOT NULL,
    description text,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    organization_id integer,
    is_live boolean DEFAULT false NOT NULL,
    live_started_at timestamp with time zone,
    embed_token character varying(64),
    embed_settings jsonb DEFAULT '{}'::jsonb,
    auto_record_enabled boolean DEFAULT true,
    media_node_id bigint
);


--
-- Name: channels_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channels_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channels_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channels_id_seq OWNED BY public.channels.id;


--
-- Name: cta_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cta_links (
    id integer NOT NULL,
    label character varying(120) NOT NULL,
    title character varying(180) NOT NULL,
    description text,
    url text NOT NULL,
    button_text character varying(80) DEFAULT 'Open Link'::character varying,
    type character varying(40) DEFAULT 'donation'::character varying,
    stream_key character varying(255),
    is_active boolean DEFAULT true,
    sort_order integer DEFAULT 0,
    created_by integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    organization_id integer
);


--
-- Name: cta_links_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.cta_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cta_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.cta_links_id_seq OWNED BY public.cta_links.id;


--
-- Name: feature_flags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.feature_flags (
    key character varying(80) NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    description text,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: live_chat_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.live_chat_messages (
    id integer NOT NULL,
    stream_key character varying(255) NOT NULL,
    display_name character varying(120) NOT NULL,
    message text NOT NULL,
    is_pinned boolean DEFAULT false,
    is_hidden boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    organization_id integer
);


--
-- Name: live_chat_messages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.live_chat_messages_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: live_chat_messages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.live_chat_messages_id_seq OWNED BY public.live_chat_messages.id;


--
-- Name: media_nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.media_nodes (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    hostname character varying(255),
    public_ip inet,
    private_ip inet,
    region character varying(64),
    role character varying(32) DEFAULT 'media'::character varying NOT NULL,
    status character varying(32) DEFAULT 'offline'::character varying NOT NULL,
    is_enabled boolean DEFAULT true NOT NULL,
    is_draining boolean DEFAULT false NOT NULL,
    max_streams integer,
    active_streams integer DEFAULT 0 NOT NULL,
    cpu_percent numeric(5,2),
    memory_percent numeric(5,2),
    disk_percent numeric(5,2),
    load_1m numeric(8,2),
    ffmpeg_processes integer DEFAULT 0 NOT NULL,
    srs_healthy boolean DEFAULT false NOT NULL,
    api_healthy boolean DEFAULT false NOT NULL,
    last_heartbeat_at timestamp with time zone,
    last_error text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: media_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.media_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: media_nodes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.media_nodes_id_seq OWNED BY public.media_nodes.id;


--
-- Name: notification_preferences; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.notification_preferences (
    admin_id integer NOT NULL,
    stream_live boolean DEFAULT true NOT NULL,
    chat_needs_moderation boolean DEFAULT false NOT NULL,
    recording_processed boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: organization_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organization_settings (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    watch_page_title character varying(255),
    logo_url text,
    primary_color character varying(40) DEFAULT '#0d6efd'::character varying,
    secondary_color character varying(40) DEFAULT '#fd9d00'::character varying,
    donation_url text,
    custom_css text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: organization_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.organization_settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: organization_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.organization_settings_id_seq OWNED BY public.organization_settings.id;


--
-- Name: organization_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organization_users (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    admin_id integer NOT NULL,
    role character varying(40) DEFAULT 'owner'::character varying,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: organization_users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.organization_users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: organization_users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.organization_users_id_seq OWNED BY public.organization_users.id;


--
-- Name: organizations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organizations (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    slug character varying(255) NOT NULL,
    logo_url text,
    primary_color character varying(40) DEFAULT '#0d6efd'::character varying,
    custom_domain character varying(255),
    subscription_plan character varying(80) DEFAULT 'starter'::character varying,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    bunny_pull_zone_id character varying(40),
    bunny_pull_zone_hostname character varying(255),
    bunny_storage_zone_id character varying(40),
    bunny_storage_zone_name character varying(255),
    bunny_storage_zone_hostname character varying(255),
    bunny_storage_zone_password text,
    bunny_provisioned_at timestamp with time zone,
    bunny_recordings_pull_zone_id character varying(40),
    bunny_recordings_cdn_url character varying(255),
    transcoding_override boolean
);


--
-- Name: organizations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.organizations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: organizations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.organizations_id_seq OWNED BY public.organizations.id;


--
-- Name: overlay_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.overlay_history (
    id integer NOT NULL,
    stream_key character varying(255) NOT NULL,
    overlay_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    action character varying(40) DEFAULT 'show'::character varying NOT NULL,
    created_by integer,
    created_at timestamp with time zone DEFAULT now(),
    organization_id integer
);


--
-- Name: overlay_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.overlay_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: overlay_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.overlay_history_id_seq OWNED BY public.overlay_history.id;


--
-- Name: overlay_states; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.overlay_states (
    stream_key character varying(255) NOT NULL,
    overlay_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_visible boolean DEFAULT false,
    updated_by integer,
    updated_at timestamp with time zone DEFAULT now(),
    organization_id integer
);


--
-- Name: pending_signups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pending_signups (
    id integer NOT NULL,
    checkout_session_id character varying(255),
    stripe_customer_id character varying(255),
    plan_key character varying(80) NOT NULL,
    organization_name character varying(255) NOT NULL,
    client_name character varying(255) NOT NULL,
    client_email character varying(255) NOT NULL,
    password_hash text NOT NULL,
    stream_key character varying(255) NOT NULL,
    primary_color character varying(40) DEFAULT '#0d6efd'::character varying,
    secondary_color character varying(40) DEFAULT '#fd9d00'::character varying,
    donation_url text,
    status character varying(40) DEFAULT 'pending'::character varying,
    organization_id integer,
    admin_id integer,
    channel_id integer,
    created_at timestamp with time zone DEFAULT now(),
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: pending_signups_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pending_signups_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pending_signups_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pending_signups_id_seq OWNED BY public.pending_signups.id;


--
-- Name: plan_alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.plan_alerts (
    id integer NOT NULL,
    organization_id integer,
    channel_id integer,
    alert_type character varying(60) NOT NULL,
    message text NOT NULL,
    observed_bitrate_kbps integer,
    plan_bitrate_kbps integer,
    acknowledged boolean DEFAULT false,
    acknowledged_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    overage_bytes bigint,
    email_sent boolean DEFAULT false,
    email_sent_at timestamp with time zone
);


--
-- Name: plan_alerts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.plan_alerts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: plan_alerts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.plan_alerts_id_seq OWNED BY public.plan_alerts.id;


--
-- Name: plans; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.plans (
    id integer NOT NULL,
    plan_key character varying(80) NOT NULL,
    name character varying(120) NOT NULL,
    monthly_price_cents integer DEFAULT 0,
    max_channels integer DEFAULT 1,
    max_admins integer DEFAULT 2,
    max_storage_gb integer DEFAULT 25,
    max_bitrate_kbps integer DEFAULT 6000,
    transcoding_enabled boolean DEFAULT false,
    analytics_enabled boolean DEFAULT false,
    custom_domain_enabled boolean DEFAULT false,
    priority_support_enabled boolean DEFAULT false,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    stripe_price_id character varying(255),
    whmcs_product_id character varying(40),
    whmcs_description text,
    max_cdn_bandwidth_gb integer DEFAULT 0,
    max_egress_bandwidth_gb integer DEFAULT 0,
    tv_channel_enabled boolean DEFAULT false,
    recording_enabled boolean DEFAULT false,
    rewind_enabled boolean DEFAULT false,
    reduced_latency_enabled boolean DEFAULT false
);


--
-- Name: plans_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.plans_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: plans_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.plans_id_seq OWNED BY public.plans.id;


--
-- Name: prayer_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prayer_requests (
    id integer NOT NULL,
    stream_key character varying(255) NOT NULL,
    display_name character varying(120),
    request_text text NOT NULL,
    is_anonymous boolean DEFAULT false,
    status character varying(40) DEFAULT 'new'::character varying,
    prayed_at timestamp with time zone,
    reviewed_by integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    organization_id integer
);


--
-- Name: prayer_requests_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.prayer_requests_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: prayer_requests_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.prayer_requests_id_seq OWNED BY public.prayer_requests.id;


--
-- Name: processed_whmcs_orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.processed_whmcs_orders (
    whmcs_order_id character varying(40) NOT NULL,
    organization_id integer,
    outcome character varying(40) NOT NULL,
    processed_at timestamp with time zone DEFAULT now()
);


--
-- Name: recordings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.recordings (
    id integer NOT NULL,
    channel_id integer,
    filename text,
    filepath text,
    started_at timestamp without time zone,
    ended_at timestamp without time zone,
    duration integer,
    size_mb numeric,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    organization_id integer,
    stream_key character varying(255),
    file_type character varying(40),
    file_size_bytes bigint,
    duration_seconds integer,
    status character varying(40) DEFAULT 'archived'::character varying,
    source character varying(80) DEFAULT 'local_srs'::character varying,
    updated_at timestamp with time zone DEFAULT now(),
    mp4_filename character varying(255),
    mp4_filepath text,
    thumbnail_filename character varying(255),
    thumbnail_filepath text,
    processing_status character varying(40) DEFAULT 'pending'::character varying,
    processing_error text,
    converted_at timestamp with time zone,
    thumbnail_generated_at timestamp with time zone,
    width integer,
    height integer,
    bitrate_kbps integer,
    codec character varying(80),
    is_public boolean DEFAULT false,
    public_slug character varying(255),
    public_title character varying(255),
    public_description text,
    published_at timestamp with time zone,
    replay_views integer DEFAULT 0,
    replay_category character varying(120),
    replay_tags text,
    replay_visibility character varying(40) DEFAULT 'public'::character varying,
    bunny_storage_path text,
    bunny_archived_at timestamp without time zone,
    archive_status character varying(20) DEFAULT 'local'::character varying NOT NULL,
    bunny_cdn_url text
);


--
-- Name: recordings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.recordings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recordings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.recordings_id_seq OWNED BY public.recordings.id;


--
-- Name: replay_saved_replays; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replay_saved_replays (
    id integer NOT NULL,
    member_id integer NOT NULL,
    recording_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: replay_saved_replays_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.replay_saved_replays_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: replay_saved_replays_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.replay_saved_replays_id_seq OWNED BY public.replay_saved_replays.id;


--
-- Name: replay_session_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replay_session_events (
    id integer NOT NULL,
    organization_id integer,
    recording_id integer,
    replay_session_id integer,
    public_slug character varying(255) NOT NULL,
    viewer_id character varying(255) NOT NULL,
    event_type character varying(80) NOT NULL,
    current_time_seconds integer DEFAULT 0,
    watched_seconds integer DEFAULT 0,
    delta_seconds integer DEFAULT 0,
    playback_rate numeric DEFAULT 1,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: replay_session_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.replay_session_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: replay_session_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.replay_session_events_id_seq OWNED BY public.replay_session_events.id;


--
-- Name: replay_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replay_sessions (
    id integer NOT NULL,
    organization_id integer,
    recording_id integer,
    public_slug character varying(255) NOT NULL,
    viewer_id character varying(255) NOT NULL,
    session_token character varying(255) NOT NULL,
    ip_address character varying(100),
    user_agent text,
    referrer text,
    started_at timestamp with time zone DEFAULT now(),
    last_seen_at timestamp with time zone DEFAULT now(),
    ended_at timestamp with time zone,
    current_time_seconds integer DEFAULT 0,
    max_position_seconds integer DEFAULT 0,
    watched_seconds integer DEFAULT 0,
    completed boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    member_id integer,
    device_type character varying(80),
    browser_name character varying(120),
    os_name character varying(120),
    country_code character varying(10),
    country_name character varying(120),
    last_event_type character varying(80),
    last_playback_rate numeric DEFAULT 1,
    heartbeat_count integer DEFAULT 0
);


--
-- Name: replay_sessions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.replay_sessions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: replay_sessions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.replay_sessions_id_seq OWNED BY public.replay_sessions.id;


--
-- Name: replay_viewer_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replay_viewer_members (
    id integer NOT NULL,
    organization_id integer,
    name character varying(255) NOT NULL,
    email character varying(255) NOT NULL,
    password_hash text NOT NULL,
    status character varying(40) DEFAULT 'active'::character varying,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: replay_viewer_members_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.replay_viewer_members_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: replay_viewer_members_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.replay_viewer_members_id_seq OWNED BY public.replay_viewer_members.id;


--
-- Name: restart_audit_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.restart_audit_log (
    id integer NOT NULL,
    admin_id integer,
    admin_email character varying(255),
    action character varying(20) NOT NULL,
    reason text,
    active_streams_at_time integer DEFAULT 0 NOT NULL,
    affected_organizations text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: restart_audit_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.restart_audit_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: restart_audit_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.restart_audit_log_id_seq OWNED BY public.restart_audit_log.id;


--
-- Name: scheduled_streams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scheduled_streams (
    id integer NOT NULL,
    channel_id integer,
    title character varying(255) NOT NULL,
    stream_key character varying(255) NOT NULL,
    description text,
    scheduled_start timestamp with time zone NOT NULL,
    scheduled_end timestamp with time zone,
    timezone character varying(100) DEFAULT 'America/Los_Angeles'::character varying,
    status character varying(40) DEFAULT 'scheduled'::character varying,
    created_by integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    organization_id integer,
    automation_enabled boolean DEFAULT false,
    automation_start_targets boolean DEFAULT true,
    automation_stop_targets boolean DEFAULT true,
    automation_started_at timestamp with time zone,
    automation_ended_at timestamp with time zone,
    automation_last_checked_at timestamp with time zone,
    automation_last_error text,
    automation_attempts integer DEFAULT 0
);


--
-- Name: scheduled_streams_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scheduled_streams_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scheduled_streams_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scheduled_streams_id_seq OWNED BY public.scheduled_streams.id;


--
-- Name: server_metrics_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.server_metrics_history (
    id integer NOT NULL,
    recorded_at timestamp with time zone DEFAULT now(),
    cpu_percent integer,
    mem_used_percent integer,
    disk_used_percent integer,
    live_stream_count integer,
    live_viewer_count integer,
    incoming_kbps integer
);


--
-- Name: server_metrics_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.server_metrics_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: server_metrics_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.server_metrics_history_id_seq OWNED BY public.server_metrics_history.id;


--
-- Name: social_destinations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.social_destinations (
    id integer NOT NULL,
    channel_id integer NOT NULL,
    platform character varying(20) NOT NULL,
    stream_key text NOT NULL,
    is_running boolean DEFAULT false NOT NULL,
    ffmpeg_pid integer,
    started_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    oauth_account_id integer,
    platform_broadcast_id character varying(255),
    platform_stream_id character varying(255),
    automation_mode character varying(20) DEFAULT 'manual'::character varying,
    name character varying(255),
    target_type character varying(40),
    destination_url text,
    protocol character varying(20),
    enabled boolean DEFAULT true,
    auto_start boolean DEFAULT false,
    auto_reconnect boolean DEFAULT true,
    status character varying(30) DEFAULT 'stopped'::character varying,
    reconnect_count integer DEFAULT 0,
    dropped_frames bigint DEFAULT 0,
    current_bitrate_kbps integer DEFAULT 0,
    last_error text,
    last_connected_at timestamp with time zone,
    last_disconnected_at timestamp with time zone,
    active_destination_url text,
    target_metadata jsonb DEFAULT '{}'::jsonb,
    stream_key_lookup_hash character varying(64),
    credential_encryption_version integer DEFAULT 0
);


--
-- Name: social_destinations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.social_destinations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: social_destinations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.social_destinations_id_seq OWNED BY public.social_destinations.id;


--
-- Name: social_oauth_accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.social_oauth_accounts (
    id integer NOT NULL,
    organization_id integer,
    platform character varying(20) NOT NULL,
    external_account_id character varying(255) NOT NULL,
    external_account_name character varying(255),
    access_token text NOT NULL,
    refresh_token text,
    token_expires_at timestamp with time zone,
    connected_by_admin_id integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    token_encryption_version integer DEFAULT 0,
    connection_status character varying(32) DEFAULT 'connected'::character varying,
    token_last_validated_at timestamp with time zone,
    token_last_error text,
    reconnect_required_at timestamp with time zone
);


--
-- Name: social_oauth_accounts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.social_oauth_accounts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: social_oauth_accounts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.social_oauth_accounts_id_seq OWNED BY public.social_oauth_accounts.id;


--
-- Name: stream_health_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stream_health_history (
    id bigint NOT NULL,
    organization_id integer NOT NULL,
    channel_id integer NOT NULL,
    broadcast_started_at timestamp with time zone NOT NULL,
    broadcast_generation integer DEFAULT 0 NOT NULL,
    event_type character varying(60) NOT NULL,
    monitor_status character varying(30) DEFAULT 'unknown'::character varying NOT NULL,
    monitor_score integer,
    media_status character varying(30) DEFAULT 'unknown'::character varying NOT NULL,
    media_score integer,
    media_pending boolean DEFAULT false NOT NULL,
    operational_state character varying(30) DEFAULT 'unknown'::character varying NOT NULL,
    operational_severity character varying(20) DEFAULT 'info'::character varying NOT NULL,
    attention_required boolean DEFAULT false NOT NULL,
    reason_code character varying(100),
    issue_count integer DEFAULT 0 NOT NULL,
    critical_count integer DEFAULT 0 NOT NULL,
    warning_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stream_health_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stream_health_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stream_health_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stream_health_history_id_seq OWNED BY public.stream_health_history.id;


--
-- Name: stream_target_health_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stream_target_health_history (
    id bigint NOT NULL,
    organization_id integer NOT NULL,
    channel_id integer NOT NULL,
    target_id integer NOT NULL,
    event_type character varying(60) NOT NULL,
    health_status character varying(30) NOT NULL,
    severity character varying(20) DEFAULT 'info'::character varying NOT NULL,
    requires_attention boolean DEFAULT false NOT NULL,
    failure_code character varying(100),
    failure_category character varying(60),
    failure_scope character varying(60),
    failure_retryable boolean,
    message text,
    execution character varying(20),
    reconnect_count integer DEFAULT 0 NOT NULL,
    delivery_verified boolean DEFAULT false NOT NULL,
    current_bitrate_kbps integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stream_target_health_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stream_target_health_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stream_target_health_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stream_target_health_history_id_seq OWNED BY public.stream_target_health_history.id;


--
-- Name: streams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.streams (
    id integer NOT NULL,
    name text NOT NULL,
    stream_key text NOT NULL,
    description text,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: streams_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.streams_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: streams_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.streams_id_seq OWNED BY public.streams.id;


--
-- Name: subscriptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.subscriptions (
    id integer NOT NULL,
    organization_id integer NOT NULL,
    plan_key character varying(80) DEFAULT 'starter'::character varying NOT NULL,
    status character varying(40) DEFAULT 'trialing'::character varying NOT NULL,
    trial_ends_at timestamp with time zone DEFAULT (now() + '14 days'::interval),
    current_period_start timestamp with time zone DEFAULT now(),
    current_period_end timestamp with time zone DEFAULT (now() + '30 days'::interval),
    stripe_customer_id character varying(255),
    stripe_subscription_id character varying(255),
    cancel_at_period_end boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    whmcs_client_id character varying(40),
    whmcs_order_id character varying(40),
    whmcs_invoice_id character varying(40)
);


--
-- Name: subscriptions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.subscriptions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: subscriptions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.subscriptions_id_seq OWNED BY public.subscriptions.id;


--
-- Name: viewer_reactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_reactions (
    id integer NOT NULL,
    stream_key character varying(255) NOT NULL,
    reaction_type character varying(40) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    organization_id integer
);


--
-- Name: viewer_reactions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.viewer_reactions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: viewer_reactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.viewer_reactions_id_seq OWNED BY public.viewer_reactions.id;


--
-- Name: viewer_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_sessions (
    id integer NOT NULL,
    organization_id integer,
    stream_key character varying(255) NOT NULL,
    viewer_id character varying(255) NOT NULL,
    session_token character varying(255) NOT NULL,
    ip_address character varying(100),
    user_agent text,
    referrer text,
    started_at timestamp with time zone DEFAULT now(),
    last_seen_at timestamp with time zone DEFAULT now(),
    ended_at timestamp with time zone,
    duration_seconds integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    country_code character varying(12),
    country_name character varying(120),
    device_type character varying(40),
    browser_name character varying(80),
    os_name character varying(80)
);


--
-- Name: viewer_sessions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.viewer_sessions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: viewer_sessions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.viewer_sessions_id_seq OWNED BY public.viewer_sessions.id;


--
-- Name: admins id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admins ALTER COLUMN id SET DEFAULT nextval('public.admins_id_seq'::regclass);


--
-- Name: api_keys id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys ALTER COLUMN id SET DEFAULT nextval('public.api_keys_id_seq'::regclass);


--
-- Name: channel_ha_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events ALTER COLUMN id SET DEFAULT nextval('public.channel_ha_events_id_seq'::regclass);


--
-- Name: channel_media_node_migrations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations ALTER COLUMN id SET DEFAULT nextval('public.channel_media_node_migrations_id_seq'::regclass);


--
-- Name: channel_pull_sources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_pull_sources ALTER COLUMN id SET DEFAULT nextval('public.channel_pull_sources_id_seq'::regclass);


--
-- Name: channels id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels ALTER COLUMN id SET DEFAULT nextval('public.channels_id_seq'::regclass);


--
-- Name: cta_links id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cta_links ALTER COLUMN id SET DEFAULT nextval('public.cta_links_id_seq'::regclass);


--
-- Name: live_chat_messages id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.live_chat_messages ALTER COLUMN id SET DEFAULT nextval('public.live_chat_messages_id_seq'::regclass);


--
-- Name: media_nodes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_nodes ALTER COLUMN id SET DEFAULT nextval('public.media_nodes_id_seq'::regclass);


--
-- Name: organization_settings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_settings ALTER COLUMN id SET DEFAULT nextval('public.organization_settings_id_seq'::regclass);


--
-- Name: organization_users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_users ALTER COLUMN id SET DEFAULT nextval('public.organization_users_id_seq'::regclass);


--
-- Name: organizations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations ALTER COLUMN id SET DEFAULT nextval('public.organizations_id_seq'::regclass);


--
-- Name: overlay_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.overlay_history ALTER COLUMN id SET DEFAULT nextval('public.overlay_history_id_seq'::regclass);


--
-- Name: pending_signups id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_signups ALTER COLUMN id SET DEFAULT nextval('public.pending_signups_id_seq'::regclass);


--
-- Name: plan_alerts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plan_alerts ALTER COLUMN id SET DEFAULT nextval('public.plan_alerts_id_seq'::regclass);


--
-- Name: plans id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plans ALTER COLUMN id SET DEFAULT nextval('public.plans_id_seq'::regclass);


--
-- Name: prayer_requests id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prayer_requests ALTER COLUMN id SET DEFAULT nextval('public.prayer_requests_id_seq'::regclass);


--
-- Name: recordings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recordings ALTER COLUMN id SET DEFAULT nextval('public.recordings_id_seq'::regclass);


--
-- Name: replay_saved_replays id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_saved_replays ALTER COLUMN id SET DEFAULT nextval('public.replay_saved_replays_id_seq'::regclass);


--
-- Name: replay_session_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_session_events ALTER COLUMN id SET DEFAULT nextval('public.replay_session_events_id_seq'::regclass);


--
-- Name: replay_sessions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_sessions ALTER COLUMN id SET DEFAULT nextval('public.replay_sessions_id_seq'::regclass);


--
-- Name: replay_viewer_members id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_viewer_members ALTER COLUMN id SET DEFAULT nextval('public.replay_viewer_members_id_seq'::regclass);


--
-- Name: restart_audit_log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restart_audit_log ALTER COLUMN id SET DEFAULT nextval('public.restart_audit_log_id_seq'::regclass);


--
-- Name: scheduled_streams id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheduled_streams ALTER COLUMN id SET DEFAULT nextval('public.scheduled_streams_id_seq'::regclass);


--
-- Name: server_metrics_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.server_metrics_history ALTER COLUMN id SET DEFAULT nextval('public.server_metrics_history_id_seq'::regclass);


--
-- Name: social_destinations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_destinations ALTER COLUMN id SET DEFAULT nextval('public.social_destinations_id_seq'::regclass);


--
-- Name: social_oauth_accounts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_oauth_accounts ALTER COLUMN id SET DEFAULT nextval('public.social_oauth_accounts_id_seq'::regclass);


--
-- Name: stream_health_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_health_history ALTER COLUMN id SET DEFAULT nextval('public.stream_health_history_id_seq'::regclass);


--
-- Name: stream_target_health_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_target_health_history ALTER COLUMN id SET DEFAULT nextval('public.stream_target_health_history_id_seq'::regclass);


--
-- Name: streams id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.streams ALTER COLUMN id SET DEFAULT nextval('public.streams_id_seq'::regclass);


--
-- Name: subscriptions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscriptions ALTER COLUMN id SET DEFAULT nextval('public.subscriptions_id_seq'::regclass);


--
-- Name: viewer_reactions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_reactions ALTER COLUMN id SET DEFAULT nextval('public.viewer_reactions_id_seq'::regclass);


--
-- Name: viewer_sessions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_sessions ALTER COLUMN id SET DEFAULT nextval('public.viewer_sessions_id_seq'::regclass);


--
-- Name: admins admins_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admins
    ADD CONSTRAINT admins_email_key UNIQUE (email);


--
-- Name: admins admins_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admins
    ADD CONSTRAINT admins_pkey PRIMARY KEY (id);


--
-- Name: api_keys api_keys_key_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_key_hash_key UNIQUE (key_hash);


--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: channel_ha_events channel_ha_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events
    ADD CONSTRAINT channel_ha_events_pkey PRIMARY KEY (id);


--
-- Name: channel_media_node_migrations channel_media_node_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations
    ADD CONSTRAINT channel_media_node_migrations_pkey PRIMARY KEY (id);


--
-- Name: channel_pull_sources channel_pull_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_pull_sources
    ADD CONSTRAINT channel_pull_sources_pkey PRIMARY KEY (id);


--
-- Name: channel_source_failover channel_source_failover_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_source_failover
    ADD CONSTRAINT channel_source_failover_pkey PRIMARY KEY (channel_id);


--
-- Name: channels channels_embed_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_embed_token_key UNIQUE (embed_token);


--
-- Name: channels channels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (id);


--
-- Name: channels channels_stream_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_stream_key_key UNIQUE (stream_key);


--
-- Name: cta_links cta_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cta_links
    ADD CONSTRAINT cta_links_pkey PRIMARY KEY (id);


--
-- Name: feature_flags feature_flags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.feature_flags
    ADD CONSTRAINT feature_flags_pkey PRIMARY KEY (key);


--
-- Name: live_chat_messages live_chat_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.live_chat_messages
    ADD CONSTRAINT live_chat_messages_pkey PRIMARY KEY (id);


--
-- Name: media_nodes media_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_nodes
    ADD CONSTRAINT media_nodes_pkey PRIMARY KEY (id);


--
-- Name: notification_preferences notification_preferences_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.notification_preferences
    ADD CONSTRAINT notification_preferences_pkey PRIMARY KEY (admin_id);


--
-- Name: organization_settings organization_settings_organization_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_settings
    ADD CONSTRAINT organization_settings_organization_id_key UNIQUE (organization_id);


--
-- Name: organization_settings organization_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_settings
    ADD CONSTRAINT organization_settings_pkey PRIMARY KEY (id);


--
-- Name: organization_users organization_users_organization_id_admin_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_users
    ADD CONSTRAINT organization_users_organization_id_admin_id_key UNIQUE (organization_id, admin_id);


--
-- Name: organization_users organization_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_users
    ADD CONSTRAINT organization_users_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_slug_key UNIQUE (slug);


--
-- Name: overlay_history overlay_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.overlay_history
    ADD CONSTRAINT overlay_history_pkey PRIMARY KEY (id);


--
-- Name: overlay_states overlay_states_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.overlay_states
    ADD CONSTRAINT overlay_states_pkey PRIMARY KEY (stream_key);


--
-- Name: pending_signups pending_signups_checkout_session_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_signups
    ADD CONSTRAINT pending_signups_checkout_session_id_key UNIQUE (checkout_session_id);


--
-- Name: pending_signups pending_signups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_signups
    ADD CONSTRAINT pending_signups_pkey PRIMARY KEY (id);


--
-- Name: plan_alerts plan_alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plan_alerts
    ADD CONSTRAINT plan_alerts_pkey PRIMARY KEY (id);


--
-- Name: plans plans_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plans
    ADD CONSTRAINT plans_pkey PRIMARY KEY (id);


--
-- Name: plans plans_plan_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plans
    ADD CONSTRAINT plans_plan_key_key UNIQUE (plan_key);


--
-- Name: prayer_requests prayer_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prayer_requests
    ADD CONSTRAINT prayer_requests_pkey PRIMARY KEY (id);


--
-- Name: processed_whmcs_orders processed_whmcs_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.processed_whmcs_orders
    ADD CONSTRAINT processed_whmcs_orders_pkey PRIMARY KEY (whmcs_order_id);


--
-- Name: recordings recordings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recordings
    ADD CONSTRAINT recordings_pkey PRIMARY KEY (id);


--
-- Name: replay_saved_replays replay_saved_replays_member_id_recording_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_saved_replays
    ADD CONSTRAINT replay_saved_replays_member_id_recording_id_key UNIQUE (member_id, recording_id);


--
-- Name: replay_saved_replays replay_saved_replays_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_saved_replays
    ADD CONSTRAINT replay_saved_replays_pkey PRIMARY KEY (id);


--
-- Name: replay_session_events replay_session_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_session_events
    ADD CONSTRAINT replay_session_events_pkey PRIMARY KEY (id);


--
-- Name: replay_sessions replay_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_sessions
    ADD CONSTRAINT replay_sessions_pkey PRIMARY KEY (id);


--
-- Name: replay_sessions replay_sessions_session_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_sessions
    ADD CONSTRAINT replay_sessions_session_token_key UNIQUE (session_token);


--
-- Name: replay_viewer_members replay_viewer_members_organization_id_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_viewer_members
    ADD CONSTRAINT replay_viewer_members_organization_id_email_key UNIQUE (organization_id, email);


--
-- Name: replay_viewer_members replay_viewer_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_viewer_members
    ADD CONSTRAINT replay_viewer_members_pkey PRIMARY KEY (id);


--
-- Name: restart_audit_log restart_audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restart_audit_log
    ADD CONSTRAINT restart_audit_log_pkey PRIMARY KEY (id);


--
-- Name: scheduled_streams scheduled_streams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheduled_streams
    ADD CONSTRAINT scheduled_streams_pkey PRIMARY KEY (id);


--
-- Name: server_metrics_history server_metrics_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.server_metrics_history
    ADD CONSTRAINT server_metrics_history_pkey PRIMARY KEY (id);


--
-- Name: social_destinations social_destinations_channel_id_platform_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_destinations
    ADD CONSTRAINT social_destinations_channel_id_platform_key UNIQUE (channel_id, platform);


--
-- Name: social_destinations social_destinations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_destinations
    ADD CONSTRAINT social_destinations_pkey PRIMARY KEY (id);


--
-- Name: social_oauth_accounts social_oauth_accounts_organization_id_platform_external_acc_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_oauth_accounts
    ADD CONSTRAINT social_oauth_accounts_organization_id_platform_external_acc_key UNIQUE (organization_id, platform, external_account_id);


--
-- Name: social_oauth_accounts social_oauth_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_oauth_accounts
    ADD CONSTRAINT social_oauth_accounts_pkey PRIMARY KEY (id);


--
-- Name: stream_health_history stream_health_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_health_history
    ADD CONSTRAINT stream_health_history_pkey PRIMARY KEY (id);


--
-- Name: stream_target_health_history stream_target_health_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_target_health_history
    ADD CONSTRAINT stream_target_health_history_pkey PRIMARY KEY (id);


--
-- Name: streams streams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.streams
    ADD CONSTRAINT streams_pkey PRIMARY KEY (id);


--
-- Name: streams streams_stream_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.streams
    ADD CONSTRAINT streams_stream_key_key UNIQUE (stream_key);


--
-- Name: subscriptions subscriptions_organization_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscriptions
    ADD CONSTRAINT subscriptions_organization_id_key UNIQUE (organization_id);


--
-- Name: subscriptions subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscriptions
    ADD CONSTRAINT subscriptions_pkey PRIMARY KEY (id);


--
-- Name: viewer_reactions viewer_reactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_reactions
    ADD CONSTRAINT viewer_reactions_pkey PRIMARY KEY (id);


--
-- Name: viewer_sessions viewer_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_sessions
    ADD CONSTRAINT viewer_sessions_pkey PRIMARY KEY (id);


--
-- Name: viewer_sessions viewer_sessions_session_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_sessions
    ADD CONSTRAINT viewer_sessions_session_token_key UNIQUE (session_token);


--
-- Name: idx_api_keys_organization_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_api_keys_organization_id ON public.api_keys USING btree (organization_id);


--
-- Name: idx_channel_ha_events_channel_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_ha_events_channel_created ON public.channel_ha_events USING btree (channel_id, created_at DESC);


--
-- Name: idx_channel_ha_events_org_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_ha_events_org_created ON public.channel_ha_events USING btree (organization_id, created_at DESC);


--
-- Name: idx_channel_ha_events_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_ha_events_type ON public.channel_ha_events USING btree (event_type);


--
-- Name: idx_channel_media_node_migrations_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_media_node_migrations_channel ON public.channel_media_node_migrations USING btree (channel_id, created_at DESC);


--
-- Name: idx_channel_media_node_migrations_destination_node; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_media_node_migrations_destination_node ON public.channel_media_node_migrations USING btree (destination_media_node_id) WHERE (destination_media_node_id IS NOT NULL);


--
-- Name: idx_channel_media_node_migrations_one_active; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_channel_media_node_migrations_one_active ON public.channel_media_node_migrations USING btree (channel_id) WHERE ((status)::text = ANY ((ARRAY['requested'::character varying, 'validating'::character varying, 'prepared'::character varying, 'stopping_source'::character varying, 'ownership_committing'::character varying])::text[]));


--
-- Name: idx_channel_media_node_migrations_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_media_node_migrations_org ON public.channel_media_node_migrations USING btree (organization_id, created_at DESC);


--
-- Name: idx_channel_media_node_migrations_source_node; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_media_node_migrations_source_node ON public.channel_media_node_migrations USING btree (source_media_node_id) WHERE (source_media_node_id IS NOT NULL);


--
-- Name: idx_channel_pull_sources_channel_priority; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_pull_sources_channel_priority ON public.channel_pull_sources USING btree (channel_id, priority, id);


--
-- Name: idx_channel_pull_sources_one_active; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_channel_pull_sources_one_active ON public.channel_pull_sources USING btree (channel_id) WHERE (is_active_source = true);


--
-- Name: idx_channel_pull_sources_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_pull_sources_org ON public.channel_pull_sources USING btree (organization_id);


--
-- Name: idx_channel_source_failover_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_source_failover_org ON public.channel_source_failover USING btree (organization_id);


--
-- Name: idx_channels_media_node_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channels_media_node_id ON public.channels USING btree (media_node_id);


--
-- Name: idx_media_nodes_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_media_nodes_enabled ON public.media_nodes USING btree (is_enabled);


--
-- Name: idx_media_nodes_last_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_media_nodes_last_heartbeat ON public.media_nodes USING btree (last_heartbeat_at DESC);


--
-- Name: idx_media_nodes_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_media_nodes_status ON public.media_nodes USING btree (status);


--
-- Name: idx_recordings_archive_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_recordings_archive_status ON public.recordings USING btree (archive_status);


--
-- Name: idx_replay_saved_replays_member; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_saved_replays_member ON public.replay_saved_replays USING btree (member_id, created_at DESC);


--
-- Name: idx_replay_session_events_recording; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_session_events_recording ON public.replay_session_events USING btree (recording_id, created_at DESC);


--
-- Name: idx_replay_session_events_session; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_session_events_session ON public.replay_session_events USING btree (replay_session_id, created_at DESC);


--
-- Name: idx_replay_sessions_continue_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_continue_lookup ON public.replay_sessions USING btree (viewer_id, last_seen_at DESC);


--
-- Name: idx_replay_sessions_member; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_member ON public.replay_sessions USING btree (member_id, recording_id, last_seen_at DESC);


--
-- Name: idx_replay_sessions_organization_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_organization_id ON public.replay_sessions USING btree (organization_id);


--
-- Name: idx_replay_sessions_public_slug; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_public_slug ON public.replay_sessions USING btree (public_slug);


--
-- Name: idx_replay_sessions_recording_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_recording_id ON public.replay_sessions USING btree (recording_id);


--
-- Name: idx_replay_sessions_started_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_started_at ON public.replay_sessions USING btree (started_at);


--
-- Name: idx_replay_sessions_viewer_progress; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_sessions_viewer_progress ON public.replay_sessions USING btree (viewer_id, recording_id, last_seen_at DESC);


--
-- Name: idx_replay_viewer_members_org_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_replay_viewer_members_org_email ON public.replay_viewer_members USING btree (organization_id, email);


--
-- Name: idx_server_metrics_history_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_server_metrics_history_recorded_at ON public.server_metrics_history USING btree (recorded_at);


--
-- Name: idx_social_destinations_channel_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_social_destinations_channel_id ON public.social_destinations USING btree (channel_id);


--
-- Name: idx_social_destinations_oauth_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_social_destinations_oauth_account ON public.social_destinations USING btree (oauth_account_id);


--
-- Name: idx_social_oauth_accounts_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_social_oauth_accounts_org ON public.social_oauth_accounts USING btree (organization_id, platform);


--
-- Name: idx_stream_health_attention; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_health_attention ON public.stream_health_history USING btree (organization_id, attention_required, created_at DESC);


--
-- Name: idx_stream_health_broadcast; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_health_broadcast ON public.stream_health_history USING btree (channel_id, broadcast_started_at, created_at DESC);


--
-- Name: idx_stream_health_channel_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_health_channel_created ON public.stream_health_history USING btree (channel_id, created_at DESC);


--
-- Name: idx_stream_health_org_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_health_org_created ON public.stream_health_history USING btree (organization_id, created_at DESC);


--
-- Name: idx_stream_target_health_attention; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_target_health_attention ON public.stream_target_health_history USING btree (organization_id, requires_attention, created_at DESC);


--
-- Name: idx_stream_target_health_org_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_target_health_org_created ON public.stream_target_health_history USING btree (organization_id, created_at DESC);


--
-- Name: idx_stream_target_health_target_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_target_health_target_created ON public.stream_target_health_history USING btree (target_id, created_at DESC);


--
-- Name: idx_stream_targets_channel_autostart; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_targets_channel_autostart ON public.social_destinations USING btree (channel_id, auto_start);


--
-- Name: idx_stream_targets_channel_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_targets_channel_enabled ON public.social_destinations USING btree (channel_id, enabled);


--
-- Name: idx_stream_targets_stream_key_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stream_targets_stream_key_lookup ON public.social_destinations USING btree (stream_key_lookup_hash);


--
-- Name: idx_viewer_reactions_stream_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_viewer_reactions_stream_key ON public.viewer_reactions USING btree (stream_key);


--
-- Name: idx_viewer_sessions_geo_device; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_viewer_sessions_geo_device ON public.viewer_sessions USING btree (organization_id, country_code, device_type);


--
-- Name: idx_viewer_sessions_last_seen_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_viewer_sessions_last_seen_at ON public.viewer_sessions USING btree (last_seen_at);


--
-- Name: idx_viewer_sessions_organization_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_viewer_sessions_organization_id ON public.viewer_sessions USING btree (organization_id);


--
-- Name: idx_viewer_sessions_stream_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_viewer_sessions_stream_key ON public.viewer_sessions USING btree (stream_key);


--
-- Name: idx_viewer_sessions_tab_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_viewer_sessions_tab_lookup ON public.viewer_sessions USING btree (organization_id, stream_key, viewer_id, last_seen_at DESC);


--
-- Name: recordings_public_slug_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX recordings_public_slug_unique ON public.recordings USING btree (public_slug) WHERE (public_slug IS NOT NULL);


--
-- Name: api_keys api_keys_created_by_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_created_by_admin_id_fkey FOREIGN KEY (created_by_admin_id) REFERENCES public.admins(id) ON DELETE SET NULL;


--
-- Name: api_keys api_keys_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: channel_ha_events channel_ha_events_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events
    ADD CONSTRAINT channel_ha_events_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE;


--
-- Name: channel_ha_events channel_ha_events_new_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events
    ADD CONSTRAINT channel_ha_events_new_source_id_fkey FOREIGN KEY (new_source_id) REFERENCES public.channel_pull_sources(id) ON DELETE SET NULL;


--
-- Name: channel_ha_events channel_ha_events_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events
    ADD CONSTRAINT channel_ha_events_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: channel_ha_events channel_ha_events_previous_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events
    ADD CONSTRAINT channel_ha_events_previous_source_id_fkey FOREIGN KEY (previous_source_id) REFERENCES public.channel_pull_sources(id) ON DELETE SET NULL;


--
-- Name: channel_ha_events channel_ha_events_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ha_events
    ADD CONSTRAINT channel_ha_events_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.channel_pull_sources(id) ON DELETE SET NULL;


--
-- Name: channel_media_node_migrations channel_media_node_migrations_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations
    ADD CONSTRAINT channel_media_node_migrations_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE;


--
-- Name: channel_media_node_migrations channel_media_node_migrations_destination_media_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations
    ADD CONSTRAINT channel_media_node_migrations_destination_media_node_id_fkey FOREIGN KEY (destination_media_node_id) REFERENCES public.media_nodes(id) ON DELETE SET NULL;


--
-- Name: channel_media_node_migrations channel_media_node_migrations_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations
    ADD CONSTRAINT channel_media_node_migrations_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: channel_media_node_migrations channel_media_node_migrations_requested_by_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations
    ADD CONSTRAINT channel_media_node_migrations_requested_by_admin_id_fkey FOREIGN KEY (requested_by_admin_id) REFERENCES public.admins(id) ON DELETE SET NULL;


--
-- Name: channel_media_node_migrations channel_media_node_migrations_source_media_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_media_node_migrations
    ADD CONSTRAINT channel_media_node_migrations_source_media_node_id_fkey FOREIGN KEY (source_media_node_id) REFERENCES public.media_nodes(id) ON DELETE SET NULL;


--
-- Name: channel_pull_sources channel_pull_sources_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_pull_sources
    ADD CONSTRAINT channel_pull_sources_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE;


--
-- Name: channel_pull_sources channel_pull_sources_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_pull_sources
    ADD CONSTRAINT channel_pull_sources_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: channel_source_failover channel_source_failover_active_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_source_failover
    ADD CONSTRAINT channel_source_failover_active_source_id_fkey FOREIGN KEY (active_source_id) REFERENCES public.channel_pull_sources(id) ON DELETE SET NULL;


--
-- Name: channel_source_failover channel_source_failover_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_source_failover
    ADD CONSTRAINT channel_source_failover_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE;


--
-- Name: channel_source_failover channel_source_failover_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_source_failover
    ADD CONSTRAINT channel_source_failover_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: channels channels_media_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_media_node_id_fkey FOREIGN KEY (media_node_id) REFERENCES public.media_nodes(id) ON DELETE SET NULL;


--
-- Name: channels channels_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: cta_links cta_links_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cta_links
    ADD CONSTRAINT cta_links_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: live_chat_messages live_chat_messages_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.live_chat_messages
    ADD CONSTRAINT live_chat_messages_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: notification_preferences notification_preferences_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.notification_preferences
    ADD CONSTRAINT notification_preferences_admin_id_fkey FOREIGN KEY (admin_id) REFERENCES public.admins(id) ON DELETE CASCADE;


--
-- Name: organization_settings organization_settings_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_settings
    ADD CONSTRAINT organization_settings_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: organization_users organization_users_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_users
    ADD CONSTRAINT organization_users_admin_id_fkey FOREIGN KEY (admin_id) REFERENCES public.admins(id) ON DELETE CASCADE;


--
-- Name: organization_users organization_users_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_users
    ADD CONSTRAINT organization_users_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: overlay_history overlay_history_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.overlay_history
    ADD CONSTRAINT overlay_history_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: overlay_states overlay_states_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.overlay_states
    ADD CONSTRAINT overlay_states_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: pending_signups pending_signups_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_signups
    ADD CONSTRAINT pending_signups_admin_id_fkey FOREIGN KEY (admin_id) REFERENCES public.admins(id) ON DELETE SET NULL;


--
-- Name: pending_signups pending_signups_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_signups
    ADD CONSTRAINT pending_signups_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE SET NULL;


--
-- Name: pending_signups pending_signups_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_signups
    ADD CONSTRAINT pending_signups_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE SET NULL;


--
-- Name: plan_alerts plan_alerts_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plan_alerts
    ADD CONSTRAINT plan_alerts_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE SET NULL;


--
-- Name: plan_alerts plan_alerts_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plan_alerts
    ADD CONSTRAINT plan_alerts_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: prayer_requests prayer_requests_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prayer_requests
    ADD CONSTRAINT prayer_requests_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: processed_whmcs_orders processed_whmcs_orders_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.processed_whmcs_orders
    ADD CONSTRAINT processed_whmcs_orders_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE SET NULL;


--
-- Name: recordings recordings_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recordings
    ADD CONSTRAINT recordings_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: replay_saved_replays replay_saved_replays_member_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_saved_replays
    ADD CONSTRAINT replay_saved_replays_member_id_fkey FOREIGN KEY (member_id) REFERENCES public.replay_viewer_members(id) ON DELETE CASCADE;


--
-- Name: replay_saved_replays replay_saved_replays_recording_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_saved_replays
    ADD CONSTRAINT replay_saved_replays_recording_id_fkey FOREIGN KEY (recording_id) REFERENCES public.recordings(id) ON DELETE CASCADE;


--
-- Name: replay_session_events replay_session_events_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_session_events
    ADD CONSTRAINT replay_session_events_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE SET NULL;


--
-- Name: replay_session_events replay_session_events_recording_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_session_events
    ADD CONSTRAINT replay_session_events_recording_id_fkey FOREIGN KEY (recording_id) REFERENCES public.recordings(id) ON DELETE CASCADE;


--
-- Name: replay_session_events replay_session_events_replay_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_session_events
    ADD CONSTRAINT replay_session_events_replay_session_id_fkey FOREIGN KEY (replay_session_id) REFERENCES public.replay_sessions(id) ON DELETE CASCADE;


--
-- Name: replay_sessions replay_sessions_member_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_sessions
    ADD CONSTRAINT replay_sessions_member_id_fkey FOREIGN KEY (member_id) REFERENCES public.replay_viewer_members(id) ON DELETE SET NULL;


--
-- Name: replay_sessions replay_sessions_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_sessions
    ADD CONSTRAINT replay_sessions_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE SET NULL;


--
-- Name: replay_sessions replay_sessions_recording_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_sessions
    ADD CONSTRAINT replay_sessions_recording_id_fkey FOREIGN KEY (recording_id) REFERENCES public.recordings(id) ON DELETE CASCADE;


--
-- Name: replay_viewer_members replay_viewer_members_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replay_viewer_members
    ADD CONSTRAINT replay_viewer_members_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: restart_audit_log restart_audit_log_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restart_audit_log
    ADD CONSTRAINT restart_audit_log_admin_id_fkey FOREIGN KEY (admin_id) REFERENCES public.admins(id) ON DELETE SET NULL;


--
-- Name: scheduled_streams scheduled_streams_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheduled_streams
    ADD CONSTRAINT scheduled_streams_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: social_destinations social_destinations_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_destinations
    ADD CONSTRAINT social_destinations_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE;


--
-- Name: social_destinations social_destinations_oauth_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_destinations
    ADD CONSTRAINT social_destinations_oauth_account_id_fkey FOREIGN KEY (oauth_account_id) REFERENCES public.social_oauth_accounts(id) ON DELETE SET NULL;


--
-- Name: social_oauth_accounts social_oauth_accounts_connected_by_admin_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_oauth_accounts
    ADD CONSTRAINT social_oauth_accounts_connected_by_admin_id_fkey FOREIGN KEY (connected_by_admin_id) REFERENCES public.admins(id) ON DELETE SET NULL;


--
-- Name: social_oauth_accounts social_oauth_accounts_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.social_oauth_accounts
    ADD CONSTRAINT social_oauth_accounts_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: stream_health_history stream_health_history_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_health_history
    ADD CONSTRAINT stream_health_history_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE;


--
-- Name: stream_health_history stream_health_history_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_health_history
    ADD CONSTRAINT stream_health_history_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: stream_target_health_history stream_target_health_history_target_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stream_target_health_history
    ADD CONSTRAINT stream_target_health_history_target_id_fkey FOREIGN KEY (target_id) REFERENCES public.social_destinations(id) ON DELETE CASCADE;


--
-- Name: subscriptions subscriptions_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscriptions
    ADD CONSTRAINT subscriptions_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE CASCADE;


--
-- Name: viewer_reactions viewer_reactions_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_reactions
    ADD CONSTRAINT viewer_reactions_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: viewer_sessions viewer_sessions_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_sessions
    ADD CONSTRAINT viewer_sessions_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE SET NULL;


--
-- PostgreSQL database dump complete
--


