-- =====================================================
-- 1. SCHEMA
-- =====================================================
CREATE SCHEMA IF NOT EXISTS erp_no_sap;
SET search_path TO erp_no_sap;

-- =====================================================
-- 2. FUNCTION (MUST COME BEFORE TRIGGER)
-- =====================================================
CREATE OR REPLACE FUNCTION erp_no_sap.update_total_tokens_used()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    input_tokens  BIGINT;
    output_tokens BIGINT;
BEGIN
    IF NEW.meta_key NOT IN ('total_input_token_used', 'total_output_token_used') THEN
        RETURN NEW;
    END IF;

    IF NEW.meta_key = 'total_input_token_used' THEN
        input_tokens  := COALESCE(NULLIF(TRIM(NEW.meta_value), '')::BIGINT, 0);
        SELECT COALESCE(NULLIF(TRIM(meta_value), '')::BIGINT, 0)
        INTO output_tokens
        FROM erp_no_sap.metadata_table
        WHERE key_id = 'application'
          AND meta_key = 'total_output_token_used';
    ELSE
        output_tokens := COALESCE(NULLIF(TRIM(NEW.meta_value), '')::BIGINT, 0);
        SELECT COALESCE(NULLIF(TRIM(meta_value), '')::BIGINT, 0)
        INTO input_tokens
        FROM erp_no_sap.metadata_table
        WHERE key_id = 'application'
          AND meta_key = 'total_input_token_used';
    END IF;

    UPDATE erp_no_sap.metadata_table
    SET meta_value = (input_tokens + output_tokens)::TEXT
    WHERE key_id = 'application'
      AND meta_key = 'total_token_used';

    RETURN NEW;
END;
$function$;

-- =====================================================
-- 3. TABLES (IDEMPOTENT)
-- =====================================================

CREATE TABLE IF NOT EXISTS erp_no_sap.document_table (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_name text DEFAULT '',
    needed_fields jsonb DEFAULT '{}'::jsonb,
    document_description text,
    created_at timestamptz DEFAULT now(),
    document_key text DEFAULT '',
    document_key_is_enabled bool DEFAULT false
);

CREATE TABLE IF NOT EXISTS erp_no_sap.job_table (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name text DEFAULT '',
    document_id jsonb DEFAULT '[]'::jsonb,
    created_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'),
    created_by text,
    updated_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'),
    updated_by text,
    delete_status bool DEFAULT false,
    reference_document_id int8
);

CREATE TABLE IF NOT EXISTS erp_no_sap.metadata_table (
    id bigserial PRIMARY KEY,
    key_id text,
    "type" text,
    meta_key text,
    meta_value text,
    CONSTRAINT uq_metadata_key_type_metakey UNIQUE (key_id, type, meta_key)
);

CREATE TABLE IF NOT EXISTS erp_no_sap.prompt ();

CREATE TABLE IF NOT EXISTS erp_no_sap.session_table (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id text UNIQUE,
    job_id int8,
    reconcile_status text,
    reason_for_failure jsonb DEFAULT '{}'::jsonb,
    created_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'),
    created_by text,
    updated_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'),
    documents jsonb,
    extracted_fields jsonb DEFAULT '[]'::jsonb,
    input_tokens_used int8 DEFAULT 0,
    output_tokens_used int8 DEFAULT 0
);

CREATE TABLE IF NOT EXISTS erp_no_sap.session_table_test (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id text UNIQUE,
    job_id int8,
    reconcile_status text,
    reason_for_failure jsonb DEFAULT '{}'::jsonb,
    created_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'),
    created_by text,
    updated_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'),
    documents jsonb,
    extracted_fields jsonb DEFAULT '[]'::jsonb,
    is_delete bool,
    input_tokens_used int8 DEFAULT 0,
    output_tokens_used int8 DEFAULT 0
);

CREATE TABLE IF NOT EXISTS erp_no_sap.sop_table (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exception_id text,
    exception_name text,
    created_at timestamptz DEFAULT timezone('Asia/Kolkata', now()),
    created_by text,
    updated_at timestamptz DEFAULT timezone('Asia/Kolkata', now()),
    updated_by text,
    job_id jsonb DEFAULT '[]'::jsonb,
    delete_status bool DEFAULT false,
    s3_path jsonb
);

CREATE TABLE IF NOT EXISTS erp_no_sap.sop_table_test (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exception_id text,
    exception_name text,
    created_at timestamptz DEFAULT timezone('Asia/Kolkata', now()),
    created_by text,
    updated_at timestamptz DEFAULT timezone('Asia/Kolkata', now()),
    updated_by text,
    job_id jsonb DEFAULT '[]'::jsonb,
    delete_status bool DEFAULT false,
    s3_path jsonb
);

CREATE TABLE IF NOT EXISTS erp_no_sap.token_log_table (
    id int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "module" text,
    "type" text,
    ref_table text,
    ref_id int8,
    document_name text,
    input_tokens int8 DEFAULT 0,
    output_tokens int8 DEFAULT 0,
    total_tokens int8 GENERATED ALWAYS AS (input_tokens + output_tokens) STORED,
    created_by text,
    created_at timestamptz DEFAULT timezone('Asia/Kolkata', now())
);

-- =====================================================
-- 4. SEED REQUIRED METADATA (CRITICAL)
-- =====================================================
INSERT INTO erp_no_sap.metadata_table (key_id, meta_key, meta_value)
VALUES
('application', 'total_input_token_used', '0'),
('application', 'total_output_token_used', '0'),
('application', 'total_token_used', '0')
ON CONFLICT DO NOTHING;

-- =====================================================
-- 5. TRIGGER (IDEMPOTENT)
-- =====================================================
DROP TRIGGER IF EXISTS trg_update_total_tokens_used ON erp_no_sap.metadata_table;

CREATE TRIGGER trg_update_total_tokens_used
AFTER UPDATE OF meta_value
ON erp_no_sap.metadata_table
FOR EACH ROW
EXECUTE FUNCTION erp_no_sap.update_total_tokens_used();

-- =====================================================
-- 6. PERMISSIONS
-- =====================================================
ALTER TABLE erp_no_sap.document_table OWNER TO postgres;
ALTER TABLE erp_no_sap.job_table OWNER TO postgres;
ALTER TABLE erp_no_sap.metadata_table OWNER TO postgres;
ALTER TABLE erp_no_sap.prompt OWNER TO postgres;
ALTER TABLE erp_no_sap.session_table OWNER TO postgres;
ALTER TABLE erp_no_sap.session_table_test OWNER TO postgres;
ALTER TABLE erp_no_sap.sop_table OWNER TO postgres;
ALTER TABLE erp_no_sap.sop_table_test OWNER TO postgres;
ALTER TABLE erp_no_sap.token_log_table OWNER TO postgres;

GRANT ALL ON ALL TABLES IN SCHEMA erp_no_sap TO postgres;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA erp_no_sap TO postgres;