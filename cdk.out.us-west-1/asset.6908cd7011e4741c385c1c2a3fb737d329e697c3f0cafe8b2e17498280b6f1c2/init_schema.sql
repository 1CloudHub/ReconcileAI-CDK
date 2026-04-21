-- erp_no_sap.document_table definition

-- Drop table

-- DROP TABLE erp_no_sap.document_table;

CREATE TABLE erp_no_sap.document_table ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, document_name text DEFAULT ''::text NULL, needed_fields jsonb DEFAULT '{}'::jsonb NULL, document_description text NULL, created_at timestamptz DEFAULT now() NULL, document_key text DEFAULT ''::text NULL, document_key_is_enabled bool DEFAULT false NULL, CONSTRAINT document_pkey PRIMARY KEY (id));

-- Permissions

ALTER TABLE erp_no_sap.document_table OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.document_table TO postgres;


-- erp_no_sap.job_table definition

-- Drop table

-- DROP TABLE erp_no_sap.job_table;

CREATE TABLE erp_no_sap.job_table ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, job_name text DEFAULT ''::text NULL, document_id jsonb DEFAULT '[]'::jsonb NULL, created_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'::text) NULL, created_by text NULL, updated_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'::text) NULL, updated_by text NULL, delete_status bool DEFAULT false NULL, reference_document_id int8 NULL, CONSTRAINT job_pkey PRIMARY KEY (id));

-- Permissions

ALTER TABLE erp_no_sap.job_table OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.job_table TO postgres;


-- erp_no_sap.metadata_table definition

-- Drop table

-- DROP TABLE erp_no_sap.metadata_table;

CREATE TABLE erp_no_sap.metadata_table ( id bigserial NOT NULL, key_id text NULL, "type" text NULL, meta_key text NULL, meta_value text NULL, CONSTRAINT metadata_table_pkey PRIMARY KEY (id), CONSTRAINT uq_metadata_key_type_metakey UNIQUE (key_id, type, meta_key));

-- Table Triggers

create trigger trg_update_total_tokens_used after
update
    of meta_value on
    erp_no_sap.metadata_table for each row execute function erp_no_sap.update_total_tokens_used();

-- Permissions

ALTER TABLE erp_no_sap.metadata_table OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.metadata_table TO postgres;


-- erp_no_sap.prompt definition

-- Drop table

-- DROP TABLE erp_no_sap.prompt;

CREATE TABLE erp_no_sap.prompt ();

-- Permissions

ALTER TABLE erp_no_sap.prompt OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.prompt TO postgres;


-- erp_no_sap.session_table definition

-- Drop table

-- DROP TABLE erp_no_sap.session_table;

CREATE TABLE erp_no_sap.session_table ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, session_id text NULL, job_id int8 NULL, reconcile_status text NULL, reason_for_failure jsonb DEFAULT '{}'::jsonb NULL, created_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'::text) NULL, created_by text NULL, updated_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'::text) NULL, documents jsonb NULL, extracted_fields jsonb DEFAULT '[]'::jsonb NULL, input_tokens_used int8 DEFAULT 0 NULL, output_tokens_used int8 DEFAULT 0 NULL, CONSTRAINT session_pkey PRIMARY KEY (id), CONSTRAINT session_table_session_id_unique UNIQUE (session_id));

-- Permissions

ALTER TABLE erp_no_sap.session_table OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.session_table TO postgres;


-- erp_no_sap.session_table_test definition

-- Drop table

-- DROP TABLE erp_no_sap.session_table_test;

CREATE TABLE erp_no_sap.session_table_test ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, session_id text NULL, job_id int8 NULL, reconcile_status text NULL, reason_for_failure jsonb DEFAULT '{}'::jsonb NULL, created_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'::text) NULL, created_by text NULL, updated_at timestamptz DEFAULT (now() AT TIME ZONE 'Asia/Kolkata'::text) NULL, documents jsonb NULL, extracted_fields jsonb DEFAULT '[]'::jsonb NULL, is_delete bool NULL, input_tokens_used int8 DEFAULT 0 NULL, output_tokens_used int8 DEFAULT 0 NULL, CONSTRAINT session_pkey_test PRIMARY KEY (id), CONSTRAINT session_table_session_id_unique_test UNIQUE (session_id));

-- Permissions

ALTER TABLE erp_no_sap.session_table_test OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.session_table_test TO postgres;


-- erp_no_sap.sop_table definition

-- Drop table

-- DROP TABLE erp_no_sap.sop_table;

CREATE TABLE erp_no_sap.sop_table ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, exception_id text NULL, exception_name text NULL, created_at timestamptz DEFAULT timezone('Asia/Kolkata'::text, now()) NULL, created_by text NULL, updated_at timestamptz DEFAULT timezone('Asia/Kolkata'::text, now()) NULL, updated_by text NULL, job_id jsonb DEFAULT '[]'::jsonb NULL, delete_status bool DEFAULT false NULL, s3_path jsonb NULL, CONSTRAINT sop_pkey PRIMARY KEY (id));

-- Permissions

ALTER TABLE erp_no_sap.sop_table OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.sop_table TO postgres;


-- erp_no_sap.sop_table_test definition

-- Drop table

-- DROP TABLE erp_no_sap.sop_table_test;

CREATE TABLE erp_no_sap.sop_table_test ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, exception_id text NULL, exception_name text NULL, created_at timestamptz DEFAULT timezone('Asia/Kolkata'::text, now()) NULL, created_by text NULL, updated_at timestamptz DEFAULT timezone('Asia/Kolkata'::text, now()) NULL, updated_by text NULL, job_id jsonb DEFAULT '[]'::jsonb NULL, delete_status bool DEFAULT false NULL, s3_path jsonb NULL, CONSTRAINT sop_pkey_test PRIMARY KEY (id));

-- Permissions

ALTER TABLE erp_no_sap.sop_table_test OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.sop_table_test TO postgres;


-- erp_no_sap.token_log_table definition

-- Drop table

-- DROP TABLE erp_no_sap.token_log_table;

CREATE TABLE erp_no_sap.token_log_table ( id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, "module" text NULL, "type" text NULL, ref_table text NULL, ref_id int8 NULL, document_name text NULL, input_tokens int8 DEFAULT 0 NULL, output_tokens int8 DEFAULT 0 NULL, total_tokens int8 GENERATED ALWAYS AS (input_tokens + output_tokens) STORED NULL, created_by text NULL, created_at timestamptz DEFAULT timezone('Asia/Kolkata'::text, now()) NULL, CONSTRAINT token_log_table_pkey PRIMARY KEY (id));

-- Permissions

ALTER TABLE erp_no_sap.token_log_table OWNER TO postgres;
GRANT ALL ON TABLE erp_no_sap.token_log_table TO postgres;