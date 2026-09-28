-- =============================================================================
-- Watheeq Insurance — Supabase migration (single file, idempotent-ish)
-- Canonical spec: SPEC.md §1 (shared conventions) + §3 (Watheeq).
--
-- RUN ORDER
--   1. This file — Supabase SQL editor (self-owned project) or, locally,
--      `psql -f supabase/migration.sql` after harness/supabase_stubs.sql.
--   2. python scripts/seed_baseline.py      (shared tables + *_backup tables)
--   3. python scripts/generate_assets.py && python scripts/upload_assets.py
--   4. Create auth users (portal sign-up with raw_user_meta_data
--      {whatsapp_number:'+9665…', full_name:'…'}). The AFTER INSERT trigger on
--      auth.users upserts demo_users and clones the baseline for that user.
--      Users created BEFORE step 2 got an empty clone: fix with
--      `python scripts/seed_baseline.py --reclone-owner <uuid> --wipe`.
--   5. Fallback tenant: sign up demo-fallback+watheeq@nebelus.ai with a throwaway
--      number; its uuid -> API env DEFAULT_OWNER_ID.
--
-- WHAT IS HERE
--   shared (read-only reference): demo_meta, health_classes, providers,
--     provider_doctors, service_centres, plans, addons, business_rules, demo_assets
--   per-tenant (owner_id + composite PK/FKs): customers, dependents, vehicles,
--     policies, health_members, najm_reports, claims, valuation_disputes,
--     preauths, bookings, quotes, payment_requests, refunds, complaints,
--     agent_actions (never cloned)
--   *_backup for every per-tenant table except agent_actions (no owner_id)
--   demo_users, clone_baseline_for_user(), reset_demo_data(),
--   reset_demo_data_for() (service role), on-signup trigger, RLS, realtime,
--   storage bucket `documents`.
--
-- Re-running: tables use IF NOT EXISTS; functions/policies/triggers are replaced.
-- A structural change to an existing table needs an explicit ALTER.
-- =============================================================================

set client_min_messages = warning;
create schema if not exists extensions;
create extension if not exists pgcrypto with schema extensions;
set search_path = public, extensions;

-- -----------------------------------------------------------------------------
-- Helpers
-- -----------------------------------------------------------------------------

-- 24-char URL-safe random token for payment links (18 random bytes -> base64).
create or replace function public.demo_new_pay_token()
returns text language sql volatile
set search_path = public, extensions, pg_temp
as $$ select translate(encode(gen_random_bytes(18), 'base64'), '+/', '-_') $$;

-- Shift every ISO date ('YYYY-MM-DD') and ISO timestamp string inside a jsonb
-- value by p_days. Used by the clone so dates embedded in jsonb (travel
-- trip_start/trip_end, insurance_sync.synced_at, document received_at, driver
-- licence dates…) stay coherent with the shifted columns.
create or replace function public.demo_shift_jsonb(p jsonb, p_days int)
returns jsonb language plpgsql immutable
set timezone = 'Asia/Riyadh'
as $$
declare r jsonb; s text;
begin
  if p is null or p_days = 0 then return p; end if;
  case jsonb_typeof(p)
    when 'object' then
      select coalesce(jsonb_object_agg(e.key, public.demo_shift_jsonb(e.value, p_days)), '{}'::jsonb)
        into r from jsonb_each(p) e;
      return r;
    when 'array' then
      select coalesce(jsonb_agg(public.demo_shift_jsonb(e.value, p_days) order by e.ord), '[]'::jsonb)
        into r from jsonb_array_elements(p) with ordinality e(value, ord);
      return r;
    when 'string' then
      s := p #>> '{}';
      if s ~ '^\d{4}-\d{2}-\d{2}$' then
        return to_jsonb(to_char(s::date + p_days, 'YYYY-MM-DD'));
      elsif s ~ '^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}(:?\d{2})?)$' then
        return to_jsonb(to_char(s::timestamptz + p_days * interval '1 day', 'YYYY-MM-DD"T"HH24:MI:SS') || '+03:00');
      end if;
      return p;
    else
      return p;
  end case;
end $$;

-- =============================================================================
-- SHARED TABLES (no owner_id, single-column PK)
-- =============================================================================

create table if not exists public.demo_meta (
  key   text primary key,
  value text not null
);

create table if not exists public.health_classes (
  class_code                           text primary key check (class_code in ('VIP','A','B','C')),
  name_en                              text not null,
  name_ar                              text not null,
  annual_limit_sar                     numeric(12,2) not null,
  dental_limit_sar                     numeric(12,2) not null,
  optical_limit_sar                    numeric(12,2) not null,
  maternity_limit_sar                  numeric(12,2) not null,
  out_of_network_reimbursement_percent int not null check (out_of_network_reimbursement_percent between 0 and 100),
  copay_per_visit_sar                  numeric(10,2) not null default 0,
  annual_premium_per_member_sar        numeric(12,2) not null,
  network_tier                         int not null check (network_tier between 1 and 4)
);

create table if not exists public.providers (
  provider_id      text primary key check (provider_id ~ '^PRV-[0-9]{3}$'),
  name_en          text not null,
  name_ar          text not null,
  type             text not null check (type in ('Hospital','Clinic','Dental Clinic')),
  city_en          text not null,
  city_ar          text not null,
  district_en      text,
  lat              numeric(9,6) not null,
  lng              numeric(9,6) not null,
  min_network_tier int not null check (min_network_tier between 1 and 4),
  specialties      jsonb not null default '[]'::jsonb,
  phone            text,
  address_en       text,
  address_ar       text
);

create table if not exists public.provider_doctors (
  doctor_id    text primary key check (doctor_id ~ '^PDR-[0-9]{3}$'),
  provider_id  text not null references public.providers(provider_id),
  name_en      text not null,
  name_ar      text not null,
  specialty_en text not null,
  specialty_ar text not null
);
create index if not exists provider_doctors_provider_idx on public.provider_doctors (provider_id);

create table if not exists public.service_centres (
  centre_id    text primary key check (centre_id ~ '^SVC-[0-9]{3}$'),
  type         text not null check (type in ('Inspection Centre','Approved Workshop','Agency Workshop','Rental Branch','Home Inspector Team')),
  name_en      text not null,
  name_ar      text not null,
  city_en      text not null,
  lat          numeric(9,6) not null,
  lng          numeric(9,6) not null,
  address_en   text,
  address_ar   text,
  phone        text,
  hours_en     text,
  hours_ar     text,
  partner_name text
);

create table if not exists public.plans (
  plan_code        text primary key,
  product          text not null check (product in ('Motor','Health','Travel','Home')),
  name_en          text not null,
  name_ar          text not null,
  cover_summary_en text,
  cover_summary_ar text,
  details          jsonb not null default '{}'::jsonb
);

create table if not exists public.addons (
  addon_code             text primary key check (addon_code in ('AGENCY_REPAIR','REPLACEMENT_CAR','ADDITIONAL_DRIVER')),
  name_en                text not null,
  name_ar                text not null,
  annual_price_sar       numeric(10,2) not null,
  limit_text_en          text,
  limit_text_ar          text,
  max_vehicle_age_years  int,
  per_accident_limit_sar numeric(10,2)
);

create table if not exists public.business_rules (
  rule_key       text primary key,
  value_num      numeric,
  value_text     text,
  unit           text,
  description_en text,
  description_ar text,
  check (value_num is not null or value_text is not null)
);

create table if not exists public.demo_assets (
  asset_id       text primary key,
  persona_id     text,
  use_case       text,
  title_en       text not null,
  title_ar       text,
  storage_path   text not null unique,
  content_type   text not null,
  description_en text
);

-- =============================================================================
-- demo_users (§1.3)
-- =============================================================================
create table if not exists public.demo_users (
  owner_id        uuid primary key references auth.users(id) on delete cascade,
  email           text,
  whatsapp_number text not null unique check (whatsapp_number ~ '^\+[1-9][0-9]{7,14}$'),
  full_name       text,
  created_at      timestamptz not null default now()
);

-- =============================================================================
-- PER-TENANT TABLES (owner_id, composite PK (owner_id, business_id))
-- =============================================================================

create table if not exists public.customers (
  owner_id          uuid not null references auth.users(id) on delete cascade,
  customer_id       text not null check (customer_id ~ '^WTQ-C-[0-9]{4}$'),
  national_id       text not null check (national_id ~ '^[12][0-9]{9}$'),
  national_id_last4 text not null check (national_id_last4 ~ '^[0-9]{4}$'),
  full_name_en      text not null,
  full_name_ar      text not null,
  dob               date,
  phone             text not null,
  email             text,
  preferred_language text not null default 'Arabic' check (preferred_language in ('Arabic','English')),
  city_en           text,
  city_ar           text,
  district_en       text,
  district_ar       text,
  address_en        text,
  address_ar        text,
  home_lat          numeric(9,6),
  home_lng          numeric(9,6),
  iban_masked       text,
  iban_holder_name  text,
  ncd_percent       int not null default 0 check (ncd_percent between 0 and 60),
  status            text not null default 'Active' check (status in ('Active','Inactive','Suspended')),
  registered_since  date,
  demo_notes        text,
  primary key (owner_id, customer_id)
);
create index if not exists customers_phone_idx       on public.customers (owner_id, phone);
create index if not exists customers_national_id_idx on public.customers (owner_id, national_id);

create table if not exists public.dependents (
  owner_id                 uuid not null references auth.users(id) on delete cascade,
  dependent_id             text not null check (dependent_id ~ '^DEP-[0-9]{5}$'),
  customer_id              text not null,
  full_name_en             text not null,
  full_name_ar             text not null,
  relationship             text not null check (relationship in ('Spouse','Son','Daughter','Parent','Sibling')),
  dob                      date,
  national_id_or_birth_cert text,
  gender                   text check (gender in ('Male','Female')),
  primary key (owner_id, dependent_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id)
);
create index if not exists dependents_customer_idx on public.dependents (owner_id, customer_id);

create table if not exists public.vehicles (
  owner_id                     uuid not null references auth.users(id) on delete cascade,
  vehicle_id                   text not null check (vehicle_id ~ '^VEH-[0-9]{5}$'),
  owner_customer_id            text,
  owner_name_en                text,
  sequence_number              text not null check (sequence_number ~ '^[0-9]{9}$'),
  plate_en                     text not null,
  plate_ar                     text,
  make_en                      text not null,
  make_ar                      text,
  model_en                     text not null,
  model_ar                     text,
  year                         int not null check (year between 1980 and 2100),
  color_en                     text,
  color_ar                     text,
  market_value_sar             numeric(12,2) not null check (market_value_sar >= 0),
  category                     text not null check (category in ('Sedan','SUV','Large SUV','Pickup')),
  tpl_annual_sar               numeric(10,2) not null check (tpl_annual_sar >= 0),
  comp_rate_percent            numeric(5,2) not null check (comp_rate_percent >= 0),
  registration_expiry          date,
  inspection_valid_until       date,
  open_traffic_fines_sar       numeric(10,2) not null default 0,
  insurance_status             text not null default 'Not Insured' check (insurance_status in ('Insured','Not Insured','Expired')),
  insurance_sync               jsonb not null default '{}'::jsonb,
  transfer_status              text not null default 'None' check (transfer_status in ('None','Transferred','Pending Transfer')),
  transferred_on               date,
  registration_renewal_blocked boolean not null default false,
  block_reason_en              text,
  block_reason_ar              text,
  primary key (owner_id, vehicle_id),
  unique (owner_id, sequence_number),
  foreign key (owner_id, owner_customer_id) references public.customers (owner_id, customer_id)
);
create index if not exists vehicles_customer_idx on public.vehicles (owner_id, owner_customer_id);

create table if not exists public.policies (
  owner_id         uuid not null references auth.users(id) on delete cascade,
  policy_id        text not null check (policy_id ~ '^POL-(MTR|HLT|TRV|HOM)-[0-9]{5}$'),
  customer_id      text not null,
  product          text not null check (product in ('Motor','Health','Travel','Home')),
  plan_code        text not null references public.plans(plan_code),
  status           text not null check (status in ('Active','Expired','Cancelled','Pending Payment')),
  start_date       date not null,
  end_date         date not null,
  premium_paid_sar numeric(12,2) not null default 0,
  cover            jsonb not null default '{}'::jsonb,
  primary key (owner_id, policy_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id),
  check (end_date >= start_date)
);
create index if not exists policies_customer_idx on public.policies (owner_id, customer_id);
create index if not exists policies_status_idx   on public.policies (owner_id, status);

create table if not exists public.health_members (
  owner_id             uuid not null references auth.users(id) on delete cascade,
  member_id            text not null check (member_id ~ '^MBR-[0-9]{5}$'),
  policy_id            text not null,
  customer_id          text not null,
  dependent_id         text,
  full_name_en         text not null,
  full_name_ar         text not null,
  relationship         text not null check (relationship in ('Principal','Spouse','Son','Daughter','Parent','Sibling')),
  dob                  date,
  card_number          text not null check (card_number ~ '^WTQ-HC-[0-9]{8}$'),
  class_code           text not null references public.health_classes(class_code),
  limit_used_sar       numeric(12,2) not null default 0 check (limit_used_sar >= 0),
  dental_used_sar      numeric(12,2) not null default 0 check (dental_used_sar >= 0),
  optical_used_sar     numeric(12,2) not null default 0 check (optical_used_sar >= 0),
  cover_start          date,
  waiting_period_until date,
  status               text not null default 'Active' check (status in ('Active','Suspended','Terminated')),
  primary key (owner_id, member_id),
  foreign key (owner_id, policy_id)    references public.policies (owner_id, policy_id),
  foreign key (owner_id, customer_id)  references public.customers (owner_id, customer_id),
  foreign key (owner_id, dependent_id) references public.dependents (owner_id, dependent_id)
);
create index if not exists health_members_policy_idx   on public.health_members (owner_id, policy_id);
create index if not exists health_members_customer_idx on public.health_members (owner_id, customer_id);

create table if not exists public.najm_reports (
  owner_id              uuid not null references auth.users(id) on delete cascade,
  report_number         text not null check (report_number ~ '^NJM-[0-9]{7}$'),
  accident_at           timestamptz not null,
  location_en           text,
  location_ar           text,
  city_en               text,
  parties               jsonb not null default '[]'::jsonb,
  damage_en             text,
  damage_ar             text,
  estimated_damage_sar  numeric(12,2),
  injuries              boolean not null default false,
  report_type           text not null check (report_type in ('Self Report','Officer')),
  primary key (owner_id, report_number)
);

create table if not exists public.claims (
  owner_id               uuid not null references auth.users(id) on delete cascade,
  claim_id               text not null check (claim_id ~ '^(CLM-[0-9]{5}|TPC-[0-9]{4}|RMB-[0-9]{4}|TRV-[0-9]{4}|PRP-[0-9]{4})$'),
  claim_type             text not null check (claim_type in ('Motor','Third Party','Health Reimbursement','Travel','Home')),
  customer_id            text,
  policy_id              text,
  vehicle_id             text,
  najm_report_number     text,
  status                 text not null check (status in ('Open','Awaiting Documents','Inspection Booked','Under Review','Approved','Paid','Rejected','Total Loss — Valuation','Second Valuation','Closed')),
  opened_at              timestamptz not null default now(),
  documents_completed_at timestamptz,
  payment_due_by         date,
  description_en         text,
  description_ar         text,
  fault_percent          int check (fault_percent between 0 and 100),
  customer_pays_sar      numeric(12,2) not null default 0,
  approved_amount_sar    numeric(12,2),
  missing_documents      jsonb not null default '[]'::jsonb,
  documents              jsonb not null default '[]'::jsonb,
  settlement_preference  text check (settlement_preference in ('Repair','Cash')),
  iban_masked            text,
  claimant               jsonb,
  valuation              jsonb,
  rejection_reason_en    text,
  rejection_reason_ar    text,
  rental                 jsonb,
  amounts                jsonb,
  assigned_to_en         text,
  primary key (owner_id, claim_id),
  foreign key (owner_id, customer_id)        references public.customers (owner_id, customer_id),
  foreign key (owner_id, policy_id)          references public.policies (owner_id, policy_id),
  foreign key (owner_id, vehicle_id)         references public.vehicles (owner_id, vehicle_id),
  foreign key (owner_id, najm_report_number) references public.najm_reports (owner_id, report_number)
);
create index if not exists claims_customer_idx on public.claims (owner_id, customer_id);
create index if not exists claims_policy_idx   on public.claims (owner_id, policy_id);
create index if not exists claims_najm_idx     on public.claims (owner_id, najm_report_number);
create index if not exists claims_status_idx   on public.claims (owner_id, status);

create table if not exists public.valuation_disputes (
  owner_id    uuid not null references auth.users(id) on delete cascade,
  dispute_id  text not null check (dispute_id ~ '^DSP-[0-9]{4}$'),
  claim_id    text not null,
  customer_id text not null,
  status      text not null default 'Open' check (status in ('Open','With Expert','Decided')),
  reasons_en  text,
  evidence    jsonb not null default '[]'::jsonb,
  opened_at   timestamptz not null default now(),
  due_by      date,
  primary key (owner_id, dispute_id),
  foreign key (owner_id, claim_id)    references public.claims (owner_id, claim_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id)
);
create index if not exists valuation_disputes_claim_idx on public.valuation_disputes (owner_id, claim_id);

create table if not exists public.preauths (
  owner_id            uuid not null references auth.users(id) on delete cascade,
  preauth_id          text not null check (preauth_id ~ '^APR-[0-9]{5}$'),
  customer_id         text not null,
  member_id           text not null,
  provider_id         text not null references public.providers(provider_id),
  procedure_en        text not null,
  procedure_ar        text,
  status              text not null default 'Submitted' check (status in ('Submitted','Under Review','Approved','Rejected')),
  submitted_at        timestamptz not null default now(),
  patient_waiting     boolean not null default false,
  priority            text not null default 'Normal' check (priority in ('Normal','Urgent')),
  approved_amount_sar numeric(12,2),
  approval_number     text,
  decided_at          timestamptz,
  notes_en            text,
  primary key (owner_id, preauth_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id),
  foreign key (owner_id, member_id)   references public.health_members (owner_id, member_id)
);
create index if not exists preauths_customer_idx on public.preauths (owner_id, customer_id);
create index if not exists preauths_status_idx   on public.preauths (owner_id, status);

create table if not exists public.bookings (
  owner_id         uuid not null references auth.users(id) on delete cascade,
  booking_id       text not null check (booking_id ~ '^BKG-[0-9]{5}$'),
  customer_id      text,             -- NULL for a third-party claimant's booking (I-13; claim_id set)
  booking_type     text not null check (booking_type in ('Inspection','Rental Car','Hospital Appointment','Home Inspection','Dental')),
  claim_id         text,
  provider_id      text references public.providers(provider_id),
  centre_id        text references public.service_centres(centre_id),
  doctor_id        text references public.provider_doctors(doctor_id),
  member_id        text,
  start_at         timestamptz not null,
  end_at           timestamptz,
  status           text not null default 'Booked' check (status in ('Booked','Completed','Cancelled')),
  details          jsonb not null default '{}'::jsonb,
  patient_pays_sar numeric(10,2) not null default 0,
  primary key (owner_id, booking_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id),
  foreign key (owner_id, claim_id)    references public.claims (owner_id, claim_id),
  foreign key (owner_id, member_id)   references public.health_members (owner_id, member_id),
  check (provider_id is not null or centre_id is not null),
  constraint bookings_customer_or_claim check (customer_id is not null or claim_id is not null)
);
create index if not exists bookings_customer_idx on public.bookings (owner_id, customer_id);
create index if not exists bookings_claim_idx    on public.bookings (owner_id, claim_id);
create index if not exists bookings_start_idx    on public.bookings (owner_id, start_at);
create index if not exists bookings_place_idx    on public.bookings (owner_id, provider_id, centre_id, start_at);

create table if not exists public.quotes (
  owner_id    uuid not null references auth.users(id) on delete cascade,
  quote_id    text not null check (quote_id ~ '^QTE-[0-9]{5}$'),
  customer_id text not null,
  quote_type  text not null check (quote_type in ('renew','upgrade_cover','add_driver','add_addon','cancel_refund','new_policy_motor','health_class_upgrade','add_member','travel')),
  policy_id   text,
  vehicle_id  text,
  params      jsonb not null default '{}'::jsonb,
  breakdown   jsonb not null default '[]'::jsonb,
  total_sar   numeric(12,2) not null,
  status      text not null default 'Open' check (status in ('Open','Applied','Expired')),
  expires_at  timestamptz not null,
  created_at  timestamptz not null default now(),
  primary key (owner_id, quote_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id),
  foreign key (owner_id, policy_id)   references public.policies (owner_id, policy_id),
  foreign key (owner_id, vehicle_id)  references public.vehicles (owner_id, vehicle_id)
);
create index if not exists quotes_customer_idx on public.quotes (owner_id, customer_id);

create table if not exists public.payment_requests (
  owner_id       uuid not null references auth.users(id) on delete cascade,
  payment_id     text not null check (payment_id ~ '^PAY-[0-9]{5}$'),
  customer_id    text not null,
  reference_type text not null check (reference_type in ('Quote','Policy')),
  reference_id   text not null,
  amount_sar     numeric(12,2) not null check (amount_sar >= 0),
  description_en text,
  description_ar text,
  status         text not null default 'Pending' check (status in ('Pending','Paid','Cancelled')),
  method         text check (method in ('mada','Apple Pay','Credit Card','STC Pay')),
  pay_token      text not null default public.demo_new_pay_token() unique,
  created_at     timestamptz not null default now(),
  paid_at        timestamptz,
  primary key (owner_id, payment_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id)
);
create index if not exists payment_requests_customer_idx on public.payment_requests (owner_id, customer_id);
create index if not exists payment_requests_ref_idx      on public.payment_requests (owner_id, reference_id);

create table if not exists public.refunds (
  owner_id    uuid not null references auth.users(id) on delete cascade,
  refund_id   text not null check (refund_id ~ '^RFD-[0-9]{5}$'),
  customer_id text not null,
  policy_id   text not null,
  amount_sar  numeric(12,2) not null check (amount_sar >= 0),
  iban_masked text,
  status      text not null default 'Initiated' check (status in ('Initiated','Sent')),
  expected_by date,
  created_at  timestamptz not null default now(),
  primary key (owner_id, refund_id),
  foreign key (owner_id, customer_id) references public.customers (owner_id, customer_id),
  foreign key (owner_id, policy_id)   references public.policies (owner_id, policy_id)
);
create index if not exists refunds_customer_idx on public.refunds (owner_id, customer_id);

create table if not exists public.complaints (
  owner_id         uuid not null references auth.users(id) on delete cascade,
  complaint_id     text not null check (complaint_id ~ '^(CMP|OBJ)-[0-9]{4}$'),
  customer_id      text not null,
  related_claim_id text,
  complaint_type   text not null check (complaint_type in ('Complaint','Objection')),
  description_en   text,
  status           text not null default 'Open' check (status in ('Open','With Manager','Resolved')),
  opened_at        timestamptz not null default now(),
  due_by           date,
  assigned_to_en   text,
  assigned_to_ar   text,
  regulator_phone  text,
  primary key (owner_id, complaint_id),
  foreign key (owner_id, customer_id)      references public.customers (owner_id, customer_id),
  foreign key (owner_id, related_claim_id) references public.claims (owner_id, claim_id)
);
create index if not exists complaints_customer_idx on public.complaints (owner_id, customer_id);

create table if not exists public.agent_actions (
  id           bigserial primary key,
  owner_id     uuid not null references auth.users(id) on delete cascade,
  customer_id  text,
  reference_id text,
  action_type  text not null,
  description  text,
  metadata     jsonb not null default '{}'::jsonb,
  status       text not null default 'Success' check (status in ('Success','Failed')),
  source       text not null default 'Agent' check (source in ('Agent','Portal','Pay Page')),
  created_at   timestamptz not null default now()
);
create index if not exists agent_actions_owner_created_idx on public.agent_actions (owner_id, created_at desc);
create index if not exists agent_actions_customer_idx      on public.agent_actions (owner_id, customer_id);
create index if not exists agent_actions_reference_idx     on public.agent_actions (owner_id, reference_id);

-- =============================================================================
-- *_backup TABLES — identical columns minus owner_id, PK = business id.
-- Built with LIKE so they can never drift from the live table definitions;
-- dropping owner_id also drops every composite PK/unique/index that used it.
-- =============================================================================
do $$
declare
  r record;
begin
  for r in
    select * from (values
      ('customers','customer_id'), ('dependents','dependent_id'), ('vehicles','vehicle_id'),
      ('policies','policy_id'), ('health_members','member_id'), ('najm_reports','report_number'),
      ('claims','claim_id'), ('valuation_disputes','dispute_id'), ('preauths','preauth_id'),
      ('bookings','booking_id'), ('quotes','quote_id'), ('payment_requests','payment_id'),
      ('refunds','refund_id'), ('complaints','complaint_id')
    ) v(t, pk)
  loop
    if to_regclass('public.' || r.t || '_backup') is null then
      execute format('create table public.%I (like public.%I including defaults including constraints including indexes)',
                     r.t || '_backup', r.t);
      execute format('alter table public.%I drop column owner_id', r.t || '_backup');
      execute format('alter table public.%I add primary key (%I)', r.t || '_backup', r.pk);
    end if;
  end loop;
  -- registry uniqueness inside the baseline too
  if not exists (select 1 from pg_constraint where conname = 'vehicles_backup_sequence_number_key') then
    alter table public.vehicles_backup add constraint vehicles_backup_sequence_number_key unique (sequence_number);
  end if;
end $$;

-- Third-party bookings (I-13): customer_id NULL, claim_id set. Idempotent for
-- databases created before customer_id became nullable (live + backup).
alter table public.bookings        alter column customer_id drop not null;
alter table public.bookings_backup alter column customer_id drop not null;
do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'bookings_customer_or_claim'
                 and conrelid = 'public.bookings'::regclass) then
    alter table public.bookings add constraint bookings_customer_or_claim
      check (customer_id is not null or claim_id is not null);
  end if;
  if not exists (select 1 from pg_constraint where conrelid = 'public.bookings_backup'::regclass
                 and contype = 'c' and pg_get_constraintdef(oid) like '%customer_id IS NOT NULL%claim_id IS NOT NULL%') then
    alter table public.bookings_backup add constraint bookings_backup_customer_or_claim
      check (customer_id is not null or claim_id is not null);
  end if;
end $$;

-- =============================================================================
-- CLONE / RESET
-- =============================================================================

-- Parent-first order for cloning; the reverse (plus agent_actions) for deleting.
create or replace function public.demo_tenant_tables()
returns text[] language sql immutable as $$
  select array['customers','dependents','vehicles','policies','health_members','najm_reports',
               'claims','valuation_disputes','preauths','bookings','quotes','payment_requests',
               'refunds','complaints']
$$;

create or replace function public.clone_baseline_for_user(p_owner uuid)
returns json
language plpgsql security definer
set search_path = public, extensions, pg_temp
set timezone = 'Asia/Riyadh'
as $$
declare
  v_anchor date;
  v_shift  int;
  t        text;
  c        record;
  v_cols   text;
  v_exprs  text;
  v_n      bigint;
  v_rows   jsonb := '{}'::jsonb;
begin
  if p_owner is null then
    raise exception 'clone_baseline_for_user: p_owner is required';
  end if;
  select value::date into v_anchor from public.demo_meta where key = 'anchor_date';
  if v_anchor is null then
    raise exception 'clone_baseline_for_user: demo_meta.anchor_date is missing (run scripts/seed_baseline.py)';
  end if;
  -- "today" is Riyadh today (function runs with timezone Asia/Riyadh).
  v_shift := current_date - v_anchor;

  foreach t in array public.demo_tenant_tables() loop
    v_cols := null; v_exprs := null;
    for c in
      select column_name, data_type
      from information_schema.columns
      where table_schema = 'public' and table_name = t || '_backup'
      order by ordinal_position
    loop
      v_cols := concat_ws(', ', v_cols, quote_ident(c.column_name));
      v_exprs := concat_ws(', ', v_exprs,
        case
          when t = 'payment_requests' and c.column_name = 'pay_token'
            then 'public.demo_new_pay_token()'
          when c.data_type = 'date'
            then format('%I + $2', c.column_name)
          when c.data_type = 'timestamp with time zone'
            then format('%I + ($2 * interval ''1 day'')', c.column_name)
          when c.data_type = 'jsonb'
            then format('public.demo_shift_jsonb(%I, $2)', c.column_name)
          else quote_ident(c.column_name)
        end);
    end loop;
    if v_cols is null then
      raise exception 'clone_baseline_for_user: table public.%_backup not found', t;
    end if;
    execute format('insert into public.%I (owner_id, %s) select $1, %s from public.%I on conflict do nothing',
                   t, v_cols, v_exprs, t || '_backup')
      using p_owner, v_shift;
    get diagnostics v_n = row_count;
    v_rows := v_rows || jsonb_build_object(t, v_n);
  end loop;

  -- Relative-to-now overrides (SPEC §3.4)
  update public.preauths
     set submitted_at = now() - interval '65 minutes'
   where owner_id = p_owner and status = 'Submitted';
  update public.najm_reports
     set accident_at = now() - interval '20 hours'
   where owner_id = p_owner and report_number = 'NJM-2610441';

  return json_build_object('ok', true, 'owner_id', p_owner, 'shift_days', v_shift, 'rows', v_rows);
end $$;

-- Deletes every per-tenant row of one owner, children first (incl. agent_actions).
create or replace function public.demo_wipe_tenant(p_owner uuid)
returns void
language plpgsql security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_tables text[] := public.demo_tenant_tables();
  i int;
begin
  if p_owner is null then raise exception 'demo_wipe_tenant: p_owner is required'; end if;
  delete from public.agent_actions where owner_id = p_owner;
  for i in reverse array_length(v_tables, 1) .. 1 loop
    execute format('delete from public.%I where owner_id = $1', v_tables[i]) using p_owner;
  end loop;
end $$;

-- Portal "Reset Demo Data" — scoped to the signed-in user.
create or replace function public.reset_demo_data()
returns json
language plpgsql security definer
set search_path = public, extensions, pg_temp
set timezone = 'Asia/Riyadh'
as $$
declare
  v_uid   uuid := auth.uid();
  v_clone json;
begin
  if v_uid is null then
    raise exception 'reset_demo_data: not authenticated' using errcode = '42501';
  end if;
  perform public.demo_wipe_tenant(v_uid);
  v_clone := public.clone_baseline_for_user(v_uid);
  return json_build_object('ok', true, 'reset_at', now(),
                           'shift_days', v_clone->'shift_days', 'rows', v_clone->'rows');
end $$;

-- Service-role variant (seed script --wipe, ops). Never exposed to users.
create or replace function public.reset_demo_data_for(p_owner uuid)
returns json
language plpgsql security definer
set search_path = public, extensions, pg_temp
set timezone = 'Asia/Riyadh'
as $$
declare v_clone json;
begin
  perform public.demo_wipe_tenant(p_owner);
  v_clone := public.clone_baseline_for_user(p_owner);
  return json_build_object('ok', true, 'reset_at', now(),
                           'shift_days', v_clone->'shift_days', 'rows', v_clone->'rows');
end $$;

-- =============================================================================
-- ON-SIGNUP TRIGGER (auth.users AFTER INSERT)
--   raw_user_meta_data: {whatsapp_number: '+9665…', full_name: '…', skip_clone?: true}
--   Never blocks sign-up (same behaviour as Barq): failures are downgraded to
--   warnings and the portal's "register your number" gate inserts the row.
--   - whatsapp_number normalised (spaces/dashes stripped, 00 -> +, + added) and
--     validated E.164; invalid or already registered -> WARNING, no demo_users
--     row, baseline still cloned
--   - missing whatsapp_number -> no demo_users row
--   - skip_clone=true -> service account (Mode B): no demo_users row, nothing cloned
-- =============================================================================
create or replace function public.handle_new_demo_user()
returns trigger
language plpgsql security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_meta jsonb := coalesce(new.raw_user_meta_data, '{}'::jsonb);
  v_raw  text  := v_meta->>'whatsapp_number';
  v_num  text;
begin
  if coalesce(v_meta->>'skip_clone', 'false') = 'true' then
    return new;
  end if;

  if nullif(btrim(coalesce(v_raw, '')), '') is not null then
    v_num := regexp_replace(v_raw, '[\s\-\(\)\.]', '', 'g');
    if v_num like '00%' then
      v_num := '+' || substr(v_num, 3);
    elsif left(v_num, 1) <> '+' then
      v_num := '+' || v_num;
    end if;
    if v_num ~ '^\+[1-9][0-9]{7,14}$' then
      begin
        insert into public.demo_users (owner_id, email, whatsapp_number, full_name)
        values (new.id, new.email, v_num, nullif(btrim(coalesce(v_meta->>'full_name', '')), ''))
        on conflict (owner_id) do update
          set email = excluded.email,
              whatsapp_number = excluded.whatsapp_number,
              full_name = coalesce(excluded.full_name, public.demo_users.full_name);
      exception when unique_violation then
        raise warning 'handle_new_demo_user: whatsapp_number % already registered to another user — demo_users row not created for %',
          v_num, new.id;
      end;
    else
      raise warning 'handle_new_demo_user: whatsapp_number % is not E.164 — demo_users row not created for %', v_raw, new.id;
    end if;
  end if;

  begin
    perform public.clone_baseline_for_user(new.id);
  exception when others then
    raise warning 'handle_new_demo_user: baseline clone failed for %: %', new.id, sqlerrm;
  end;
  return new;
end $$;

drop trigger if exists on_auth_user_created_watheeq on auth.users;
create trigger on_auth_user_created_watheeq
  after insert on auth.users
  for each row execute function public.handle_new_demo_user();

-- =============================================================================
-- RLS + GRANTS
-- =============================================================================
do $$
declare
  t text;
  v_tenant text[] := public.demo_tenant_tables() || array['agent_actions'];
  v_shared text[] := array['demo_meta','health_classes','providers','provider_doctors','service_centres',
                           'plans','addons','business_rules','demo_assets'];
  v_has_jwt boolean := to_regprocedure('auth.jwt()') is not null;
begin
  -- per-tenant: own rows only
  foreach t in array v_tenant loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists tenant_rw on public.%I', t);
    execute format('create policy tenant_rw on public.%I for all to authenticated
                      using (owner_id = auth.uid()) with check (owner_id = auth.uid())', t);
    -- Mode B (API signs in as a service-account user): additive cross-tenant
    -- policy for users whose app_metadata.role = 'service_agent' (set by an
    -- admin only — users cannot write app_metadata).
    execute format('drop policy if exists service_agent_rw on public.%I', t);
    if v_has_jwt then
      execute format('create policy service_agent_rw on public.%I for all to authenticated
                        using ((auth.jwt() -> ''app_metadata'' ->> ''role'') = ''service_agent'')
                        with check ((auth.jwt() -> ''app_metadata'' ->> ''role'') = ''service_agent'')', t);
    end if;
    execute format('revoke all on public.%I from anon', t);
    execute format('grant select, insert, update, delete on public.%I to authenticated', t);
    execute format('grant all on public.%I to service_role', t);
  end loop;

  -- shared: read-only for authenticated
  foreach t in array v_shared loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists shared_read on public.%I', t);
    execute format('create policy shared_read on public.%I for select to authenticated using (true)', t);
    execute format('revoke all on public.%I from anon, authenticated', t);
    execute format('grant select on public.%I to authenticated', t);
    execute format('grant all on public.%I to service_role', t);
  end loop;

  -- backups: service role only
  foreach t in array public.demo_tenant_tables() loop
    execute format('alter table public.%I enable row level security', t || '_backup');
    execute format('revoke all on public.%I from anon, authenticated', t || '_backup');
    execute format('grant all on public.%I to service_role', t || '_backup');
  end loop;
end $$;

grant usage, select on sequence public.agent_actions_id_seq to authenticated, service_role;
revoke all on sequence public.agent_actions_id_seq from anon;

-- demo_users: own row (select / update; insert own row so the portal's
-- "register your number" gate can create it — same as Barq); service agent
-- (Mode B) may read/write all rows for tenant routing
alter table public.demo_users enable row level security;
drop policy if exists demo_users_select_own on public.demo_users;
create policy demo_users_select_own on public.demo_users for select to authenticated using (owner_id = auth.uid());
drop policy if exists demo_users_update_own on public.demo_users;
create policy demo_users_update_own on public.demo_users for update to authenticated
  using (owner_id = auth.uid()) with check (owner_id = auth.uid());
drop policy if exists demo_users_insert_own on public.demo_users;
create policy demo_users_insert_own on public.demo_users for insert to authenticated with check (owner_id = auth.uid());
drop policy if exists demo_users_service_agent_read on public.demo_users;
drop policy if exists demo_users_service_agent_rw on public.demo_users;
do $$ begin
  if to_regprocedure('auth.jwt()') is not null then
    create policy demo_users_service_agent_rw on public.demo_users for all to authenticated
      using ((auth.jwt() -> 'app_metadata' ->> 'role') = 'service_agent')
      with check ((auth.jwt() -> 'app_metadata' ->> 'role') = 'service_agent');
  end if;
end $$;
revoke all on public.demo_users from anon, authenticated;
grant select, insert, update on public.demo_users to authenticated;
grant all on public.demo_users to service_role;

-- functions
revoke all on function public.clone_baseline_for_user(uuid) from public, anon, authenticated;
revoke all on function public.reset_demo_data_for(uuid)     from public, anon, authenticated;
revoke all on function public.demo_wipe_tenant(uuid)        from public, anon, authenticated;
revoke all on function public.handle_new_demo_user()        from public, anon, authenticated;
revoke all on function public.reset_demo_data()             from public, anon;
grant execute on function public.clone_baseline_for_user(uuid) to service_role;
grant execute on function public.reset_demo_data_for(uuid)     to service_role;
grant execute on function public.demo_wipe_tenant(uuid)        to service_role;
grant execute on function public.reset_demo_data()             to authenticated, service_role;

-- =============================================================================
-- REALTIME (guarded: publication exists on Supabase; skipped elsewhere)
-- =============================================================================
do $$
declare t text;
begin
  if exists (select 1 from pg_publication where pubname = 'supabase_realtime') then
    foreach t in array array['agent_actions','policies','claims','preauths','bookings','payment_requests',
                             'vehicles','health_members','complaints','valuation_disputes','refunds',
                             'customers','quotes','dependents'] loop
      if not exists (select 1 from pg_publication_tables
                     where pubname = 'supabase_realtime' and schemaname = 'public' and tablename = t) then
        execute format('alter publication supabase_realtime add table public.%I', t);
      end if;
    end loop;
  end if;
end $$;

-- =============================================================================
-- STORAGE (guarded)
--   bucket `documents` (private):
--     demo-assets/…                 Demo Kit files (any signed-in user may read)
--     <owner_id>/<type>/…           generated PDFs (own folder only)
--     attachments/<owner_id>/…      customer media copies (own folder only)
-- =============================================================================
do $$
begin
  -- The private bucket 'documents' is NOT created here: Lovable Cloud only allows bucket
  -- creation through its Storage API. Create it there (private) before using the portal.
  if to_regclass('storage.objects') is not null then
    if not exists (select 1 from pg_policies
                   where schemaname = 'storage' and tablename = 'objects' and policyname = 'watheeq_documents_read') then
      create policy watheeq_documents_read on storage.objects for select to authenticated
        using (
          bucket_id = 'documents'
          and (
            split_part(name, '/', 1) = 'demo-assets'
            or split_part(name, '/', 1) = auth.uid()::text
            or (split_part(name, '/', 1) = 'attachments' and split_part(name, '/', 2) = auth.uid()::text)
          )
        );
    end if;
  end if;
exception when others then
  raise notice 'storage policy watheeq_documents_read not created (%). Create it via the platform storage tools.', sqlerrm;
end $$;

notify pgrst, 'reload schema';
