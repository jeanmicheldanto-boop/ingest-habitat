--
-- PostgreSQL database dump
--

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
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

CREATE SCHEMA public;


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: eligibilite_statut; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.eligibilite_statut AS ENUM (
    'avp_eligible',
    'non_eligible',
    'a_verifier'
);


--
-- Name: fourchette_prix; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.fourchette_prix AS ENUM (
    'euro',
    'deux_euros',
    'trois_euros'
);


--
-- Name: geocode_precision; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.geocode_precision AS ENUM (
    'rooftop',
    'range_interpolated',
    'street',
    'locality',
    'unknown'
);


--
-- Name: habitat_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.habitat_type AS ENUM (
    'logement_independant',
    'residence',
    'habitat_partage'
);


--
-- Name: proposition_item_statut; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.proposition_item_statut AS ENUM (
    'pending',
    'accepted',
    'rejected'
);


--
-- Name: proposition_statut; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.proposition_statut AS ENUM (
    'en_attente',
    'approuvee',
    'partielle',
    'rejetee',
    'retiree'
);


--
-- Name: reclamation_statut; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.reclamation_statut AS ENUM (
    'en_attente',
    'verifiee',
    'rejetee'
);


--
-- Name: statut_editorial; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.statut_editorial AS ENUM (
    'draft',
    'soumis',
    'valide',
    'publie',
    'archive'
);


--
-- Name: can_publish(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.can_publish(p_etab uuid) RETURNS boolean
    LANGUAGE sql STABLE
    AS $_$
WITH e AS (
  SELECT *
  FROM public.etablissements
  WHERE id = p_etab
)
SELECT
  -- 1) nom
  COALESCE(NULLIF(trim(nom),''), NULL) IS NOT NULL
  -- 2) adresse (accepte adresse_l1 OU adresse_l2), commune, code postal (non vide), géoloc non nulle
  AND COALESCE(NULLIF(trim(adresse_l1),''), NULLIF(trim(adresse_l2),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(commune),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(code_postal),''), NULL) IS NOT NULL
  AND geom IS NOT NULL
  -- 3) gestionnaire
  AND COALESCE(NULLIF(trim(gestionnaire),''), NULL) IS NOT NULL
  -- 4) typage d’habitat : **nouveau champ** OU (legacy) au moins une sous-catégorie liée
  AND (
        habitat_type IS NOT NULL
     OR EXISTS (
          SELECT 1
          FROM public.etablissement_sous_categorie esc
          WHERE esc.etablissement_id = p_etab
        )
  )
  -- 5) email au format simple (comme avant)
  AND email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
FROM e;
$_$;


--
-- Name: is_admin(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.is_admin() RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
  select exists (select 1 from public.admins a where a.user_id = auth.uid());
$$;


--
-- Name: set_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  new.updated_at := now();
  return new;
end; $$;


--
-- Name: snapshot_on_publish(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.snapshot_on_publish() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if (tg_op = 'UPDATE')
     and (new.statut_editorial = 'publie')
     and (coalesce(old.statut_editorial::text, '') <> 'publie') then
    insert into public.etablissement_versions(etablissement_id, snapshot, created_by, reason)
    values (new.id, public.snapshot_payload(new.id), auth.uid(), 'publication');
  end if;
  return new;
end;
$$;


--
-- Name: snapshot_payload(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.snapshot_payload(p_etab uuid) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
with e as (
  select *
  from public.etablissements
  where id = p_etab
),
etab_json as (
  select (to_jsonb(e.*) - 'geom')
         || jsonb_build_object(
              'geom',
              case when e.geom is not null then ST_AsGeoJSON(e.geom)::jsonb else null end
            ) as obj
  from e
),
latest_tarif as (
  select to_jsonb(t.*) as obj
  from (
    select distinct on (t.etablissement_id) t.*
    from public.tarifications t
    where t.etablissement_id = p_etab
    order by t.etablissement_id, t.date_observation desc nulls last, t.periode desc nulls last
  ) t
),
services as (
  select coalesce(jsonb_agg(distinct s.libelle order by s.libelle), '[]'::jsonb) as arr
  from public.etablissement_service es
  join public.services s on s.id = es.service_id
  where es.etablissement_id = p_etab
),
restauration as (
  select to_jsonb(r.*) as obj
  from public.restaurations r
  where r.etablissement_id = p_etab
  limit 1
),
logements as (
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'libelle',   lt.libelle,
        'pmr',       lt.pmr,
        'domotique', lt.domotique,
        'meuble',    lt.meuble
      ) order by lt.libelle
    ),
    '[]'::jsonb
  ) as arr
  from public.logements_types lt
  where lt.etablissement_id = p_etab
)
select jsonb_build_object(
  'etablissement',   (select obj from etab_json),
  'tarification',    (select obj from latest_tarif limit 1),
  'services',        (select arr from services),
  'restaurations',   (select obj from restauration),
  'logements_types', (select arr from logements)
);
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admins; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admins (
    user_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: categories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.categories (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    libelle text NOT NULL
);


--
-- Name: disponibilites; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.disponibilites (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    date_capture date DEFAULT CURRENT_DATE NOT NULL,
    statut_disponibilite text,
    nb_unites_dispo integer,
    date_prochaine_dispo date,
    canal text,
    note text,
    CONSTRAINT disponibilites_statut_disponibilite_check CHECK ((statut_disponibilite = ANY (ARRAY['oui'::text, 'non'::text, 'nous_contacter'::text, 'inconnu'::text])))
);


--
-- Name: etablissement_proprietaires; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.etablissement_proprietaires (
    etablissement_id uuid NOT NULL,
    user_id uuid NOT NULL,
    role text DEFAULT 'gestionnaire'::text NOT NULL,
    active boolean DEFAULT true NOT NULL
);


--
-- Name: etablissement_service; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.etablissement_service (
    etablissement_id uuid NOT NULL,
    service_id uuid NOT NULL
);


--
-- Name: etablissement_sous_categorie; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.etablissement_sous_categorie (
    etablissement_id uuid NOT NULL,
    sous_categorie_id uuid NOT NULL
);


--
-- Name: etablissement_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.etablissement_versions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    snapshot jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid,
    reason text
);


--
-- Name: etablissements; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.etablissements (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    nom text NOT NULL,
    presentation text,
    adresse_l1 text,
    adresse_l2 text,
    code_postal text,
    commune text,
    code_insee text,
    departement text,
    region text,
    pays text DEFAULT 'FR'::text,
    geom public.geometry(Point,4326),
    geocode_precision public.geocode_precision,
    statut_editorial public.statut_editorial DEFAULT 'draft'::public.statut_editorial,
    eligibilite_statut public.eligibilite_statut,
    public_cible text,
    source text,
    url_source text,
    date_observation date,
    date_verification date,
    confiance_score double precision,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    telephone text,
    email text,
    site_web text,
    gestionnaire text,
    is_test boolean DEFAULT false NOT NULL,
    test_tag text,
    habitat_type public.habitat_type DEFAULT 'residence'::public.habitat_type,
    CONSTRAINT etablissements_confiance_score_check CHECK (((confiance_score IS NULL) OR ((confiance_score >= (0)::double precision) AND (confiance_score <= (1)::double precision))))
);


--
-- Name: logements_types; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logements_types (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    libelle text,
    surface_min numeric,
    surface_max numeric,
    meuble boolean,
    pmr boolean,
    domotique boolean,
    nb_unites integer,
    plain_pied boolean DEFAULT false NOT NULL
);


--
-- Name: medias; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.medias (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    storage_path text NOT NULL,
    alt_text text,
    priority integer DEFAULT 0,
    licence text,
    credit text,
    source_url text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proposition_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proposition_items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    proposition_id uuid NOT NULL,
    table_name text NOT NULL,
    column_name text NOT NULL,
    old_value jsonb,
    new_value jsonb,
    statut public.proposition_item_statut DEFAULT 'pending'::public.proposition_item_statut NOT NULL
);


--
-- Name: propositions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.propositions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid,
    cible_id uuid,
    type_cible text NOT NULL,
    action text NOT NULL,
    statut public.proposition_statut DEFAULT 'en_attente'::public.proposition_statut NOT NULL,
    source text DEFAULT 'gestionnaire'::text NOT NULL,
    created_by uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reviewed_by uuid,
    reviewed_at timestamp with time zone,
    review_note text,
    payload jsonb,
    habitat_type public.habitat_type,
    surface_min numeric(6,2),
    surface_max numeric(6,2),
    plain_pied boolean,
    espace_partage boolean,
    CONSTRAINT propositions_action_check CHECK ((action = ANY (ARRAY['create'::text, 'update'::text]))),
    CONSTRAINT propositions_type_cible_check CHECK ((type_cible = ANY (ARRAY['etablissement'::text, 'logements_types'::text, 'restaurations'::text, 'tarifications'::text, 'services'::text, 'etablissement_service'::text])))
);


--
-- Name: reclamations_propriete; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reclamations_propriete (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    user_id uuid NOT NULL,
    organisation text,
    email_declaire text,
    domaine text,
    preuve_path text,
    statut public.reclamation_statut DEFAULT 'en_attente'::public.reclamation_statut NOT NULL,
    note_moderation text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: restaurations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.restaurations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    kitchenette boolean DEFAULT false NOT NULL,
    resto_collectif_midi boolean DEFAULT false NOT NULL,
    resto_collectif boolean DEFAULT false NOT NULL,
    portage_repas boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: services; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.services (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    libelle text NOT NULL
);


--
-- Name: sous_categories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sous_categories (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    categorie_id uuid NOT NULL,
    libelle text NOT NULL,
    alias text
);


--
-- Name: stg_etablissements_csv; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_etablissements_csv (
    id bigint NOT NULL,
    nom text,
    presentation text,
    adresse_l1 text,
    code_postal text,
    commune text,
    departement text,
    region text,
    pays text,
    statut_editorial text,
    eligibilite_statut text,
    public_cible text,
    sous_categories text,
    services text,
    fourchette_prix text,
    prix_min numeric,
    prix_max numeric,
    statut_disponibilite text,
    image_path text,
    loaded_at timestamp with time zone DEFAULT now()
);


--
-- Name: stg_etablissements_csv_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stg_etablissements_csv_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_etablissements_csv_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stg_etablissements_csv_id_seq OWNED BY public.stg_etablissements_csv.id;


--
-- Name: tarifications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tarifications (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    etablissement_id uuid NOT NULL,
    logements_type_id uuid,
    periode text,
    fourchette_prix public.fourchette_prix,
    prix_min numeric,
    prix_max numeric,
    loyer_base numeric,
    charges numeric,
    devise text DEFAULT 'EUR'::text,
    source text,
    date_observation date
);


--
-- Name: v_file_moderation; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_file_moderation AS
 SELECT 'proposition'::text AS kind,
    p.id,
    p.created_at,
    p.source,
    p.type_cible,
    p.action,
    p.statut,
    p.etablissement_id,
    e.nom,
    e.commune,
    e.departement
   FROM (public.propositions p
     LEFT JOIN public.etablissements e ON ((e.id = p.etablissement_id)))
  WHERE (p.statut = 'en_attente'::public.proposition_statut)
UNION ALL
 SELECT 'reclamation'::text AS kind,
    r.id,
    r.created_at,
    'gestionnaire'::text AS source,
    'etablissement'::text AS type_cible,
    'update'::text AS action,
        CASE r.statut
            WHEN 'en_attente'::public.reclamation_statut THEN 'en_attente'::public.proposition_statut
            WHEN 'verifiee'::public.reclamation_statut THEN 'approuvee'::public.proposition_statut
            ELSE 'rejetee'::public.proposition_statut
        END AS statut,
    r.etablissement_id,
    e.nom,
    e.commune,
    e.departement
   FROM (public.reclamations_propriete r
     LEFT JOIN public.etablissements e ON ((e.id = r.etablissement_id)))
  WHERE (r.statut = 'en_attente'::public.reclamation_statut);


--
-- Name: v_liste_publication; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_liste_publication AS
 SELECT id AS etab_id,
    nom,
    presentation,
    commune,
    departement,
    region,
    code_postal,
    pays,
    geom,
    geocode_precision,
    telephone,
    email,
    site_web,
        CASE
            WHEN (COALESCE(public_cible, ''::text) = ''::text) THEN ARRAY[]::text[]
            ELSE ( SELECT array_agg(TRIM(BOTH FROM x.x)) AS array_agg
               FROM unnest(string_to_array(e.public_cible, ','::text)) x(x))
        END AS public_cible,
    ( SELECT array_agg(DISTINCT sc.libelle ORDER BY sc.libelle) AS array_agg
           FROM (public.etablissement_sous_categorie esc
             JOIN public.sous_categories sc ON ((sc.id = esc.sous_categorie_id)))
          WHERE (esc.etablissement_id = e.id)) AS sous_categories,
    ( SELECT array_agg(DISTINCT s.libelle ORDER BY s.libelle) AS array_agg
           FROM (public.etablissement_service es
             JOIN public.services s ON ((s.id = es.service_id)))
          WHERE (es.etablissement_id = e.id)) AS services,
    ( SELECT t.fourchette_prix
           FROM public.tarifications t
          WHERE (t.etablissement_id = e.id)
          ORDER BY t.date_observation DESC NULLS LAST
         LIMIT 1) AS fourchette_prix,
    ( SELECT t.prix_min
           FROM public.tarifications t
          WHERE (t.etablissement_id = e.id)
          ORDER BY t.date_observation DESC NULLS LAST
         LIMIT 1) AS prix_min,
    ( SELECT t.prix_max
           FROM public.tarifications t
          WHERE (t.etablissement_id = e.id)
          ORDER BY t.date_observation DESC NULLS LAST
         LIMIT 1) AS prix_max,
    ( SELECT d.statut_disponibilite
           FROM public.disponibilites d
          WHERE (d.etablissement_id = e.id)
          ORDER BY d.date_capture DESC NULLS LAST
         LIMIT 1) AS statut_disponibilite,
    ( SELECT d.nb_unites_dispo
           FROM public.disponibilites d
          WHERE (d.etablissement_id = e.id)
          ORDER BY d.date_capture DESC NULLS LAST
         LIMIT 1) AS nb_unites_dispo,
    ( SELECT m.storage_path
           FROM public.medias m
          WHERE (m.etablissement_id = e.id)
          ORDER BY m.priority, m.created_at DESC
         LIMIT 1) AS image_path,
    ( SELECT r.kitchenette
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS kitchenette,
    ( SELECT r.resto_collectif_midi
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS resto_collectif_midi,
    ( SELECT r.resto_collectif
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS resto_collectif,
    ( SELECT r.portage_repas
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS portage_repas,
    ( SELECT COALESCE(json_agg(json_build_object('libelle', lt.libelle, 'surface_min', lt.surface_min, 'surface_max', lt.surface_max, 'meuble', lt.meuble, 'pmr', lt.pmr, 'domotique', lt.domotique, 'nb_unites', lt.nb_unites)) FILTER (WHERE (lt.id IS NOT NULL)), '[]'::json) AS "coalesce"
           FROM public.logements_types lt
          WHERE (lt.etablissement_id = e.id)) AS logements_types
   FROM public.etablissements e
  WHERE (statut_editorial = 'publie'::public.statut_editorial);


--
-- Name: v_liste_publication_geoloc; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_liste_publication_geoloc AS
 SELECT id AS etab_id,
    nom,
    presentation,
    commune,
    departement,
    region,
    code_postal,
    pays,
    gestionnaire,
    geom,
    public.st_y(geom) AS latitude,
    public.st_x(geom) AS longitude,
    geocode_precision,
    telephone,
    email,
    site_web,
    habitat_type,
        CASE
            WHEN (COALESCE(public_cible, ''::text) = ''::text) THEN ARRAY[]::text[]
            ELSE ( SELECT array_agg(TRIM(BOTH FROM x.x)) AS array_agg
               FROM unnest(string_to_array(e.public_cible, ','::text)) x(x))
        END AS public_cible,
    ( SELECT array_agg(DISTINCT sc.libelle ORDER BY sc.libelle) AS array_agg
           FROM (public.etablissement_sous_categorie esc
             JOIN public.sous_categories sc ON ((sc.id = esc.sous_categorie_id)))
          WHERE (esc.etablissement_id = e.id)) AS sous_categories,
    ( SELECT array_agg(DISTINCT s.libelle ORDER BY s.libelle) AS array_agg
           FROM (public.etablissement_service es
             JOIN public.services s ON ((s.id = es.service_id)))
          WHERE (es.etablissement_id = e.id)) AS services,
    ( SELECT t.fourchette_prix
           FROM public.tarifications t
          WHERE (t.etablissement_id = e.id)
          ORDER BY t.date_observation DESC NULLS LAST
         LIMIT 1) AS fourchette_prix,
    ( SELECT t.prix_min
           FROM public.tarifications t
          WHERE (t.etablissement_id = e.id)
          ORDER BY t.date_observation DESC NULLS LAST
         LIMIT 1) AS prix_min,
    ( SELECT t.prix_max
           FROM public.tarifications t
          WHERE (t.etablissement_id = e.id)
          ORDER BY t.date_observation DESC NULLS LAST
         LIMIT 1) AS prix_max,
    ( SELECT d.statut_disponibilite
           FROM public.disponibilites d
          WHERE (d.etablissement_id = e.id)
          ORDER BY d.date_capture DESC NULLS LAST
         LIMIT 1) AS statut_disponibilite,
    ( SELECT d.nb_unites_dispo
           FROM public.disponibilites d
          WHERE (d.etablissement_id = e.id)
          ORDER BY d.date_capture DESC NULLS LAST
         LIMIT 1) AS nb_unites_dispo,
    ( SELECT m.storage_path
           FROM public.medias m
          WHERE (m.etablissement_id = e.id)
          ORDER BY m.priority, m.created_at DESC
         LIMIT 1) AS image_path,
    ( SELECT r.kitchenette
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS kitchenette,
    ( SELECT r.resto_collectif_midi
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS resto_collectif_midi,
    ( SELECT r.resto_collectif
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS resto_collectif,
    ( SELECT r.portage_repas
           FROM public.restaurations r
          WHERE (r.etablissement_id = e.id)
         LIMIT 1) AS portage_repas,
    ( SELECT COALESCE(json_agg(json_build_object('libelle', lt.libelle, 'surface_min', lt.surface_min, 'surface_max', lt.surface_max, 'meuble', lt.meuble, 'pmr', lt.pmr, 'domotique', lt.domotique, 'nb_unites', lt.nb_unites)) FILTER (WHERE (lt.id IS NOT NULL)), '[]'::json) AS "coalesce"
           FROM public.logements_types lt
          WHERE (lt.etablissement_id = e.id)) AS logements_types
   FROM public.etablissements e
  WHERE (statut_editorial = 'publie'::public.statut_editorial);


--
-- Name: stg_etablissements_csv id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_etablissements_csv ALTER COLUMN id SET DEFAULT nextval('public.stg_etablissements_csv_id_seq'::regclass);


--
-- Name: admins admins_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admins
    ADD CONSTRAINT admins_pkey PRIMARY KEY (user_id);


--
-- Name: categories categories_libelle_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.categories
    ADD CONSTRAINT categories_libelle_key UNIQUE (libelle);


--
-- Name: categories categories_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.categories
    ADD CONSTRAINT categories_pkey PRIMARY KEY (id);


--
-- Name: disponibilites disponibilites_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.disponibilites
    ADD CONSTRAINT disponibilites_pkey PRIMARY KEY (id);


--
-- Name: etablissement_proprietaires etablissement_proprietaires_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_proprietaires
    ADD CONSTRAINT etablissement_proprietaires_pkey PRIMARY KEY (etablissement_id, user_id);


--
-- Name: etablissement_service etablissement_service_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_service
    ADD CONSTRAINT etablissement_service_pkey PRIMARY KEY (etablissement_id, service_id);


--
-- Name: etablissement_sous_categorie etablissement_sous_categorie_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_sous_categorie
    ADD CONSTRAINT etablissement_sous_categorie_pkey PRIMARY KEY (etablissement_id, sous_categorie_id);


--
-- Name: etablissement_versions etablissement_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_versions
    ADD CONSTRAINT etablissement_versions_pkey PRIMARY KEY (id);


--
-- Name: etablissements etablissements_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissements
    ADD CONSTRAINT etablissements_pkey PRIMARY KEY (id);


--
-- Name: etablissements etablissements_publish_check; Type: CHECK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.etablissements
    ADD CONSTRAINT etablissements_publish_check CHECK (((statut_editorial <> 'publie'::public.statut_editorial) OR public.can_publish(id))) NOT VALID;


--
-- Name: logements_types logements_types_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logements_types
    ADD CONSTRAINT logements_types_pkey PRIMARY KEY (id);


--
-- Name: medias medias_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.medias
    ADD CONSTRAINT medias_pkey PRIMARY KEY (id);


--
-- Name: proposition_items proposition_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proposition_items
    ADD CONSTRAINT proposition_items_pkey PRIMARY KEY (id);


--
-- Name: propositions propositions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.propositions
    ADD CONSTRAINT propositions_pkey PRIMARY KEY (id);


--
-- Name: reclamations_propriete reclamations_propriete_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reclamations_propriete
    ADD CONSTRAINT reclamations_propriete_pkey PRIMARY KEY (id);


--
-- Name: restaurations restaurations_etablissement_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurations
    ADD CONSTRAINT restaurations_etablissement_id_key UNIQUE (etablissement_id);


--
-- Name: restaurations restaurations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurations
    ADD CONSTRAINT restaurations_pkey PRIMARY KEY (id);


--
-- Name: services services_libelle_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.services
    ADD CONSTRAINT services_libelle_key UNIQUE (libelle);


--
-- Name: services services_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.services
    ADD CONSTRAINT services_pkey PRIMARY KEY (id);


--
-- Name: sous_categories sous_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sous_categories
    ADD CONSTRAINT sous_categories_pkey PRIMARY KEY (id);


--
-- Name: stg_etablissements_csv stg_etablissements_csv_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_etablissements_csv
    ADD CONSTRAINT stg_etablissements_csv_pkey PRIMARY KEY (id);


--
-- Name: tarifications tarifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tarifications
    ADD CONSTRAINT tarifications_pkey PRIMARY KEY (id);


--
-- Name: idx_etablissements_commune; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_etablissements_commune ON public.etablissements USING gin (commune public.gin_trgm_ops);


--
-- Name: idx_etablissements_geom; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_etablissements_geom ON public.etablissements USING gist (geom);


--
-- Name: idx_etablissements_nom; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_etablissements_nom ON public.etablissements USING gin (nom public.gin_trgm_ops);


--
-- Name: idx_prop_created_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_prop_created_by ON public.propositions USING btree (created_by);


--
-- Name: idx_prop_etab; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_prop_etab ON public.propositions USING btree (etablissement_id);


--
-- Name: idx_prop_items_prop; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_prop_items_prop ON public.proposition_items USING btree (proposition_id);


--
-- Name: idx_prop_statut; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_prop_statut ON public.propositions USING btree (statut);


--
-- Name: idx_tarifs_periode; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tarifs_periode ON public.tarifications USING btree (etablissement_id, periode);


--
-- Name: restaurations trg_restaurations_updated; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_restaurations_updated BEFORE UPDATE ON public.restaurations FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: etablissements trg_snapshot_on_publish; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_snapshot_on_publish AFTER UPDATE OF statut_editorial ON public.etablissements FOR EACH ROW EXECUTE FUNCTION public.snapshot_on_publish();


--
-- Name: disponibilites disponibilites_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.disponibilites
    ADD CONSTRAINT disponibilites_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: etablissement_proprietaires etablissement_proprietaires_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_proprietaires
    ADD CONSTRAINT etablissement_proprietaires_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: etablissement_service etablissement_service_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_service
    ADD CONSTRAINT etablissement_service_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: etablissement_service etablissement_service_service_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_service
    ADD CONSTRAINT etablissement_service_service_id_fkey FOREIGN KEY (service_id) REFERENCES public.services(id) ON DELETE CASCADE;


--
-- Name: etablissement_sous_categorie etablissement_sous_categorie_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_sous_categorie
    ADD CONSTRAINT etablissement_sous_categorie_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: etablissement_sous_categorie etablissement_sous_categorie_sous_categorie_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_sous_categorie
    ADD CONSTRAINT etablissement_sous_categorie_sous_categorie_id_fkey FOREIGN KEY (sous_categorie_id) REFERENCES public.sous_categories(id) ON DELETE CASCADE;


--
-- Name: etablissement_versions etablissement_versions_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etablissement_versions
    ADD CONSTRAINT etablissement_versions_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: logements_types logements_types_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logements_types
    ADD CONSTRAINT logements_types_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: medias medias_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.medias
    ADD CONSTRAINT medias_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: proposition_items proposition_items_proposition_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proposition_items
    ADD CONSTRAINT proposition_items_proposition_id_fkey FOREIGN KEY (proposition_id) REFERENCES public.propositions(id) ON DELETE CASCADE;


--
-- Name: propositions propositions_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.propositions
    ADD CONSTRAINT propositions_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: reclamations_propriete reclamations_propriete_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reclamations_propriete
    ADD CONSTRAINT reclamations_propriete_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: restaurations restaurations_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurations
    ADD CONSTRAINT restaurations_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: sous_categories sous_categories_categorie_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sous_categories
    ADD CONSTRAINT sous_categories_categorie_id_fkey FOREIGN KEY (categorie_id) REFERENCES public.categories(id) ON DELETE CASCADE;


--
-- Name: tarifications tarifications_etablissement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tarifications
    ADD CONSTRAINT tarifications_etablissement_id_fkey FOREIGN KEY (etablissement_id) REFERENCES public.etablissements(id) ON DELETE CASCADE;


--
-- Name: tarifications tarifications_logements_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tarifications
    ADD CONSTRAINT tarifications_logements_type_id_fkey FOREIGN KEY (logements_type_id) REFERENCES public.logements_types(id) ON DELETE SET NULL;


--
-- Name: etablissements Allow insert for all; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "Allow insert for all" ON public.etablissements FOR INSERT WITH CHECK (true);


--
-- Name: admins; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.admins ENABLE ROW LEVEL SECURITY;

--
-- Name: admins admins manage admins; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "admins manage admins" ON public.admins TO authenticated USING (public.is_admin()) WITH CHECK (public.is_admin());


--
-- Name: categories; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.categories ENABLE ROW LEVEL SECURITY;

--
-- Name: disponibilites; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.disponibilites ENABLE ROW LEVEL SECURITY;

--
-- Name: etablissement_proprietaires; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.etablissement_proprietaires ENABLE ROW LEVEL SECURITY;

--
-- Name: etablissement_service; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.etablissement_service ENABLE ROW LEVEL SECURITY;

--
-- Name: etablissement_sous_categorie; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.etablissement_sous_categorie ENABLE ROW LEVEL SECURITY;

--
-- Name: etablissements; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.etablissements ENABLE ROW LEVEL SECURITY;

--
-- Name: logements_types; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.logements_types ENABLE ROW LEVEL SECURITY;

--
-- Name: medias; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.medias ENABLE ROW LEVEL SECURITY;

--
-- Name: etablissement_proprietaires owners select self or admin; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "owners select self or admin" ON public.etablissement_proprietaires FOR SELECT TO authenticated USING (((user_id = auth.uid()) OR public.is_admin()));


--
-- Name: etablissement_proprietaires owners write admin only; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "owners write admin only" ON public.etablissement_proprietaires TO authenticated USING (public.is_admin()) WITH CHECK (public.is_admin());


--
-- Name: propositions prop delete admin only; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "prop delete admin only" ON public.propositions FOR DELETE TO authenticated USING (public.is_admin());


--
-- Name: propositions prop insert any authenticated; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "prop insert any authenticated" ON public.propositions FOR INSERT TO authenticated WITH CHECK (true);


--
-- Name: propositions prop select owner or admin; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "prop select owner or admin" ON public.propositions FOR SELECT TO authenticated USING (((created_by = auth.uid()) OR public.is_admin()));


--
-- Name: propositions prop update admin only; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "prop update admin only" ON public.propositions FOR UPDATE TO authenticated USING (public.is_admin()) WITH CHECK (public.is_admin());


--
-- Name: proposition_items prop_items select by owner or admin; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "prop_items select by owner or admin" ON public.proposition_items FOR SELECT TO authenticated USING ((EXISTS ( SELECT 1
   FROM public.propositions p
  WHERE ((p.id = proposition_items.proposition_id) AND ((p.created_by = auth.uid()) OR public.is_admin())))));


--
-- Name: proposition_items prop_items write admin only; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "prop_items write admin only" ON public.proposition_items TO authenticated USING (public.is_admin()) WITH CHECK (public.is_admin());


--
-- Name: proposition_items; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.proposition_items ENABLE ROW LEVEL SECURITY;

--
-- Name: propositions; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.propositions ENABLE ROW LEVEL SECURITY;

--
-- Name: categories public read categories; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read categories" ON public.categories FOR SELECT TO anon USING (true);


--
-- Name: disponibilites public read disponibilites via parent publie; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read disponibilites via parent publie" ON public.disponibilites FOR SELECT TO anon USING ((EXISTS ( SELECT 1
   FROM public.etablissements e
  WHERE ((e.id = disponibilites.etablissement_id) AND (e.statut_editorial = 'publie'::public.statut_editorial)))));


--
-- Name: etablissement_service public read etab_service via parent publie; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read etab_service via parent publie" ON public.etablissement_service FOR SELECT TO anon USING ((EXISTS ( SELECT 1
   FROM public.etablissements e
  WHERE ((e.id = etablissement_service.etablissement_id) AND (e.statut_editorial = 'publie'::public.statut_editorial)))));


--
-- Name: etablissement_sous_categorie public read etab_sous_cat via parent publie; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read etab_sous_cat via parent publie" ON public.etablissement_sous_categorie FOR SELECT TO anon USING ((EXISTS ( SELECT 1
   FROM public.etablissements e
  WHERE ((e.id = etablissement_sous_categorie.etablissement_id) AND (e.statut_editorial = 'publie'::public.statut_editorial)))));


--
-- Name: etablissements public read etablissements publies; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read etablissements publies" ON public.etablissements FOR SELECT TO anon USING ((statut_editorial = 'publie'::public.statut_editorial));


--
-- Name: logements_types public read logements_types via parent publie; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read logements_types via parent publie" ON public.logements_types FOR SELECT TO anon USING ((EXISTS ( SELECT 1
   FROM public.etablissements e
  WHERE ((e.id = logements_types.etablissement_id) AND (e.statut_editorial = 'publie'::public.statut_editorial)))));


--
-- Name: medias public read medias via parent publie; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read medias via parent publie" ON public.medias FOR SELECT TO anon USING ((EXISTS ( SELECT 1
   FROM public.etablissements e
  WHERE ((e.id = medias.etablissement_id) AND (e.statut_editorial = 'publie'::public.statut_editorial)))));


--
-- Name: services public read services; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read services" ON public.services FOR SELECT TO anon USING (true);


--
-- Name: sous_categories public read sous_categories; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read sous_categories" ON public.sous_categories FOR SELECT TO anon USING (true);


--
-- Name: tarifications public read tarifications via parent publie; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "public read tarifications via parent publie" ON public.tarifications FOR SELECT TO anon USING ((EXISTS ( SELECT 1
   FROM public.etablissements e
  WHERE ((e.id = tarifications.etablissement_id) AND (e.statut_editorial = 'publie'::public.statut_editorial)))));


--
-- Name: reclamations_propriete reclam insert authenticated; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "reclam insert authenticated" ON public.reclamations_propriete FOR INSERT TO authenticated WITH CHECK (true);


--
-- Name: reclamations_propriete reclam select owner or admin; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "reclam select owner or admin" ON public.reclamations_propriete FOR SELECT TO authenticated USING (((user_id = auth.uid()) OR public.is_admin()));


--
-- Name: reclamations_propriete reclam write admin only; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY "reclam write admin only" ON public.reclamations_propriete FOR UPDATE TO authenticated USING (public.is_admin()) WITH CHECK (public.is_admin());


--
-- Name: reclamations_propriete; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.reclamations_propriete ENABLE ROW LEVEL SECURITY;

--
-- Name: services; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.services ENABLE ROW LEVEL SECURITY;

--
-- Name: sous_categories; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.sous_categories ENABLE ROW LEVEL SECURITY;

--
-- Name: tarifications; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.tarifications ENABLE ROW LEVEL SECURITY;

--
-- PostgreSQL database dump complete
--

