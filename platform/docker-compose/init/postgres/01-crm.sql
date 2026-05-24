-- Base CRM du lab : source du flux CDC (Debezium).
CREATE TABLE public.customers (
    id          VARCHAR(16) PRIMARY KEY,
    full_name   VARCHAR(120) NOT NULL,
    email       VARCHAR(160) NOT NULL,
    phone       VARCHAR(32),             -- exclu par Debezium (minimisation des données)
    birth_date  DATE,                    -- exclu par Debezium
    country     CHAR(2)      NOT NULL,
    segment     VARCHAR(16)  NOT NULL CHECK (segment IN ('STANDARD', 'PREMIUM', 'VIP')),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Les UPDATE doivent publier l'ancienne ligne complète pour les tombstones et l'audit.
ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (id, full_name, email, phone, birth_date, country, segment) VALUES
  ('C-0001', 'Amina Diallo',     'amina.diallo@example.com',   '+33600000001', '1988-02-14', 'FR', 'PREMIUM'),
  ('C-0002', 'Lucas Martin',     'lucas.martin@example.com',   '+33600000002', '1992-07-01', 'FR', 'STANDARD'),
  ('C-0003', 'Sofia Rossi',      'sofia.rossi@example.com',    '+39300000003', '1979-11-23', 'IT', 'VIP'),
  ('C-0004', 'Jonas Becker',     'jonas.becker@example.com',   '+49150000004', '1995-05-30', 'DE', 'STANDARD'),
  ('C-0005', 'Ines Moreau',      'ines.moreau@example.com',    '+32470000005', '1985-09-09', 'BE', 'PREMIUM'),
  ('C-0006', 'Kwame Mensah',     'kwame.mensah@example.com',   '+33600000006', '2000-01-17', 'FR', 'STANDARD');

-- Utilisateur dédié Debezium : réplication + lecture, rien d'autre (moindre privilège).
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium-lab-password';
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.customers TO debezium;
-- Publication créée à l'avance : Debezium n'a pas besoin du droit CREATE.
CREATE PUBLICATION dbz_crm_publication FOR TABLE public.customers;
