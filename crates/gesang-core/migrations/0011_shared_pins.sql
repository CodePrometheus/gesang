LOCK TABLE gesang_version_pins IN ACCESS EXCLUSIVE MODE;

CREATE TABLE gesang_package_pins (
    subject jsonb NOT NULL CHECK (
        COALESCE(jsonb_typeof(subject) = 'object'
        AND subject->>'kind' IN ('owner', 'installation', 'resource')
        AND CASE subject->>'kind'
            WHEN 'owner' THEN jsonb_typeof(subject->'ownerId') = 'string'
                AND subject - ARRAY['kind', 'ownerId'] = '{}'::jsonb
            WHEN 'installation' THEN jsonb_typeof(subject->'binding') = 'object'
                AND subject - ARRAY['kind', 'binding'] = '{}'::jsonb
            WHEN 'resource' THEN jsonb_typeof(subject->'binding') = 'object'
                AND jsonb_typeof(subject->'resourceId') = 'string'
                AND subject - ARRAY['kind', 'binding', 'resourceId'] = '{}'::jsonb
            ELSE false
        END, false)
    ),
    owner_id uuid GENERATED ALWAYS AS (
        CASE WHEN subject->>'kind' = 'owner' THEN (subject->>'ownerId')::uuid END
    ) STORED REFERENCES gesang_owners(id),
    reference_id uuid NOT NULL,
    package text NOT NULL CHECK (length(package) > 0),
    version text NOT NULL CHECK (length(version) > 0),
    digest text NOT NULL CHECK (length(digest) > 0),
    PRIMARY KEY(subject, reference_id, package)
);

INSERT INTO gesang_package_pins(subject, reference_id, package, version, digest)
SELECT jsonb_build_object('kind', 'owner', 'ownerId', owner_id::text),
       reference_id, package, version, digest
FROM gesang_version_pins;

CREATE FUNCTION gesang_reject_retired_pins() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = 'version pin schema is retired; upgrade the writer';
END;
$$;

CREATE TRIGGER gesang_retired_pins
BEFORE INSERT OR UPDATE OR DELETE ON gesang_version_pins
FOR EACH ROW EXECUTE FUNCTION gesang_reject_retired_pins();
