CREATE TABLE gesang_version_pins (
    owner_id uuid NOT NULL REFERENCES gesang_owners(id),
    reference_id uuid NOT NULL,
    package text NOT NULL,
    version text NOT NULL,
    digest text NOT NULL,
    PRIMARY KEY(owner_id,reference_id,package)
);
