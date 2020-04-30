-- +goose Up
ALTER TABLE public.storage_diff
    ADD COLUMN non_canonical BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
ALTER TABLE public.storage_diff
    DROP COLUMN non_canonical;
