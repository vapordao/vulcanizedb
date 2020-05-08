-- +goose Up
ALTER TABLE public.storage_diff
    ADD address BYTEA;

-- +goose Down
ALTER TABLE public.storage_diff
    DROP COLUMN address;

