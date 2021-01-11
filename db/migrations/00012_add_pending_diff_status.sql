-- +goose NO TRANSACTION
-- +goose Up
ALTER TYPE public.diff_status ADD VALUE 'pending' AFTER 'new';

CREATE INDEX CONCURRENTLY storage_diff_pending_status_index
    ON public.storage_diff (status) WHERE status = 'pending';

-- +goose Down
UPDATE public.storage_diff SET status = 'new' WHERE status = 'pending';
DROP INDEX storage_diff_new_status_index;
DROP INDEX storage_diff_unrecognized_status_index;
DROP INDEX storage_diff_pending_status_index;

ALTER TABLE public.storage_diff ALTER COLUMN status DROP DEFAULT;
ALTER TABLE public.storage_diff ALTER COLUMN status TYPE VARCHAR(255);

DROP TYPE public.diff_status;
CREATE TYPE public.diff_status AS ENUM (
    'new',
    'transformed',
    'unrecognized',
    'noncanonical',
    'unwatched'
    );

ALTER TABLE public.storage_diff ALTER COLUMN status TYPE public.diff_status USING (status::diff_status);
ALTER TABLE public.storage_diff ALTER COLUMN status SET DEFAULT 'new';

CREATE INDEX CONCURRENTLY storage_diff_new_status_index
    ON public.storage_diff (status) WHERE status = 'new';
CREATE INDEX CONCURRENTLY storage_diff_unrecognized_status_index
    ON public.storage_diff (status) WHERE status = 'unrecognized';
