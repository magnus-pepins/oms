-- Slice 4: venue/broker rejects on FIX return path + executions audit row type.

ALTER TYPE reject_code ADD VALUE 'VENUE_REJECT';
ALTER TYPE reject_code ADD VALUE 'FIX_OUTBOUND_JOB_EXPIRED';

ALTER TYPE execution_exec_type ADD VALUE 'REJECT';
