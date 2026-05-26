-- Phase B: terminal status for binary contract resolution.

ALTER TYPE order_status ADD VALUE IF NOT EXISTS 'RESOLVED';
