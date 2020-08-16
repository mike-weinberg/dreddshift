/*
This view is a runbook generator. 

If it returns no rows, you have no blocked transactions.

If it returns rows, read them! They're pretty readable, and they'll help you understand what's going on in your cluster.

This view was written pretty hastily so if you see issues with it, please leave an issue on the repo or post a PR!

Happy camping!
--Mike
*/
DROP VIEW IF EXISTS admin.blocked_locks;
CREATE VIEW admin.blocked_locks AS
WITH 
locks AS 
(
    SELECT  txn.xid                         AS  xid
            , locks.pid                     AS  pid
            , txn.txn_owner                 AS  user_name
            , txn.relation                  AS  relation_id
            , TRIM(namespace.nspname)       AS  relation_schema_name
            , TRIM(class.relname)           AS  relation_name
            , locks.mode                    AS  lock_mode
            , locks.granted                 AS  lock_is_granted
            , txn.lockable_object_type      AS  lockable_object_type
            , txn.txn_start                 AS  xid_start_time
            , ROUND(
                DATEDIFF(second, 
                    txn.txn_start, 
                    SYSDATE)/60,
                2)                          AS transaction_duration_minutes -- the duration of the transaction containing the lock
            , ROUND(
                DATEDIFF(
                    second,
                    recents.starttime,
                    SYSDATE)/60,
                2)                          AS  current_statement_duration_minutes 
                                                    -- the ungranted-lock wait-time 
                                                    -- of the MOST RECENT STATEMENT (presumably running)
            , recents.query AS query_preview        -- the first 300 or so characters of the sql statement
    
    FROM pg_catalog.pg_locks            locks
    JOIN pg_catalog.svv_transactions    txn         ON  locks.pid = txn.pid
                                                        AND
                                                        locks.relation = txn.relation
                                                        AND
                                                        txn.lockable_object_type IS NOT NULL
   LEFT JOIN pg_catalog.pg_class        class       ON  class.oid = txn.relation
   LEFT JOIN pg_catalog.pg_namespace    namespace   ON  namespace.oid = class.relnamespace
   LEFT JOIN pg_catalog.stv_recents     recents     ON  recents.pid = locks.pid -- in stv_recents, pid=-1 for all completed queries, so the pids that match this join are all "running", so one per session.
   WHERE  locks.pid <> pg_backend_pid()
),
-----------------------------------------------------------------------------------------------------------------------
blocked_locks AS
(
    SELECT  pid                                     AS blocked_statement_pid
            , xid                                   AS blocked_statement_xid
            , relation_id                           AS blocked_statement_relation_id
            , relation_schema_name                  AS blocked_statement_relation_schema_name
            , relation_name                         AS blocked_statement_relation_name
            , lock_mode                             AS blocked_statement_unacquired_lock_mode 
            , current_statement_duration_minutes    AS blocked_statement_waittime_minutes
            , query_preview                         AS blocked_statement_sql_preview
    FROM   locks
    WHERE  lock_is_granted IS FALSE
),
-----------------------------------------------------------------------------------------------------------------------
blocking_locks AS --locks which are blocking other locks
(
    SELECT
            locks.pid                               AS blocking_lock_pid
            , locks.xid                             AS blocking_lock_xid
            , locks.relation_id                     AS locked_relation_id
            , locks.relation_schema_name            AS locked_relation_schema_name
            , locks.relation_name                   AS locked_relation_name
            , locks.transaction_duration_minutes    AS blocking_lock_duration_minutes
            , locks.query_preview                   AS blocking_lock_statement_sql_preview
    
    FROM locks
    INNER JOIN blocked_locks ON  blocked_locks.blocked_statement_relation_id = locks.relation_id
                            AND blocked_locks.blocked_statement_pid <> locks.pid
    WHERE locks.lock_is_granted
    
    AND blocked_locks.blocked_statement_relation_id IS NOT NULL
    AND (
            locks.lock_mode LIKE '%Exclusive%'
            OR
            (   
     
                locks.lock_mode LIKE '%Share%'
                AND
                blocked_locks.blocked_statement_unacquired_lock_mode LIKE '%ExclusiveLock%'
                AND
                blocked_locks.blocked_statement_unacquired_lock_mode NOT LIKE '%Share%'
                /*
                the above is a bit arcane, and aws did not explain this.
                That being said, there are three documented Redshift lock types:
                  1. AccessShareLock
                  2. ShareRowExclusiveLock
                  3. AccessExclusiveLock

                In addition, the following locks can be seen (this may not be comprehensive) on redshift tables:
                  - ShareLock
                
                It's probably the case that there are additional undocumented exclusive lock types, 
                and so pattern matching is being used to get all non-shared exclusive locks, the point being to 

                Finally, the leader node runs on postgres 8.0.2, which has additional lock types
                which are typically used for somewhat more optimistic lock operations against the redshift catalog
                  - RowExclusiveLock may be used when a relation is being updated in pg_class, for example
                  - ...? hard to say what else because AWS appears to have changed the lock names from postgres,
                    so it is hard to say exactly what locks to expect or exactly what they mean. 
                    The lockinglock_model itself may have been altered, so let's not plumb any deeper.
                  - see https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-TABLES for more info
                */
            )
        )

),   
-----------------------------------------------------------------------------------------------------------------------
blocked_statements_w_blocking_transactions AS
(
    SELECT  DISTINCT
                                                                    -- blocked statement details
              blocked_locks.blocked_statement_pid                   AS blocked_statement_pid
            , blocked_locks.blocked_statement_xid                   AS blocked_statement_xid
            , blocked_locks.blocked_statement_relation_id           AS blocked_statement_relation_id
            , blocked_locks.blocked_statement_relation_schema_name  AS blocked_statement_relation_schema_name
            , blocked_locks.blocked_statement_relation_name         AS blocked_statement_relation_name
            , blocked_locks.blocked_statement_unacquired_lock_mode  AS blocked_statement_unacquired_lock_mode
            , blocked_locks.blocked_statement_waittime_minutes      AS blocked_statement_waittime_minutes
            , blocked_locks.blocked_statement_sql_preview           AS blocked_statement_sql_preview
                                                                    -- blocking lock details
            , blocking_locks.blocking_lock_pid                      AS blocking_lock_pid
            , blocking_locks.blocking_lock_xid                      AS blocking_lock_xid
            , blocking_locks.locked_relation_id                     AS exclusive_locked_relation_id
            , blocking_locks.blocking_lock_duration_minutes         AS blocking_lock_duration_minutes
            , blocking_locks.blocking_lock_statement_sql_preview    AS blocking_lock_statement_sql_preview

    FROM blocked_locks
    LEFT JOIN blocking_locks ON locked_relation_id = blocked_statement_relation_id
)
-------------------------------------------------------------------------------------------------------------------
-- main
SELECT  main.blocked_statement_pid
        , main.blocked_statement_xid
        , main.blocked_statement_waittime_minutes AS blocked_statement_lock_waittime_minutes -- time the blocked statement has been waiting for a lock on the relation in question.
        , main.blocked_statement_sql_preview
        , main.blocked_statement_unacquired_lock_mode|| ' LOCK REQUEST FOR ' 
                || main.blocked_statement_relation_schema_name
                ||'.'
                ||main.blocked_statement_relation_name
                ||' HAS BEEN WAITING FOR '
                ||main.blocked_statement_waittime_minutes::VARCHAR
                ||' MINUTES AND CANNOT BE ACQUIRED DUE TO PRE-EXISTING TRANSACTION WITH XID ' 
                ||main.blocking_lock_xid::VARCHAR
                ||' WHICH HAS BEEN RUNNING FOR '
                ||main.blocking_lock_duration_minutes::VARCHAR
                ||' MINUTES. TO KILL THAT TRANSACTION RUN THE CODE IN THE NEXT COLUMN.' AS lock_summary 
        , 'SELECT PG_TERMINATE_BACKEND('||main.blocking_lock_pid||');' AS blocking_lock_termination_sql
        , blocking_lock_statement_sql_preview
FROM blocked_statements_w_blocking_transactions main
WITH NO SCHEMA BINDING
