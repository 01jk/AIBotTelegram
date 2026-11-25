MIGRATIONS = (
    (
        """
        CREATE TABLE runtime_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            mode TEXT NOT NULL CHECK (mode IN ('OBSERVE', 'DRAFT', 'AUTO')),
            paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
            pause_reason TEXT,
            heartbeat_at INTEGER,
            heartbeat_owner TEXT,
            CHECK ((heartbeat_at IS NULL) = (heartbeat_owner IS NULL))
        )
        """,
        """
        CREATE TABLE chats (
            account_id INTEGER NOT NULL CHECK (account_id > 0),
            chat_id INTEGER NOT NULL CHECK (chat_id != 0),
            mode TEXT NOT NULL DEFAULT 'DENY'
                CHECK (mode IN ('DENY', 'READ_ONLY', 'DRAFT_ONLY', 'AUTO_REPLY')),
            proactive_enabled INTEGER NOT NULL DEFAULT 0
                CHECK (proactive_enabled IN (0, 1)),
            CHECK (proactive_enabled = 0 OR mode = 'AUTO_REPLY'),
            PRIMARY KEY (account_id, chat_id)
        )
        """,
    ),
    (
        """
        CREATE TABLE control_updates (
            bot_id INTEGER PRIMARY KEY,
            update_id INTEGER NOT NULL,
            processed_at INTEGER NOT NULL
        )
        """,
    ),
    (
        """
        CREATE TABLE send_limits (
            account_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            value INTEGER NOT NULL CHECK (value >= 0),
            PRIMARY KEY (account_id, chat_id, name)
        )
        """,
        """
        CREATE TABLE outbound_state (
            account_id INTEGER PRIMARY KEY,
            active_chat_id INTEGER,
            active_thread_id INTEGER NOT NULL DEFAULT 0,
            active_started INTEGER NOT NULL DEFAULT 0,
            active_last INTEGER NOT NULL DEFAULT 0,
            switch_after INTEGER NOT NULL DEFAULT 0,
            blocked_until INTEGER NOT NULL DEFAULT 0,
            last_flood_at INTEGER
        )
        """,
        """
        CREATE TABLE chat_send_state (
            account_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            manual_until INTEGER NOT NULL DEFAULT 0,
            blocked_until INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (account_id, chat_id)
        )
        """,
        """
        CREATE TABLE outbound_context (
            account_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            PRIMARY KEY (account_id, chat_id, thread_id)
        )
        """,
        """
        CREATE TABLE outbox (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            dedup_key TEXT NOT NULL,
            random_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            context_id INTEGER NOT NULL,
            reply_to_id INTEGER,
            proactive INTEGER NOT NULL CHECK (proactive IN (0, 1)),
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'sending', 'sent', 'failed', 'unknown')),
            reserved_at INTEGER,
            finished_at INTEGER,
            telegram_message_id INTEGER,
            reason TEXT,
            notify_owner INTEGER NOT NULL DEFAULT 0 CHECK (notify_owner IN (0, 1)),
            UNIQUE (account_id, dedup_key),
            UNIQUE (account_id, random_id)
        )
        """,
        "CREATE INDEX outbox_queue ON outbox (account_id, status, id)",
        "CREATE INDEX outbox_budget ON outbox (account_id, reserved_at)",
        "CREATE INDEX outbox_context_key ON outbox (account_id, chat_id, thread_id, context_id)",
        """
        CREATE INDEX outbox_telegram_message ON outbox (account_id, chat_id, telegram_message_id)
        WHERE telegram_message_id IS NOT NULL
        """,
        """
        CREATE UNIQUE INDEX outbox_one_sending ON outbox (account_id)
        WHERE status = 'sending'
        """,
    ),
    (
        """
        CREATE TABLE style_sources (
            owner_id INTEGER NOT NULL,
            source_path TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            report TEXT NOT NULL,
            PRIMARY KEY (owner_id, source_path)
        )
        """,
        """
        CREATE TABLE style_examples (
            owner_id INTEGER NOT NULL,
            source_path TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            language TEXT NOT NULL,
            situation TEXT NOT NULL,
            length INTEGER NOT NULL,
            text TEXT NOT NULL,
            PRIMARY KEY (owner_id, source_path, fingerprint)
        )
        """,
        "CREATE INDEX style_select ON style_examples (owner_id, language, situation, length)",
        """
        CREATE TABLE provider_usage (
            id INTEGER PRIMARY KEY,
            attempted_at INTEGER NOT NULL,
            role TEXT NOT NULL,
            model TEXT NOT NULL,
            outcome TEXT NOT NULL DEFAULT 'reserved',
            latency_ms INTEGER,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            estimated INTEGER NOT NULL DEFAULT 1
        )
        """,
        "CREATE INDEX provider_usage_time ON provider_usage (attempted_at)",
    ),
    (
        "ALTER TABLE chats ADD COLUMN external_context INTEGER NOT NULL DEFAULT 0 CHECK (external_context IN (0, 1))",
        "ALTER TABLE chats ADD COLUMN public_username TEXT",
        "ALTER TABLE outbox ADD COLUMN memory_revision INTEGER",
        "ALTER TABLE outbox ADD COLUMN memory_recorded INTEGER NOT NULL DEFAULT 0",
        """
        CREATE TABLE chat_revisions (
            account_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, revision INTEGER NOT NULL,
            forgotten_before INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (account_id, chat_id)
        )
        """,
        """
        CREATE TABLE memory_tombstones (
            account_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL, deleted_at INTEGER NOT NULL,
            PRIMARY KEY (account_id, chat_id, message_id)
        )
        """,
        """
        CREATE TABLE topics (
            id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL,
            title TEXT NOT NULL, keywords TEXT NOT NULL, parent_id INTEGER,
            status TEXT NOT NULL CHECK (status IN ('active', 'dormant', 'closed')),
            last_at INTEGER NOT NULL
        )
        """,
        "CREATE INDEX topic_scope ON topics (account_id, chat_id, thread_id, last_at)",
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL, sender_id INTEGER, sent_at INTEGER NOT NULL, text TEXT NOT NULL,
            reply_to INTEGER, edited_at INTEGER, direction TEXT NOT NULL, origin TEXT NOT NULL,
            delivery_status TEXT NOT NULL DEFAULT 'confirmed', bot INTEGER NOT NULL DEFAULT 0,
            public INTEGER NOT NULL DEFAULT 0,
            UNIQUE (account_id, chat_id, message_id)
        )
        """,
        "CREATE INDEX memory_thread ON messages (account_id, chat_id, thread_id, message_id)",
        "CREATE INDEX memory_expiry ON messages (account_id, sent_at)",
        """
        CREATE TABLE topic_messages (
            topic_id INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
            message_row INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            PRIMARY KEY (topic_id, message_row)
        )
        """,
        "CREATE INDEX message_topics ON topic_messages (message_row)",
        """
        CREATE TABLE chat_summaries (
            topic_id INTEGER PRIMARY KEY REFERENCES topics(id) ON DELETE CASCADE,
            evidence TEXT NOT NULL, summary TEXT NOT NULL, updated_at INTEGER NOT NULL,
            analysis_kind TEXT, model_summary TEXT, model_evidence TEXT
        )
        """,
        "CREATE VIRTUAL TABLE message_search USING fts5(text, content='messages', content_rowid='id')",
        """
        CREATE TRIGGER memory_insert AFTER INSERT ON messages BEGIN
            INSERT INTO message_search(rowid, text) VALUES (new.id, new.text);
        END
        """,
        """
        CREATE TRIGGER memory_delete AFTER DELETE ON messages BEGIN
            INSERT INTO message_search(message_search, rowid, text) VALUES ('delete', old.id, old.text);
        END
        """,
        """
        CREATE TRIGGER memory_update AFTER UPDATE OF text ON messages BEGIN
            INSERT INTO message_search(message_search, rowid, text) VALUES ('delete', old.id, old.text);
            INSERT INTO message_search(rowid, text) VALUES (new.id, new.text);
        END
        """,
        """
        CREATE TABLE opportunities (
            id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, fingerprint TEXT NOT NULL,
            category TEXT NOT NULL, score INTEGER NOT NULL, details TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'new', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
            model_confidence INTEGER, assessed_at INTEGER,
            UNIQUE (account_id, fingerprint)
        )
        """,
        """
        CREATE TABLE opportunity_sources (
            opportunity_id INTEGER NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,
            message_row INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            link TEXT, score INTEGER NOT NULL, PRIMARY KEY (opportunity_id, message_row)
        )
        """,
        "CREATE INDEX opportunity_message ON opportunity_sources (message_row)",
        """
        CREATE TABLE digest_runs (
            account_id INTEGER NOT NULL, local_date TEXT NOT NULL, status TEXT NOT NULL,
            PRIMARY KEY (account_id, local_date)
        )
        """,
    ),
    (
        """
        CREATE TABLE drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            context_id INTEGER NOT NULL,
            memory_revision INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending', 'queued', 'rejected')),
            UNIQUE (account_id, chat_id, thread_id, context_id)
        )
        """,
        "CREATE INDEX drafts_account ON drafts (account_id, id)",
        "ALTER TABLE outbox ADD COLUMN approved_draft_id INTEGER",
        "CREATE UNIQUE INDEX outbox_approved_draft ON outbox (account_id, approved_draft_id) WHERE approved_draft_id IS NOT NULL",
    ),
    (
        "ALTER TABLE chats ADD COLUMN external_nonpublic INTEGER NOT NULL DEFAULT 0 CHECK (external_nonpublic IN (0, 1))",
    ),
    (
        "ALTER TABLE runtime_state ADD COLUMN provider_quota_after_id INTEGER NOT NULL DEFAULT 0 CHECK (provider_quota_after_id >= 0)",
    ),
    (
        """
        CREATE TABLE style_preferences (
            owner_id INTEGER NOT NULL,
            draft_id INTEGER NOT NULL,
            language TEXT NOT NULL,
            situation TEXT NOT NULL,
            text TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (owner_id, draft_id)
        )
        """,
    ),
    (
        "ALTER TABLE runtime_state ADD COLUMN reasoning_enabled INTEGER NOT NULL DEFAULT 0 CHECK (reasoning_enabled IN (0, 1))",
        "ALTER TABLE drafts ADD COLUMN paid_chat INTEGER NOT NULL DEFAULT 0 CHECK (paid_chat IN (0, 1))",
    ),
)
