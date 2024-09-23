#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Add UUID pk to ti

Revision ID: f214b8cdd732
Revises: d0f1c55954fa
Create Date: 2024-08-21 12:35:01.508789

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f214b8cdd732"
down_revision = "d0f1c55954fa"
branch_labels = None
depends_on = None

# https://github.com/Betterment/postgresql-uuid-generate-v7/blob/main/uuid_generate_v7.sql
pg_uuid7_fn = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION
  uuid_generate_v7()
RETURNS
  uuid
LANGUAGE
  plpgsql
PARALLEL SAFE
AS $$
  BEGIN
      return uuid7(clock_timestamp());
  END
$$;

CREATE OR REPLACE FUNCTION
  uuid_generate_v7(p_timestamp timestamp with time zone)
RETURNS
  uuid
LANGUAGE
  plpgsql
PARALLEL SAFE
AS $$
  DECLARE
    -- The current UNIX timestamp in milliseconds
    unix_time_ms CONSTANT bytea NOT NULL DEFAULT substring(int8send((extract(epoch FROM p_timestamp) * 1000)::bigint) from 3);

    -- The buffer used to create the UUID, starting with the UNIX timestamp and followed by random bytes
    buffer                bytea NOT NULL DEFAULT unix_time_ms || gen_random_bytes(10);
  BEGIN
    -- Set most significant 4 bits of 7th byte to 7 (for UUID v7), keeping the last 4 bits unchanged
    buffer = set_byte(buffer, 6, (b'0111' || get_byte(buffer, 6)::bit(4))::bit(8)::int);

    -- Set most significant 2 bits of 9th byte to 2 (the UUID variant specified in RFC 4122), keeping the last 6 bits unchanged
    buffer = set_byte(buffer, 8, (b'10'   || get_byte(buffer, 8)::bit(6))::bit(8)::int);

    RETURN encode(buffer, 'hex');
  END
$$
;
"""
pg_uuid7_fn_drop = """
DROP FUNCTION uuid_generate_v7();
DROP FUNCTION uuid_generate_v7(timestamp with time zone);
"""


def upgrade():
    """Apply Add UUID pk to ti"""
    conn = op.get_bind()
    if conn.dialect.name != "postgresql":
        # TODO: support other DBs
        raise RuntimeError("TODO: support me!")

    server_default: sa.TextClause
    op.add_column(
        "task_instance",
        sa.Column(
            "id",
            sa.String(length=32).with_variant(postgresql.UUID(), "postgresql"),
            nullable=True,
        ),
    )
    if conn.dialect.name == "postgresql":
        op.execute(pg_uuid7_fn)
        # TODO: test this with a huge DB!
        op.execute(
            "UPDATE task_instance SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, clock_timestamp()))"
        )

    with op.batch_alter_table("task_instance") as batch_op:
        # Now we've migrated lets also delete the default
        batch_op.alter_column("id", exiting_nullable=True, nullable=False)
        batch_op.create_index(
            "idx_ti_denormalized", ["task_id", "dag_id", "run_id", "map_index"], unique=True
        )

    if conn.dialect.name == "postgresql":
        # Optomized version: use the existing index to create a new one. Makes Postgres's version easier!

        op.execute("""
            ALTER TABLE task_instance
                DROP CONSTRAINT task_instance_pkey CASCADE,
                ADD CONSTRAINT task_instance_pkey PRIMARY KEY (id)
        """)
        op.execute(pg_uuid7_fn_drop)
    else:
        # TODO: support other DBs
        raise RuntimeError("TODO: support me!")

    # ### end Alembic commands ###


def downgrade():
    """Unapply Add UUID pk to ti"""
    op.execute("ALTER TABLE task_instance DROP CONSTRAINT task_instance_pkey CASCADE")
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_primary_key("task_instance_pkey", ["task_id", "dag_id", "run_id", "map_index"])
        batch_op.drop_index("idx_ti_denormalized")
        batch_op.drop_column("id")

    # ### end Alembic commands ###
