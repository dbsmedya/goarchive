# E2E harness — the test estate: config rendering, docker/database setup, and the
# per-test source and destination resets.
#
# Extracted verbatim from run-tests.sh by rc-phase-023. Behaviour unchanged.
#
# Depends on the caller having defined: TESTS_DIR, SCRIPT_DIR, MYSQL_PASS,
# SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_DB, ARCHIVE_HOST, ARCHIVE_PORT,
# ARCHIVE_USER, ARCHIVE_DB, SKIP_DOCKER. Requires lib/log.sh — except
# render_test_configs, which runs before logging is configured and so uses plain
# echo to stderr.

# Render configs/*.yaml from their tracked templates -- the same gap .env closes
# above, for the same reason. A rendered config carries the operator's real
# password, so configs/*.yaml is gitignored (tests/.gitignore:5) and only the
# .yaml.template is committed. Nothing rendered them until now: a fresh clone had
# the templates, no configs, and `make e2e` died on a missing file. The four
# configs that existed had been substituted by hand, once, and were invisible to
# everyone else.
#
# Rendered unconditionally on every run. The template is the source of truth, and
# render-if-missing would let a template edit sit un-propagated behind a stale
# local .yaml -- silent divergence, which is worse than a lost local tweak. Edit
# the template, never the .yaml.
#
# Substitution is bash parameter expansion, NOT sed: a password containing '&' or
# the sed delimiter corrupts the replacement silently. ${var//pat/rep} is bash 3.2
# safe -- macOS `make` resolves to /bin/bash 3.2.57, which has no `declare -A` and
# is the floor this suite targets.
render_test_configs() {
    local template rendered line rendered_count=0

    # An empty password renders a syntactically valid config that fails much
    # later as an unexplained MySQL authentication error. Refuse up front.
    if [ -z "${MYSQL_ROOT_PASSWORD:-}" ]; then
        echo "ERROR: MYSQL_ROOT_PASSWORD is empty or unset after sourcing $TESTS_DIR/.env." >&2
        echo "       Test configs cannot be rendered without it." >&2
        return 1
    fi

    for template in "$TESTS_DIR"/configs/*.yaml.template; do
        # With no matches the glob stays literal, so test for a real file.
        [ -e "$template" ] || continue
        rendered="${template%.template}"

        if ! {
            while IFS= read -r line || [ -n "$line" ]; do
                printf '%s\n' "${line//\$\{MYSQL_ROOT_PASSWORD\}/$MYSQL_ROOT_PASSWORD}"
            done < "$template"
        } > "$rendered"; then
            echo "ERROR: failed to render $rendered from $template" >&2
            return 1
        fi

        # Fail closed on a placeholder this function does not know how to fill,
        # rather than handing goarchive a config with a literal '${...}' in it.
        if grep -q '\${' "$rendered"; then
            echo "ERROR: $rendered still contains an unsubstituted \${...} placeholder." >&2
            echo "       render_test_configs only substitutes \${MYSQL_ROOT_PASSWORD}." >&2
            return 1
        fi

        rendered_count=$((rendered_count + 1))
    done

    if [ "$rendered_count" -eq 0 ]; then
        echo "ERROR: no config templates found in $TESTS_DIR/configs/." >&2
        echo "       The .yaml.template files are tracked in git; a clone should have them." >&2
        return 1
    fi
}

# Setup test environment
setup_environment() {
    log_step "Setting up test environment..."
    
    if [ "$SKIP_DOCKER" = false ]; then
        log_info "Stopping existing docker containers and clearing data volumes..."
        cd "$TESTS_DIR"
        # `-v` removes the db1_data/db2_data/db3_data named volumes. Without it,
        # --setup does NOT give you a clean database.
        #
        # This previously read `docker compose down` followed by
        # `rm -rf "$TESTS_DIR/docker_files/db_data"` — but the directory has
        # always been `dbdata`, not `db_data`. That rm removed a path that never
        # existed, so --setup printed "Cleaning up database data..." and cleaned
        # up nothing. It is why orphaned state survived a --setup run and
        # resurfaced as "legacy GoArchive tracking tables detected".
        docker compose down -v 2>/dev/null || true

        log_info "Starting docker containers..."
        docker compose up -d
        
        log_info "Waiting for databases to be ready..."
        sleep 10
        
        # Wait for source database
        local retries=0
        while [ $retries -lt 30 ]; do
            if mysqlsh --host="$SOURCE_HOST" --port="$SOURCE_PORT" --user="$SOURCE_USER" --password="$MYSQL_PASS" --sql -e "SELECT 1" &>/dev/null; then
                log_info "Source database is ready"
                break
            fi
            retries=$((retries + 1))
            log_info "Waiting for source database... ($retries/30)"
            sleep 2
        done
        
        if [ $retries -eq 30 ]; then
            log_error "Source database failed to start"
            exit 1
        fi
    fi
    
    # Check servers
    log_info "Checking database connections..."
    "$SCRIPT_DIR/check-servers.sh"
    
    # Load Sakila into source
    log_info "Loading Sakila database into source..."
    "$SCRIPT_DIR/get_sakila_db.sh"
    
    # Create source database and load Sakila using SQL mode
    local source_uri="$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT"
    
    log_info "Creating database '$SOURCE_DB'..."
    if ! mysqlsh --uri "$source_uri" --sql -e "CREATE DATABASE IF NOT EXISTS \`$SOURCE_DB\`;"; then
        log_error "Failed to create source database"
        exit 1
    fi
    
    log_info "Loading Sakila schema..."
    if ! mysqlsh --uri "$source_uri" --sql < "$TESTS_DIR/sakila-db/sakila-schema.sql"; then
        log_error "Failed to load Sakila schema"
        exit 1
    fi
    
    log_info "Loading Sakila data..."
    if ! mysqlsh --uri "$source_uri/sakila" --sql < "$TESTS_DIR/sakila-db/sakila-data.sql"; then
        log_error "Failed to load Sakila data"
        exit 1
    fi
    
    log_info "Sakila database loaded successfully"
    
    # Dump and load schemas
    log_info "Dumping schemas from source..."
    # Clean up old dump directory if it exists
    rm -rf /tmp/db1_schema_dump
    if ! mysqlsh --uri "$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT" --js -f "$SCRIPT_DIR/dump_master.js"; then
        log_error "Schema dump failed"
        exit 1
    fi
    
    log_info "Loading schemas into archive..."
    # Enable local_infile for util.loadDump to work
    local archive_uri="$ARCHIVE_USER:$MYSQL_PASS@$ARCHIVE_HOST:$ARCHIVE_PORT"
    mysqlsh --uri "$archive_uri" --sql -e "SET GLOBAL local_infile = 1;" 2>/dev/null || true
    # Drop existing archive database to avoid conflicts
    mysqlsh --uri "$archive_uri" --sql -e "DROP DATABASE IF EXISTS \`$ARCHIVE_DB\`;" 2>/dev/null || true
    if ! mysqlsh --uri "$archive_uri" --js -f "$SCRIPT_DIR/create_archive.js"; then
        log_error "Schema load failed"
        exit 1
    fi
    
    # Attach db3 to db1 LAST, after the source has been seeded: the replica
    # seeds itself by replaying db1's GTID history, so it must not start
    # replicating until there is something to replay.
    #
    # This fails the whole setup rather than warning. A silently non-replicating
    # db3 does not break anything visibly -- it makes the replication tests pass
    # while measuring nothing, which is worse than a stopped setup.
    log_info "Attaching replica to source..."
    if ! "$SCRIPT_DIR/setup-replication.sh"; then
        log_error "Replication setup failed"
        exit 1
    fi

    log_info "Environment setup complete!"
}

# Reset source database
reset_source_database() {
    log_info "Resetting source database..."
    
    # Drop and recreate database using JS script
    if ! mysqlsh --uri "$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT" --js -f "$SCRIPT_DIR/reset_source.js"; then
        log_error "Failed to reset source database"
        return 1
    fi
    
    # Reload Sakila schema and data using SQL mode
    local source_uri="$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT"
    log_info "Reloading Sakila schema..."
    if ! mysqlsh --uri "$source_uri" --sql < "$TESTS_DIR/sakila-db/sakila-schema.sql"; then
        log_error "Failed to reload Sakila schema"
        return 1
    fi
    
    log_info "Reloading Sakila data..."
    if ! mysqlsh --uri "$source_uri/sakila" --sql < "$TESTS_DIR/sakila-db/sakila-data.sql"; then
        log_error "Failed to reload Sakila data"
        return 1
    fi
    
    log_info "Source database reset complete"
}

# Ensure the destination database has the same schema as source. Idempotent.
# Used before running working tests that actually copy data.
ensure_destination_schema() {
    local dump_dir="${DUMP_DIR:-/tmp/db1_schema_dump}"
    log_info "Preparing destination schema at $ARCHIVE_HOST:$ARCHIVE_PORT/$ARCHIVE_DB..."

    rm -rf "$dump_dir"
    if ! mysqlsh --uri "$SOURCE_USER:$MYSQL_PASS@$SOURCE_HOST:$SOURCE_PORT" \
        --js -f "$SCRIPT_DIR/dump_master.js" > /dev/null 2>&1; then
        log_error "Failed to dump source schema"
        return 1
    fi

    local archive_uri="$ARCHIVE_USER:$MYSQL_PASS@$ARCHIVE_HOST:$ARCHIVE_PORT"
    if ! mysqlsh --uri "$archive_uri" --sql \
        -e "DROP DATABASE IF EXISTS \`$ARCHIVE_DB\`; CREATE DATABASE \`$ARCHIVE_DB\`;" > /dev/null 2>&1; then
        log_error "Failed to recreate destination database"
        return 1
    fi
    if ! mysqlsh --uri "$archive_uri" --js -f "$SCRIPT_DIR/create_archive.js" > /dev/null 2>&1; then
        log_error "Failed to load schema into destination"
        return 1
    fi
    return 0
}
