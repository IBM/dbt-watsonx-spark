#!/bin/bash
set -e

# --- Configuration & Defaults ---
LAKEHOUSE_API_VERSION="${LAKEHOUSE_API_VERSION:-v3}"
AUTH_TOKEN=""

# --- Logging (Redirected to stderr to protect stdout variable capture) ---
log_info() { echo -e "\033[0;34m[INFO]\033[0m $1" >&2; }
log_success() { echo -e "\033[0;32m[SUCCESS]\033[0m $1" >&2; }
log_warning() { echo -e "\033[1;33m[WARNING]\033[0m $1" >&2; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $1" >&2; }

# Helper to sanitize IDs from log pollution
sanitize_id() {
    echo "$1" | sed 's/\[[^]]*\]//g' | awk '{print $NF}' | tr -d "[:space:]'"
}

# --- Infrastructure & API Helpers ---

get_auth_token() {
    if [ -n "$AUTH_TOKEN" ]; then return 0; fi
    log_info "Authenticating with Cloud Pak for Data..." >&2
    local response=$(curl -s -k -X POST "$WATSONX_HOSTNAME/icp4d-api/v1/authorize" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"$CPD_USERNAME\",\"api_key\":\"$WATSONX_APIKEY\"}")
    AUTH_TOKEN=$(echo "$response" | jq -r '.token // empty')
    if [ -z "$AUTH_TOKEN" ]; then log_error "Auth failed"; exit 1; fi
}

get_base_url() { echo "$WATSONX_HOSTNAME"; }
get_auth_header() { echo "Authorization: Bearer $AUTH_TOKEN"; }

get_engine_id_by_name() {
    local response=$(curl -s -k -X GET "$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines" \
        -H "$(get_auth_header)" -H "LhInstanceId: $WATSONX_INSTANCE_ID")
    echo "$response" | jq -r ".spark_engines[]? | select(.display_name == \"$1\") | .id // .engine_id // empty" | tr -d '[:space:]'
}

get_query_server_id_by_name() {
    local engine_id=$1
    local name=$2
    local response=$(curl -s -k -X GET "$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers" \
        -H "$(get_auth_header)" -H "LhInstanceId: $WATSONX_INSTANCE_ID")
    echo "$response" | jq -r ".query_servers[]? | select(.query_server_details.name == \"$name\" and (.state == \"ACTIVE\" or .state == \"RUNNING\" or .state == \"STARTING\" or .state == \"ACCEPTED\")) | .id // empty" | head -n 1 | tr -d '[:space:]'
}

create_spark_engine() {
    local engine_name=$1
    local existing_id=$(get_engine_id_by_name "$engine_name")
    if [ -n "$existing_id" ]; then log_success "Using existing engine: $existing_id" >&2; echo "$existing_id"; return 0; fi

    local vol_id=$(echo "$(get_available_volume_id "spark-engine-volume")" | tr -d '[:space:]')
    local catalogs_json="[\"$ICEBERG_CATALOG_NAME\", \"$DELTA_CATALOG_NAME\", \"$HUDI_CATALOG_NAME\"]"
    local engine_config=$(cat <<EOF
{
    "description": "Spark engine for dbt testing",
    "configuration": { "default_version": "4.0", "engine_home": { "volume_id": "$vol_id" } },
    "display_name": "$engine_name", "origin": "native", "type": "spark", "associated_catalogs": $catalogs_json
}
EOF
)
    local response=$(curl -s -k -X POST "$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines" \
        -H "$(get_auth_header)" -H "LhInstanceId: $WATSONX_INSTANCE_ID" -H "Content-Type: application/json" -d "$engine_config")
    echo "$response" | jq -r '.engine_id // .id // empty' | tr -d '[:space:]'
}

create_query_server() {
    local engine_id=$1
    local server_name=$2
    local authz_enabled=${3:-false}
    
    local existing_qs=$(get_query_server_id_by_name "$engine_id" "$server_name")
    if [ -n "$existing_qs" ]; then log_success "Found active Query Server: $existing_qs" >&2; echo "$existing_qs"; return 0; fi

    log_info "Creating Query Server: $server_name (Authz: $authz_enabled)" >&2
    local encoded_apikey=$(echo -n "$CPD_USERNAME:$WATSONX_APIKEY" | base64)
    
    # Base Conf
    local conf_str='"spark.driver.cores": "1", "spark.driver.memory": "4g", "ae.spark.executor.count": "1", "spark.hadoop.wxd.apikey": "ZenApiKey '$encoded_apikey'"'

    # Apply Authz Extension if requested
    if [ "$authz_enabled" == "true" ]; then
        conf_str+=', "spark.sql.extensions": "authz.IBMSparkACExtension"'
    fi

    local server_config="{\"query_server_details\": {\"name\": \"$server_name\", \"conf\": {$conf_str}}}"
    
    local response=$(curl -s -k -X POST "$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers" \
        -H "$(get_auth_header)" -H "LhInstanceId: $WATSONX_INSTANCE_ID" -H "Content-Type: application/json" -d "$server_config")
    echo "$response" | jq -r '.id // empty' | tr -d '[:space:]'
}

wait_for_query_server() {
    local engine_id=$1; local server_id=$2; local elapsed=0
    while [ $elapsed -lt 300 ]; do
        local response=$(curl -s -k -X GET "$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers" \
            -H "$(get_auth_header)" -H "LhInstanceId: $WATSONX_INSTANCE_ID")
        local state=$(echo "$response" | jq -r --arg sid "$server_id" '.query_servers[]? | select(.id == $sid) | .state' 2>/dev/null)
        if [[ "$state" == "RUNNING" || "$state" == "ACTIVE" ]]; then log_success "Server ready" >&2; return 0; fi
        log_info "State: ${state:-INITIALIZING} (${elapsed}s)..." >&2
        sleep 10; elapsed=$((elapsed + 10))
    done
    return 1
}

get_query_server_profile() {
    local engine_id=$1; local server_id=$2; local catalog=$3
    local api_url="$(get_base_url)/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$engine_id/query_servers/$server_id/profile"
    local response=$(curl -s -k -X GET "$api_url" -H "$(get_auth_header)" -H "LhInstanceId: $WATSONX_INSTANCE_ID")
    echo "$response" | jq -r '.profile // empty' | \
        sed "s|<wxd-schema>|wxd_schema|g; s|<wxd-catalog>|$catalog|g; s|<username>|$CPD_USERNAME|g; s|<apikey>|$WATSONX_APIKEY|g" | \
        awk '/schema: / {print; print "      auto_location: true"; next} {print}'
}

# --- Main Logic ---

main() {
    if [ -f .env ]; then source .env; else log_error "No .env found"; exit 1; fi
    get_auth_token

    # 1. Spark Engine setup (Skips if exists)
    ENGINE_ID=$(sanitize_id "$(create_spark_engine "$SPARK_ENGINE_NAME")")
    [ -z "$ENGINE_ID" ] && { log_error "Engine ID resolution failed"; exit 1; }

    # 2. Normal Query Server & Tests
    log_info "=== Phase 1: Standard Query Server Test ==="
    QS_STD_ID=$(sanitize_id "$(create_query_server "$ENGINE_ID" "dbt-standard-qs" "false")")
    wait_for_query_server "$ENGINE_ID" "$QS_STD_ID"
    
    # Update profile instead of overwriting
    mkdir -p ~/.dbt
    PROFILE_STD=$(get_query_server_profile "$ENGINE_ID" "$QS_STD_ID" "$ICEBERG_CATALOG_NAME")
    
    # Backup existing profile if it exists
    if [ -f ~/.dbt/profiles.yml ]; then
        cp ~/.dbt/profiles.yml ~/.dbt/profiles.yml.backup
        log_info "Backed up existing profile to ~/.dbt/profiles.yml.backup"
        
        # Clean up any incorrectly named 'profile_name:' entries from previous runs
        if grep -q "^profile_name:" ~/.dbt/profiles.yml 2>/dev/null; then
            log_info "Cleaning up old 'profile_name:' entries..."
            sed -i.tmp '/^profile_name:/,/^[^ ]/{ /^profile_name:/d; /^[^ ]/!d; }' ~/.dbt/profiles.yml 2>/dev/null || true
            rm -f ~/.dbt/profiles.yml.tmp
        fi
    fi
    
    # Check if watsonx_test profile exists and remove it
    if [ -f ~/.dbt/profiles.yml ] && grep -q "^watsonx_test:" ~/.dbt/profiles.yml; then
        log_info "Updating existing watsonx_test profile..."
        sed -i.tmp '/^watsonx_test:/,/^[^ ]/{ /^watsonx_test:/d; /^[^ ]/!d; }' ~/.dbt/profiles.yml 2>/dev/null || true
        rm -f ~/.dbt/profiles.yml.tmp
    else
        log_info "Creating new watsonx_test profile..."
    fi
    
    # Append new profile with unique name (API returns 'profile_name:' as placeholder)
    echo "" >> ~/.dbt/profiles.yml
    echo "$PROFILE_STD" | sed 's/profile_name:/watsonx_test:/' >> ~/.dbt/profiles.yml
    log_success "Updated profile: watsonx_test"
    
    log_info "Running standard functional tests..."
    [ -d "$PYTHON_VENV_PATH" ] && source "$PYTHON_VENV_PATH/bin/activate"
    
    # Export environment variables for pytest to use in conftest.py
    export WATSONX_HOST="$CPD_URL"
    export WATSONX_URI="/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$ENGINE_ID/query_servers/$QS_STD_ID/connect/cliservice"
    export WATSONX_CATALOG="$ICEBERG_CATALOG_NAME"
    export WATSONX_SCHEMA="wxd_schema"
    export WATSONX_APIKEY="$WATSONX_APIKEY"
    export WATSONX_INSTANCE="$WATSONX_INSTANCE_ID"
    export WATSONX_USER="$CPD_USERNAME"
    
    pytest "tests/functional/adapter/catalog_tests/" -v --profile "watsonx_test" --tb=short || log_warning "Standard tests failed"

    # 3. Authz Query Server & Tests
    log_info "=== Phase 2: Authz Query Server Test ==="
    QS_AUTHZ_ID=$(sanitize_id "$(create_query_server "$ENGINE_ID" "dbt-authz-qs" "true")")
    wait_for_query_server "$ENGINE_ID" "$QS_AUTHZ_ID"
    
    # Check if watsonx_authz_test profile exists and remove it
    if grep -q "^watsonx_authz_test:" ~/.dbt/profiles.yml 2>/dev/null; then
        log_info "Updating existing watsonx_authz_test profile..."
        sed -i.tmp '/^watsonx_authz_test:/,/^[^ ]/{ /^watsonx_authz_test:/d; /^[^ ]/!d; }' ~/.dbt/profiles.yml 2>/dev/null || true
        rm -f ~/.dbt/profiles.yml.tmp
    else
        log_info "Creating new watsonx_authz_test profile..."
    fi
    
    PROFILE_AUTHZ=$(get_query_server_profile "$ENGINE_ID" "$QS_AUTHZ_ID" "$ICEBERG_CATALOG_NAME")
    echo "" >> ~/.dbt/profiles.yml
    # API returns 'profile_name:' as placeholder
    echo "$PROFILE_AUTHZ" | sed 's/profile_name:/watsonx_authz_test:/' >> ~/.dbt/profiles.yml
    log_success "Updated profile: watsonx_authz_test"
    
    log_info "Running Authz-specific tests..."
    
    # Update environment variables for authz query server
    export WATSONX_URI="/lakehouse/api/$LAKEHOUSE_API_VERSION/$WATSONX_INSTANCE_ID/spark_engines/$ENGINE_ID/query_servers/$QS_AUTHZ_ID/connect/cliservice"
    
    pytest "tests/functional/adapter/catalog_tests/test_issue_71459_authz.py" -v --profile "watsonx_authz_test" --tb=short || log_warning "Authz tests failed"

    log_success "Automation Complete." >&2
}

main "$@"