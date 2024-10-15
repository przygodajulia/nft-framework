#!/bin/sh

# Set Vault address
export VAULT_ADDR='http://vault:8200'

# Define the file containing secrets
SECRETS_FILE="/vault/scripts/secrets.txt"

# Log in to Vault using the stored root token
ROOT_TOKEN=$(cat /vault/shared-secrets/root-token.txt)

# Log in to Vault
echo "Logging in to Vault..."
vault login $ROOT_TOKEN

# Function to check if a secret exists
check_secret_exists() {
    SECRET_PATH=$1
    SECRET_KEY=$2

    # Use the Vault CLI to get the secret and check for the key
    vault kv get -field="$SECRET_KEY" "$SECRET_PATH" > /dev/null 2>&1
    return $?
}

# Store new secrets from SECRETS_FILE
while IFS= read -r line || [[ -n "$line" ]]; do
    SECRET_PATH=$(echo "$line" | awk '{print $1}')
    SECRET_KEY=$(echo "$line" | awk -F' ' '{print $2}' | cut -d'=' -f1)
    SECRET_VALUE=$(echo "$line" | awk -F' ' '{print $2}' | cut -d'=' -f2-)

    echo "Checking for secret at $SECRET_PATH with key $SECRET_KEY..."
    
    if check_secret_exists "$SECRET_PATH" "$SECRET_KEY"; then
        echo "Secret already exists at $SECRET_PATH with key $SECRET_KEY. Skipping..."
    else
        echo "Storing new secret at $SECRET_PATH with key $SECRET_KEY and value $SECRET_VALUE..."
        vault kv put "$SECRET_PATH" "$SECRET_KEY"="$SECRET_VALUE"
    fi
done < "$SECRETS_FILE"

echo "New secrets addition complete."
