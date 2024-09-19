#!/bin/sh

# Set Vault address
export VAULT_ADDR='http://vault:8200'

# Define the file containing secrets
SECRETS_FILE="/vault/scripts/secrets.txt"

# Initialize Vault
echo "Initializing Vault..."
vault operator init > /vault/scripts/init-output.txt

# Extract the unseal keys and root token
UNSEAL_KEY_1=$(grep 'Unseal Key 1:' /vault/scripts/init-output.txt | awk '{print $NF}')
UNSEAL_KEY_2=$(grep 'Unseal Key 2:' /vault/scripts/init-output.txt | awk '{print $NF}')
UNSEAL_KEY_3=$(grep 'Unseal Key 3:' /vault/scripts/init-output.txt | awk '{print $NF}')
ROOT_TOKEN=$(grep 'Initial Root Token:' /vault/scripts/init-output.txt | awk '{print $NF}')

# Store root token for Airflow to access
echo "$ROOT_TOKEN" > /vault/shared-secrets/root-token.txt

# Unseal Vault
echo "Unsealing Vault..."
vault operator unseal $UNSEAL_KEY_1
vault operator unseal $UNSEAL_KEY_2
vault operator unseal $UNSEAL_KEY_3

# Log in to Vault
echo "Logging in to Vault..."
vault login $ROOT_TOKEN

# Enable the KV secret engine
echo "Enabling KV secret engine at path 'secret'..."
vault secrets enable -path=secret kv

# Store secrets from SECRETS_FILE
while IFS= read -r line || [[ -n "$line" ]]; do
    SECRET_PATH=$(echo "$line" | awk '{print $1}')
    SECRET_KEY=$(echo "$line" | awk -F' ' '{print $2}' | cut -d'=' -f1)
    SECRET_VALUE=$(echo "$line" | awk -F' ' '{print $2}' | cut -d'=' -f2-)

    echo "Storing secret at $SECRET_PATH with key $SECRET_KEY and value $SECRET_VALUE..."
    vault kv put $SECRET_PATH $SECRET_KEY=$SECRET_VALUE
done < $SECRETS_FILE


echo "Vault setup complete."
