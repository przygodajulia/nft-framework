storage "file" {
  path = "/vault/file"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = true
}

path "secret/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

ui = true