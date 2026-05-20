# Despliegue Azure — FarmIA Unified Memory Core

## Requisitos

- Suscripción **Azure for Students** activa
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) (`az`)
- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5

## 1. Autenticación

```bash
az login
az account set --subscription "<ID_O_NOMBRE_SUSCRIPCION>"
az account show -o table
```

## 2. Aplicar infraestructura

```bash
cd farmia-data-lakehouse/infra/Terraform

cp terraform.tfvars.example terraform.tfvars
# Si "stfarmia" no está disponible, cambia storage_account_name (solo a-z, 3-24 chars).

terraform init
terraform plan
terraform apply
```

Anota los outputs (`adls_dfs_endpoint`, `databricks_workspace_url`).

## 3. Subir contratos SDD a ADLS

`--auth-mode login` exige el rol **Storage Blob Data Contributor** en la cuenta. Sin ese rol verás *You do not have the required permissions*.

### Opción A — Clave de cuenta (inmediata, sin RBAC)

Quien creó el RG con Terraform suele poder listar la clave:

```bash
RG=rg-farmia-core
STORAGE=$(terraform output -raw storage_account_name)
ACCOUNT_KEY=$(az storage account keys list -n "$STORAGE" -g "$RG" --query '[0].value' -o tsv)

az storage fs directory create \
  --account-name "$STORAGE" \
  --account-key "$ACCOUNT_KEY" \
  --file-system metadata \
  --name ingestion \
  --auth-mode key

for f in ../../specs/control_plane/*_domain.yaml; do
  az storage fs file upload \
    --account-name "$STORAGE" \
    --account-key "$ACCOUNT_KEY" \
    --file-system metadata \
    --path "ingestion/$(basename "$f")" \
    --source "$f" \
    --auth-mode key
done
```

### Opción B — RBAC (`login`)

Tras un `terraform apply` reciente (asigna el rol al usuario de `az login`). Espera 2–5 minutos y:

```bash
STORAGE=$(terraform output -raw storage_account_name)

az storage fs directory create \
  --account-name "$STORAGE" \
  --file-system metadata \
  --name ingestion \
  --auth-mode login

for f in ../../specs/control_plane/*_domain.yaml; do
  az storage fs file upload \
    --account-name "$STORAGE" \
    --file-system metadata \
    --path "ingestion/$(basename "$f")" \
    --source "$f" \
    --auth-mode login
done
```

Si el `apply` ya se hizo antes de añadir el rol en Terraform:

```bash
terraform apply   # crea azurerm_role_assignment.deployer_blob_data
```

## 4. Databricks (post-Terraform)

1. Abre `databricks_workspace_url` en el portal Azure → **Launch Workspace**.
2. Crea un **Access Connector** / credencial hacia la storage account (Settings → Cloud storage) usando la misma suscripción.
3. Monta o usa rutas `abfss://<contenedor>@<adls_dfs_endpoint>/...` según los contratos en `specs/control_plane/`.

**Nota host ADLS:** los YAML del repo usan `farmia.dfs.core.windows.net`. Tras el despliegue el host real es el output `adls_dfs_endpoint` (p. ej. `stfarmia.dfs.core.windows.net`). Sustituye `farmia` por tu `storage_account_name` en rutas `abfss://` o alinea el nombre en `terraform.tfvars` si reservas el nombre `farmia`.

## 5. Verificación rápida

```bash
az group show -n rg-farmia-core -o table
az storage account show -n $(terraform output -raw storage_account_name) -g rg-farmia-core --query "{name:name,hns:isHnsEnabled}" -o table
az eventhubs namespace show -n eh-farmia-telemetry -g rg-farmia-core --query sku -o table
```

Capture de Event Hubs escribe Avro en `landing/` (ruta bajo el filesystem homónimo).

terraform destroy \
  -target=azurerm_eventhub.telemetry \
  -target=azurerm_eventhub_namespace.telemetry \
  -target=azurerm_databricks_workspace.compute


## Destruir (opcional)

```bash
terraform destroy
```
