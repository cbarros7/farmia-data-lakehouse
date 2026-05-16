data "azurerm_client_config" "current" {}

locals {
  resource_group_name = "rg-farmia-core"
  adls_containers     = ["landing", "bronze", "silver", "gold", "metadata", "checkpoints"]
}

resource "azurerm_resource_group" "core" {
  name     = local.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.core.name
  location                 = azurerm_resource_group.core.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lake" {
  for_each = toset(local.adls_containers)

  name               = each.value
  storage_account_id = azurerm_storage_account.adls.id
}

# Permite `az storage ... --auth-mode login` al usuario que ejecuta terraform apply.
#resource "azurerm_role_assignment" "deployer_blob_data" {
#  scope                = azurerm_storage_account.adls.id
#  role_definition_name = "Storage Blob Data Contributor"
#  principal_id         = data.azurerm_client_config.current.object_id
#}

resource "azurerm_eventhub_namespace" "telemetry" {
  name                = var.eventhub_namespace_name
  location            = azurerm_resource_group.core.location
  resource_group_name = azurerm_resource_group.core.name
  sku                 = "Standard"
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "telemetry" {
  name                = var.eventhub_name
  namespace_name      = azurerm_eventhub_namespace.telemetry.name
  resource_group_name = azurerm_resource_group.core.name
  partition_count     = 2
  message_retention   = 1

  capture_description {
    enabled             = true
    encoding            = "Avro"
    interval_in_seconds = 300
    size_limit_in_bytes = 314572800

    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      blob_container_name = "landing"
      storage_account_id  = azurerm_storage_account.adls.id
    }
  }
}

resource "azurerm_databricks_workspace" "compute" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.core.name
  location            = azurerm_resource_group.core.location
  sku                 = "premium"
  tags                = var.tags
}
