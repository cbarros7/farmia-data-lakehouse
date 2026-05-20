output "resource_group_name" {
  value = azurerm_resource_group.core.name
}

output "storage_account_name" {
  value = azurerm_storage_account.adls.name
}

output "adls_dfs_endpoint" {
  value = "${azurerm_storage_account.adls.name}.dfs.core.windows.net"
}

output "adls_containers" {
  value = local.adls_containers
}

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.telemetry.name
}

output "eventhub_name" {
  value = azurerm_eventhub.telemetry.name
}

# output "databricks_workspace_url" {
#   value = azurerm_databricks_workspace.compute.workspace_url
# }

# output "databricks_workspace_id" {
#   value = azurerm_databricks_workspace.compute.id
# }
