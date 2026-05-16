variable "location" {
  type        = string
  description = "Región Azure (ej. westeurope)."
  default     = "westeurope"
}

variable "storage_account_name" {
  type        = string
  description = "Nombre global único de la cuenta ADLS Gen2."
  default     = "stfarmia"

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name: 3-24 caracteres, solo minúsculas y números."
  }
}

variable "eventhub_namespace_name" {
  type    = string
  default = "eh-farmia-telemetry"
}

variable "eventhub_name" {
  type    = string
  default = "telemetry"
}

variable "databricks_workspace_name" {
  type    = string
  default = "dbw-farmia-compute"
}

variable "tags" {
  type = map(string)
  default = {
    project = "farmia"
    layer   = "unified-memory-core"
  }
}
