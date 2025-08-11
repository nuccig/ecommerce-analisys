variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "ecommerce-analysis"
}

variable "database_name" {
  description = "Name of the Redshift database"
  type        = string
  default     = "ecommerce_dw"
}

variable "master_username" {
  description = "Master username for Redshift cluster"
  type        = string
  default     = "admin"
}

variable "master_password" {
  description = "Master password for Redshift cluster"
  type        = string
}
