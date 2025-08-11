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

variable "ec2_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.micro"
}

variable "ec2_ssh_key_name" {
  description = "SSH key name"
  type        = string
  default     = "minha-keypair-ec2"
}
