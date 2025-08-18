variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "ecommerce-analysis"
}

variable "ec2_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.large"
}

variable "ec2_ssh_key_name" {
  description = "SSH key name"
  type        = string
  default     = "minha-keypair-ec2"
}
