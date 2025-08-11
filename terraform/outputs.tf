output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = aws_redshift_cluster.main.endpoint
}

output "redshift_port" {
  description = "Redshift cluster port"
  value       = aws_redshift_cluster.main.port
}

output "redshift_database" {
  description = "Redshift database name"
  value       = aws_redshift_cluster.main.database_name
}

output "redshift_username" {
  description = "Redshift master username"
  value       = aws_redshift_cluster.main.master_username
}

# EC2 Airflow Outputs
output "airflow_public_ip" {
  description = "Public IP of Airflow EC2 instance"
  value       = aws_instance.airflow.public_ip
}

output "airflow_dns" {
  description = "Public DNS of Airflow EC2 instance"
  value       = aws_instance.airflow.public_dns
}

output "airflow_web_ui" {
  description = "Airflow Web UI URL"
  value       = "http://${aws_instance.airflow.public_ip}:8080"
}

output "airflow_ssh_command" {
  description = "SSH command to connect to Airflow instance"
  value       = "ssh -i ~/.ssh/minha-keypair-ec2 ec2-user@${aws_instance.airflow.public_ip}"
}
