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
  value       = "http://${aws_instance.airflow.public_dns}:8080"
}

output "airflow_ssh_command" {
  description = "SSH command to connect to Airflow instance"
  value       = "ssh -i ~/.ssh/minha-keypair-ec2 ubuntu@${aws_instance.airflow.public_dns}"
}
