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
