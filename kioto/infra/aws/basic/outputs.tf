output "region" {
  value = local.region_name
}

output "availability_zone" {
  value = local.az
}

output "bucket" {
  value = aws_s3_bucket.results.bucket
}

output "placement_group" {
  value = aws_placement_group.this.name
}

output "victim_instance_id" {
  value = aws_instance.victim.id
}

output "victim_private_ip" {
  value = aws_instance.victim.private_ip
}

output "sender_instance_id" {
  value = aws_instance.sender.id
}

output "sender_private_ip" {
  value = aws_instance.sender.private_ip
}
