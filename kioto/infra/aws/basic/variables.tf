variable "run_id" {
  description = "Unique run id used for tags and resource names."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,46}$", var.run_id))
    error_message = "run_id must be 3-47 chars: lowercase letters, digits, and hyphen."
  }
}

variable "region" {
  description = "AWS region. Empty uses the AWS provider default."
  type        = string
  default     = ""
}

variable "availability_zone" {
  description = "AZ for both instances. Empty uses the first available AZ."
  type        = string
  default     = ""
}

variable "owner" {
  description = "Owner tag value."
  type        = string
  default     = "unknown"
}

variable "instance_type" {
  description = "EC2 instance type for victim and sender."
  type        = string
  default     = "c7i.2xlarge"
}

variable "vpc_cidr" {
  description = "VPC CIDR."
  type        = string
  default     = "10.71.0.0/16"
}

variable "subnet_cidr" {
  description = "Public subnet CIDR."
  type        = string
  default     = "10.71.1.0/24"
}

variable "benchmark_port_start" {
  description = "First TCP benchmark port allowed inside the security group."
  type        = number
  default     = 19000
}

variable "benchmark_port_end" {
  description = "Last TCP benchmark port allowed inside the security group."
  type        = number
  default     = 22000
}
