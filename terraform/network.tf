#######################
# VPC 
#######################

data "aws_vpc" "main" {
  id = "vpc-0727c9f3c43dcdf7e"
}

#######################
# IGW 
#######################
data "aws_internet_gateway" "main" {
  internet_gateway_id = "igw-0940564e697829b5b"
}

#######################
# Subnets Existentes
#######################

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.main.id]
  }

  filter {
    name   = "tag:Name"
    values = ["*public*"]
  }
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.main.id]
  }

  filter {
    name   = "tag:Name"
    values = ["*private*"]
  }
}

#######################
# Route Table
#######################
data "aws_route_table" "public" {
  vpc_id = data.aws_vpc.main.id

  filter {
    name   = "tag:Name"
    values = ["*public*"]
  }
}
