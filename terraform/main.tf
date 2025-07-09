# DeFtunes Data Pipeline Infrastructure
# 
# This Terraform configuration deploys the complete data pipeline infrastructure
# for DeFtunes music streaming and purchase analytics platform.

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Data extraction infrastructure
module "extract_job" {
  source = "./modules/extract_job"

  project             = var.project
  environment         = var.environment
  region              = var.region
  vpc_id              = var.vpc_id
  subnet_ids          = var.subnet_ids
  
  # Database configuration
  db_host             = var.db_host
  db_port             = var.db_port
  db_name             = var.db_name
  db_username         = var.db_username
  db_password         = var.db_password
  
  # API configuration
  api_base_url        = var.api_base_url
  
  # Storage configuration
  data_lake_bucket    = var.data_lake_bucket
  scripts_bucket      = var.scripts_bucket
  
  tags = var.common_tags
}

# Data transformation infrastructure
module "transform_job" {
  source = "./modules/transform_job"

  project             = var.project
  environment         = var.environment
  region              = var.region
  
  # Dependencies from extract module
  glue_role_arn       = module.extract_job.glue_role_arn
  
  # Storage configuration
  scripts_bucket      = var.scripts_bucket
  data_lake_bucket    = var.data_lake_bucket
  
  # Catalog configuration
  catalog_database    = var.catalog_database
  users_table         = var.users_table
  sessions_table      = var.sessions_table
  songs_table         = var.songs_table
  
  tags = var.common_tags
  depends_on = [module.extract_job]
}

# Data serving layer infrastructure
module "serving" {
  source = "./modules/serving"

  project             = var.project
  environment         = var.environment
  region              = var.region
  
  # Redshift configuration
  redshift_cluster_id = var.redshift_cluster_id
  redshift_node_type  = var.redshift_node_type
  redshift_node_count = var.redshift_node_count
  redshift_database   = var.redshift_database
  redshift_username   = var.redshift_username
  redshift_password   = var.redshift_password
  
  # Catalog configuration
  catalog_database    = var.catalog_database
  
  # Networking
  vpc_id              = var.vpc_id
  subnet_ids          = var.subnet_ids
  
  tags = var.common_tags
  depends_on = [module.transform_job]
}
