terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~> 4.16"
    }
  }
  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
}

module "s3" {
    source      = "./s3"
    bucket_name = "olmp-lake-raw"
}

module "s3_bucket" {
    source      = "./s3"
    bucket_name = "olmp-lake-stage"
}

module "iam_lambda" {
    source      = "./iam"
    policy_name = "role-lake-exec-lambda"
    service     = "lambda.amazonaws.com"
}

module "iam_glue" {
    source      = "./iam"
    policy_name = "AWSGlueServiceRole-lake-execute"
    service     = "glue.amazonaws.com"
}

module "glue_database" {
  source  = "./glue_db"
  db_name = "db_uber"
}

module "glue_table" {
  source      = "./glue_tb"
  db_name     = split(":",module.glue_database.database_name)[1]
  tb_name     = "tb_uber_movements"
  bucket_name = module.s3_bucket.s3_bucket_name
}

resource "aws_s3_object" "glue_job_script" {
  bucket = module.s3_bucket.s3_bucket_name
  key    = "/artifacts/glue/main.py"
  source = "../src/glue_jobs/main.py"
}

resource "aws_s3_object" "lambda_script" {
  bucket = module.s3_bucket.s3_bucket_name
  key    = "/artifacts/lambda/main.py"
  source = "../src/lambda/main.py"
}

module "glue_job"{
  source      = "./glue_job"
  bucket_name = module.s3_bucket.s3_bucket_name
  role_arn    = module.iam_glue.role_arn
  job_name    = "job_uber_movements"
}

module "lambda_fn"{
  source        = "./lambda"
  role_arn      = module.iam_lambda.role_arn
  fn_name       = "initialize-job_uber_movements"
  job_name      = module.glue_job.job_name
  bucket_raw    = module.s3.s3_bucket_name
  bucket_stage  = module.s3_bucket.s3_bucket_name
  db_name       = split(":",module.glue_database.database_name)[1]
  tb_name        = module.glue_table.tb_name
  bucket_raw_arn = module.s3.s3_bucket_arn
}