resource "aws_glue_job" "glue_job" {
  name     = "${var.job_name}"
  role_arn = "${var.role_arn}"

  command {
    name = "pythonshell"
    python_version="3.9"
    script_location = "s3://${var.bucket_name}/artifacts/glue/main.py"
  }
}

output "job_name" {
  value = aws_glue_job.glue_job.name
}