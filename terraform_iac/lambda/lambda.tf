resource "aws_lambda_function" "lambda_fn" {
  filename      = "../src/lambda/main.py.zip"
  function_name = "${var.fn_name}"
  role          = "${var.role_arn}"
  handler       = "main.lambda_handler"
  runtime       = "python3.9"
  description   = "Glob job initialization"

  environment {
    variables = {
      GLUE_JOB_NAME = "${var.job_name}"
      BUCKET_RAW    = "${var.bucket_raw}"
      BUCKET_STAGE  = "${var.bucket_stage}"
      DATABASE_NAME = "${var.db_name}"
      TABLE_NAME    = "${var.tb_name}"
    }
  }
}

resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_fn.arn
  principal     = "s3.amazonaws.com"
  source_arn    = "${var.bucket_raw_arn}"
}

resource "aws_s3_bucket_notification" "lambda_trigger" {
  bucket = "${var.bucket_raw}"
  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_fn.arn
    events              = ["s3:ObjectCreated:*"]
    # filter_prefix       = "AWSLogs/"
    # filter_suffix       = ".log"
  }
  depends_on = [aws_lambda_permission.allow_bucket]
}