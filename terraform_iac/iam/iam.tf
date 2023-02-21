resource "aws_iam_role" "roles" {
  name = "${var.policy_name}" 
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "${var.service}"
        }
      },
    ]
  })
  inline_policy {
    name = "policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = [
            "s3:*",
            "s3-object-lambda:*",
            "glue:StartJobRun",
            "logs:CreateLogGroup",
            "logs:PutLogEvents",
            "glue:*",
            "lambda:InvokeFunction"
          ]
          Effect   = "Allow"
          Resource = [
            "*",
            "arn:aws:s3:::olmontoy-lake-stage*",
            "arn:aws:logs:us-east-1:081900802975:*",
            "arn:aws:logs:us-east-1:081900802975:log-group:/aws/lambda/*"
          ]
        },
      ]
    })
  }
}

output "role_arn" {
  value = aws_iam_role.roles.arn
}