resource "aws_s3_bucket" "raw" {
  bucket = "${var.bucket_name}" 
}

output "s3_bucket_name" {
   value = "${aws_s3_bucket.raw.id}"
}

output "s3_bucket_arn" {
   value = "${aws_s3_bucket.raw.arn}"
}

resource "aws_s3_bucket_acl" "raw_acl" {
  bucket = aws_s3_bucket.raw.id
  acl    = "${var.acl_value}"  
}