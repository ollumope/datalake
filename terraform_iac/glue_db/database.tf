resource "aws_glue_catalog_database" "uber_database" {
  name = "${var.db_name}"
}
output "database_name" {
   value = "${aws_glue_catalog_database.uber_database.id}"
}
