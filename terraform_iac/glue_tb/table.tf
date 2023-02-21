locals {
  table_structure = jsondecode(file("${path.module}/tb_uber_movements_definition.json"))
  # columns   = [for column in local.table_structure.columns : column.name]
  columns   = [for column in local.table_structure.columns : column if column.partition!=true]
  partition = [for par in local.table_structure.columns: par if par.partition==true]
}

resource "aws_glue_catalog_table" "uber_tb" {
  name          = "${var.tb_name}"
  database_name = "${var.db_name}"
  table_type    = "EXTERNAL_TABLE"
  # dynamic name MUST correspond to resource field
  dynamic "partition_keys" {
    for_each = "${local.partition}"
    content {
      name = "${partition_keys.value.name}"
      type = "${partition_keys.value.type}"
    }
  }
  storage_descriptor {
    location      = "s3://${var.bucket_name}"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
        name                  = "SerDe"
        serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

        parameters = {
        "serialization.format" = 1
        }
    }
    dynamic "columns" {
      for_each = "${local.columns}"
      content {
        name    = "${columns.value.name}"
        type    = "${columns.value.type}"
        comment = "${columns.value.comment}"
      }
    }
  }
}

output "tb_name" {
  value = aws_glue_catalog_table.uber_tb.name
}