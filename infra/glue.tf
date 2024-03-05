resource "aws_glue_job" "glue" {
  name     = "glue_role"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://${aws_s3_bucket.ghbcokerandydrogue.bucket}/example.py"
  }

  glue_version      = "3.0"
  number_of_workers = 2
  worker_type       = "Standard"

  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.ghbcokerandydrogue.bucket}/prefix/lib_A.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "VALUE_1"
    "--PARAM_2"                         = "VALUE_2"
  }
  tags = local.tags
}
