resource "aws_s3_bucket" "ghbcokerandydrogue" {
  bucket = "ghbcokerandydrogue"

  tags = local.tags
}
