include "root" {
  path = find_in_parent_folders()
}
include "integrations" {
  path = "../env.hcl"
}
inputs = {
  name  = "rmwhub"
  image = "us-central1-docker.pkg.dev/cdip-78ca/gundi-integrations/base:latest"
}
