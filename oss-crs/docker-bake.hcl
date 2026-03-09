variable "REGISTRY" {
  default = "ghcr.io/team-atlanta"
}

variable "VERSION" {
  default = "latest"
}

function "tags" {
  params = [name]
  result = [
    "${REGISTRY}/${name}:${VERSION}",
    "${REGISTRY}/${name}:latest",
    "${name}:latest"
  ]
}

group "default" {
  targets = ["prepare"]
}

group "prepare" {
  targets = ["patch-ensemble-base"]
}

target "patch-ensemble-base" {
  context    = "."
  dockerfile = "oss-crs/base.Dockerfile"
  tags       = tags("patch-ensemble-base")
}
