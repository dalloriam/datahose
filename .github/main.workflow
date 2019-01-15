workflow "Build & Deploy" {
  on = "push"
  resolves = ["docker://github/gcloud-auth"]
}

action "Aggregate Requirements" {
  uses = "./actions/aggregate_requirements"
}

action "Run Tests" {
  uses = "./actions/run_tests"
  needs = ["Aggregate Requirements"]
}

action "docker://github/gcloud-auth" {
  uses = "docker://github/gcloud-auth"
  needs = ["Run Tests"]
  secrets = ["GCLOUD_AUTH"]
}
