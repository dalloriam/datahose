workflow "Build & Deploy" {
  on = "push"
  resolves = ["Initialize GCP"]
}

action "Aggregate Requirements" {
  uses = "./actions/aggregate_requirements"
}

action "Initialize GCP" {
  uses = "actions/gcloud/auth@master"
  needs = ["Aggregate Requirements"]
  secrets = ["GCLOUD_AUTH"]
}

action "Run Tests" {
  uses = "./actions/run_tests"
  needs = ["Aggregate Requirements"]
}
