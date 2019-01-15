workflow "Build & Deploy" {
  on = "push"
  resolves = ["Deploy Dispatcher"]
}

action "Aggregate Requirements" {
  uses = "./actions/aggregate_requirements"
}

action "Initialize GCP" {
  uses = "actions/gcloud/auth@master"
  secrets = ["GCLOUD_AUTH"]
}

action "Run Tests" {
  uses = "./actions/run_tests"
  needs = ["Aggregate Requirements"]
}

action "Deploy Object Store" {
  needs = ["Run Tests", "Initialize GCP"]
  uses = "actions/gcloud/cli@master"
  env = {
    PROJECT_ID = "personal-workspace"
  }
  args = ["functions deploy \"object-store-consume\" --entry-point \"obj_consume\" --memory 128MB --region us-central1 --runtime python37 --trigger-topic \"obj-ds\" --source \"/github/workspace/components/object_store\""]
}

action "Deploy Datastore Consumer" {
  needs = ["Run Tests", "Initialize GCP"]
  uses = "actions/gcloud/cli@master"
  env = {
    PROJECT_ID = "personal-workspace"
  }
  args = ["functions deploy \"datastore-consume\" --entry-point \"ds_consume\" --memory 128MB --region us-central1 --runtime python37 --trigger-topic \"datahose-ds\" --source \"./components/datastore\" "]
}


action "Deploy Dispatcher" {
  needs = ["Deploy Object Store", "Deploy Datastore Consumer"]
  uses = "actions/gcloud/cli@master"
  env = {
    PROJECT_ID = "personal-workspace"
  }
  args = ["functions deploy datahose --entry-point dispatch --memory 128MB --region us-central1 --runtime python37 --trigger-http --source ./components/dispatch"]
}
