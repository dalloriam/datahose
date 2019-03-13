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
  args = ["functions deploy \"object-store-consume\" --entry-point \"obj_consume\" --memory 128MB --region us-central1 --runtime python37 --trigger-topic \"obj-ds\" --source \"/github/workspace/components/object_store\" --project \"personal-workspace\""]
}

action "Deploy Datastore Consumer" {
  needs = ["Run Tests", "Initialize GCP"]
  uses = "actions/gcloud/cli@master"
  env = {
    PROJECT_ID = "personal-workspace"
  }
  args = ["functions deploy \"datastore-consume\" --entry-point \"ds_consume\" --memory 128MB --region us-central1 --runtime python37 --trigger-topic \"datahose-ds\" --source \"./components/datastore\" --project \"personal-workspace\""]
}

action "Deploy Bigquery Consumer" {
    needs = ["Run Tests", "Initialize GCP"]
    uses = "actions/gcloud/cli@master"
    env = {
        "PROJECT_ID" = "personal-workspace"
    }
    args = ["functions deploy \"bigquery-consume\" --entry-point \"bq_consume\" --memory 128MB --region us-central1 --runtime python37 --trigger-topic \"datahose-bq\" --source \"./components/bigquery\" --project \"personal-workspace\""]
}

action "Deploy Notifier" {
  needs = ["Run Tests", "Initialize GCP"]
  uses = "actions/gcloud/cli@master"
  env = {
    PROJECT_ID = "personal-workspace"
  }
  args = ["functions deploy \"telegram-consume\" --entry-point \"telegram_send\" --memory 128MB --region us-central1 --runtime python37 --trigger-topic \"notifications\" --source \"./components/telegram\" --project \"personal-workspace\""]
}

action "Deploy Dispatcher" {
  needs = ["Deploy Object Store", "Deploy Datastore Consumer", "Deploy Notifier", "Deploy Bigquery Consumer"]
  uses = "actions/gcloud/cli@master"
  env = {
    PROJECT_ID = "personal-workspace"
  }
  args = ["functions deploy datahose --entry-point dispatch --memory 128MB --region us-central1 --runtime python37 --trigger-http --source ./components/dispatch --project \"personal-workspace\""]
}
