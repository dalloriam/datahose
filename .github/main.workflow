workflow "Build & Deploy" {
  on = "push"
  resolves = ["Run Tests"]
}

action "Aggregate Requirements" {
  uses = "./actions/aggregate_requirements"
}

action "Run Tests" {
  uses = "./actions/run_tests"
  needs = ["Aggregate Requirements"]
}
