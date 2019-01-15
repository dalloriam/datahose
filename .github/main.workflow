workflow "Build & Deploy" {
  on = "push"
  resolves = ["Run Tests"]
}

action "Aggregate Requirements" {
  uses = "./actions/aggregate_requirements"
}

action "Run Tests" {
  uses = "dalloriam/actions/python/pytest@master"
  needs = ["Aggregate Requirements"]
}
