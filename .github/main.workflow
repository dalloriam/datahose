workflow "New workflow" {
  on = "push"
  resolves = ["Aggregate Requirements"]
}

action "Aggregate Requirements" {
  uses = "./actions/aggregate_requirements"
}
