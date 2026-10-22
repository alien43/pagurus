# Contributing

Issues and pull requests are welcome. For anything that changes when a member may start or must
stop a workload, open an issue first: the safety argument in `docs/00_design.md` has to keep
holding, and the change needs a test that fails without it.

## Tests

```sh
go test ./...                     # needs an `etcd` binary on PATH (v3.6); ~6 min
go test -race -timeout 30m ./...  # on amd64 (~13 min); arm64 hosts with a 47-bit VMA can't run -race
```

The simulation tests build the conch pinned in `go.mod` (a tool dependency) and run real agents
against a real etcd with fault injection. `lab/` (see `docs/06_lab.md`) runs the live drills on a
throwaway cluster; CI runs it on every push.

Keep `go vet ./...` clean and match the surrounding code: comments explain why, tests are named for
the behaviour they pin.
