# Security

pagurus runs as root on every cluster member and decides where a workload may run. Two classes of
bug matter most: anything that lets **two members run a service at once**, and anything that lets
someone who can reach etcd or the agent's socket make a member start or stop workloads.

Report either privately through GitHub's **Report a vulnerability** (Security tab of
`alien43/pagurus`), not in a public issue. Include the version, the config (redact credentials) and
the sequence of events; a `pagurus audit` log of the overlap is the most useful evidence. Expect a
first answer within a week.

The documented residual gap (`docs/00_design.md` § The residual gap) is known and not a
vulnerability; a way to trigger an overlap outside it is.
