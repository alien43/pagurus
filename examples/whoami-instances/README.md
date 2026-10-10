# whoami, as plain Incus containers

The same workload as `../whoami`, started by the **instances** starter instead of
incus-compose. Create the container once in each member's pinned project:

```sh
pagurus provision -c pagurus.json --yes   # host-<member> groups + pgs-whoami-<member> projects
for m in asus dell rpi5; do
  incus profile device add default root disk path=/ pool=zfs-pool --project pgs-whoami-$m
  incus create docker:traefik/whoami:v1.12.0 whoami --project pgs-whoami-$m \
    -c boot.autostart=false -n incusbr0 --target $m
done
```

From then on `pagurus` only ever runs `incus start` on the member that holds the office.
