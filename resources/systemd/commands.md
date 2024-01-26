# -- Status -- 
Based on: https://linuxhandbook.com/create-systemd-services/

```
mkdir -p ~/.config/systemd/user
systemctl --user daemon-reload
systemctl --user enable lineagedb.service
systemctl --user restart lineagedb.service
systemctl --user start lineagedb.service
systemctl --user stop lineagedb.service
systemctl --user is-enabled lineage.service
```

# -- Status -- 
`systemctl --user -l status lineagedb.service # Outputs some logs`

-u is unit, -b is since boot, -e will be the end, -f will follow

`journalctl --user -u lineagedb.service -e -b -f`

Note
PWD of systemctl is in the root of the home dir