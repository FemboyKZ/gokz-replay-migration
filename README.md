
# GOKZ Replay Migration tool

SM Plugin to migrate GOKZ v3 and earlier replays to the new GUID based fileformat

The tool will scan your replay folders `_runs/{mapname}` and `_tempRuns/{mapname}`, along with your GOKZ database.
It will then create a GUID for any Times that are missing it, along with renaming and moving the replay file to the new location.

SM 1.12+

## Usage

### WARNING

Be aware that this tool will modify GOKZ data in your configured `"gokz"` database. (databases.cfg)

**RUN DRY FIRST!**

Make sure to test with a dry run using `sm_migrate_replays_dryrun` and report any errors or issues that occur.

It is recommended to create a backup of files and of the database before running.

You should update GOKZ to the newest version before running this. It will work while still on old GOKZ version, but it is not recommended.

1. Download the [latest release](https://github.com/FemboyKZ/gokz-replay-migration/releases/latest) and extract it in your GOKZ server's root folder (`/csgo/`).
2. Make sure the GOKZ database is still configured in `/addons/sourcemod/configs/databases.cfg`.
3. Run the plugin commands by executing the command as an admin with root permission, or directly through the server console.

After running you should confirm that changes have applied.

### Commands

* `sm_migrate_replays_dryrun`- Preview run of migration without making any changes
* `sm_migrate_replays` - Run the full migration
