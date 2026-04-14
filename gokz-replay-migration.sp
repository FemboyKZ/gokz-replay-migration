/**
 * GOKZ Replay Migration Plugin
 *
 * Migrates old map-based replay files (from _runs/{MAP}/ and _tempRuns/{MAP}/)
 * to the new flat GUID-based structure (_runs/{GUID}.replay).
 * Also updates the Times table with a TimeGUID column.
 *
 * Replays that don't match any database entry are left untouched.
 * Works with both MySQL and SQLite databases.
 *
 * Commands:
 *   sm_migrate_replays         - Run the migration
 *   sm_migrate_replays_dryrun  - Preview without making changes
 */

#include <sourcemod>

#pragma semicolon 1
#pragma newdecls required

#define PLUGIN_VERSION "1.0.0"

// Replay binary format constants
#define RP_MAGIC_NUMBER    0x676F6B7A  // "gokz"
#define RP_FORMAT_VERSION  0x02
#define RP_FILE_EXTENSION  "replay"

// Directory paths (relative to Path_SM)
#define RP_DIRECTORY       "data/gokz-replays"
#define RP_DIRECTORY_RUNS  "data/gokz-replays/_runs"
#define RP_DIRECTORY_TEMP  "data/gokz-replays/_tempRuns"

#define REPLAY_TYPE_RUN    0
#define GUID_MAX           45

// Migration state
Database g_hDB;
ArrayList g_hFiles; // String array of replay file paths
int g_iIndex;
int g_iMigrated;
int g_iSkippedNoMatch;
int g_iSkippedDone;
int g_iErrors;
int g_iGUIDCounter;
bool g_bBusy;
bool g_bDryRun;
int g_iCaller;

public Plugin myinfo =
{
	name = "GOKZ Replay Migration Tool",
	author = "jvnipers",
	description = "Migrates GOKZ replays from map-based to GUID-based structure",
	version = PLUGIN_VERSION,
	url = "https://github.com/FemboyKZ/gokz-replay-migration"
};

public void OnPluginStart()
{
	RegAdminCmd("sm_migrate_replays", Cmd_Migrate, ADMFLAG_ROOT, "Migrate GOKZ replays from map-based to GUID-based format");
	RegAdminCmd("sm_migrate_replays_dryrun", Cmd_DryRun, ADMFLAG_ROOT, "Preview replay migration without changes");
}

public Action Cmd_Migrate(int client, int args)
{
	g_bDryRun = false;
	return BeginMigration(client);
}

public Action Cmd_DryRun(int client, int args)
{
	g_bDryRun = true;
	return BeginMigration(client);
}

Action BeginMigration(int client)
{
	if (g_bBusy)
	{
		ReplyToCommand(client, "[Replay Migration Tool] Already in progress.");
		return Plugin_Handled;
	}

	g_bBusy = true;
	g_iCaller = (client > 0) ? GetClientUserId(client) : 0;
	g_iMigrated = 0;
	g_iSkippedNoMatch = 0;
	g_iSkippedDone = 0;
	g_iErrors = 0;
	g_iGUIDCounter = 0;
	g_iIndex = 0;

	PrintToServer("[Replay Migration Tool] Starting %s...", g_bDryRun ? "DRY RUN" : "migration");

	char error[256];
	g_hDB = SQL_Connect("gokz", true, error, sizeof(error));
	if (g_hDB == null)
	{
		LogError("[Replay Migration Tool] Database connection failed: %s", error);
		PrintToServer("[Replay Migration Tool] Database connection failed: %s", error);
		g_bBusy = false;
		return Plugin_Handled;
	}

	char driver[16];
	g_hDB.Driver.GetIdentifier(driver, sizeof(driver));
	PrintToServer("[Replay Migration Tool] Connected to %s database.", driver);

	// Add TimeGUID column if it doesn't exist (error is expected if it already exists)
	g_hDB.Query(OnAlterDone, "ALTER TABLE Times ADD TimeGUID VARCHAR(255)");
	return Plugin_Handled;
}

public void OnAlterDone(Database db, DBResultSet results, const char[] error, any data)
{
	// Error is expected if column already exists - this is fine
	if (error[0] != '\0')
		PrintToServer("[Replay Migration Tool] ALTER TABLE note (likely harmless): %s", error);

	// Clean up previous file list
	delete g_hFiles;
	g_hFiles = new ArrayList(ByteCountToCells(PLATFORM_MAX_PATH));

	// Scan both _runs/ and _tempRuns/ for replay files in map subdirectories
	char path[PLATFORM_MAX_PATH];

	BuildPath(Path_SM, path, sizeof(path), RP_DIRECTORY_RUNS);
	CollectReplayFiles(path);

	BuildPath(Path_SM, path, sizeof(path), RP_DIRECTORY_TEMP);
	CollectReplayFiles(path);

	int total = g_hFiles.Length;
	if (total == 0)
	{
		PrintToServer("[Replay Migration Tool] No replay files found in _runs/ or _tempRuns/ subdirectories.");
		Finish();
		return;
	}

	PrintToServer("[Replay Migration Tool] Found %d replay files to process.", total);

	// Ensure flat target directory exists
	BuildPath(Path_SM, path, sizeof(path), RP_DIRECTORY);
	CreateDirectory(path, 511);
	BuildPath(Path_SM, path, sizeof(path), RP_DIRECTORY_RUNS);
	CreateDirectory(path, 511);

	ProcessNext();
}

// Directory scanning

void CollectReplayFiles(const char[] parentDir)
{
	if (!DirExists(parentDir))
	{
		PrintToServer("[Replay Migration Tool] Directory not found (skipping): %s", parentDir);
		return;
	}

	DirectoryListing dir = OpenDirectory(parentDir);
	if (dir == null)
		return;

	char entry[PLATFORM_MAX_PATH];
	FileType type;

	// Each subdirectory name is a map name
	while (dir.GetNext(entry, sizeof(entry), type))
	{
		if (type != FileType_Directory || entry[0] == '.')
			continue;

		char mapDir[PLATFORM_MAX_PATH];
		FormatEx(mapDir, sizeof(mapDir), "%s/%s", parentDir, entry);

		DirectoryListing sub = OpenDirectory(mapDir);
		if (sub == null)
			continue;

		char fileName[PLATFORM_MAX_PATH];
		FileType ft;
		while (sub.GetNext(fileName, sizeof(fileName), ft))
		{
			if (ft != FileType_File)
				continue;

			int len = strlen(fileName);
			if (len >= 8 && StrEqual(fileName[len - 7], ".replay"))
			{
				char fullPath[PLATFORM_MAX_PATH];
				FormatEx(fullPath, sizeof(fullPath), "%s/%s", mapDir, fileName);
				g_hFiles.PushString(fullPath);
			}
		}
		delete sub;
	}
	delete dir;
}

// Replay processing

void ProcessNext()
{
	if (g_iIndex >= g_hFiles.Length)
	{
		Finish();
		return;
	}

	if (g_iIndex > 0 && g_iIndex % 100 == 0)
		PrintToServer("[Replay Migration Tool] Progress: %d / %d ...", g_iIndex, g_hFiles.Length);

	char filePath[PLATFORM_MAX_PATH];
	g_hFiles.GetString(g_iIndex++, filePath, sizeof(filePath));

	// Read replay binary header
	File f = OpenFile(filePath, "rb");
	if (f == null)
	{
		LogError("[Replay Migration Tool] Cannot open: %s", filePath);
		g_iErrors++;
		ProcessNext();
		return;
	}

	// Validate magic number
	int magic;
	if (!f.ReadInt32(magic) || magic != RP_MAGIC_NUMBER)
	{
		delete f;
		g_iSkippedNoMatch++;
		ProcessNext();
		return;
	}

	// Validate format version
	int ver;
	if (!f.ReadInt8(ver) || ver != RP_FORMAT_VERSION)
	{
		delete f;
		g_iSkippedNoMatch++;
		ProcessNext();
		return;
	}

	// Only migrate run replays
	int replayType;
	if (!f.ReadInt8(replayType) || replayType != REPLAY_TYPE_RUN)
	{
		delete f;
		g_iSkippedNoMatch++;
		ProcessNext();
		return;
	}

	int len;

	// Skip: gokzVersion (length-prefixed string)
	if (!f.ReadInt8(len)) { delete f; g_iErrors++; ProcessNext(); return; }
	if (len > 0) f.Seek(len, SEEK_CUR);

	// Read: mapName (length-prefixed string)
	char mapName[65];
	if (!f.ReadInt8(len) || len <= 0 || len > 64)
	{
		delete f;
		g_iErrors++;
		ProcessNext();
		return;
	}
	f.ReadString(mapName, len + 1, len);
	mapName[len] = '\0';

	// Skip: mapFileSize, serverIP (2x int32)
	f.Seek(8, SEEK_CUR);

	// Read: timestamp
	int timestamp;
	f.ReadInt32(timestamp);

	// Skip: playerAlias (length-prefixed string)
	if (!f.ReadInt8(len)) { delete f; g_iErrors++; ProcessNext(); return; }
	if (len > 0) f.Seek(len, SEEK_CUR);

	// Read: playerSteamID
	int steamID;
	f.ReadInt32(steamID);

	// Read: mode, style
	int mode, style;
	f.ReadInt8(mode);
	f.ReadInt8(style);

	// Skip: sensitivity, myaw, tickrate, tickCount, weapon, knife (6x int32 = 24 bytes)
	f.Seek(24, SEEK_CUR);

	// Run header
	int timeAsInt;
	f.ReadInt32(timeAsInt);
	float runTime = view_as<float>(timeAsInt);

	int course;
	f.ReadInt8(course);

	int teleports;
	f.ReadInt32(teleports);

	delete f;

	// Convert time to milliseconds (same as GOKZ_DB_TimeFloatToInt)
	int runTimeMS = RoundFloat(runTime * 1000.0);

	// Escape map name for SQL safety
	char escapedMap[129];
	g_hDB.Escape(mapName, escapedMap, sizeof(escapedMap));

	// Query for matching Times entry, preferring rows without a GUID
	char query[1024];
	FormatEx(query, sizeof(query),
		"SELECT t.TimeID, t.TimeGUID FROM Times t \
		 JOIN MapCourses mc ON t.MapCourseID = mc.MapCourseID \
		 JOIN Maps m ON mc.MapID = m.MapID \
		 WHERE t.SteamID32 = %d \
		 AND m.Name = '%s' \
		 AND mc.Course = %d \
		 AND t.Mode = %d \
		 AND t.Style = %d \
		 AND t.RunTime = %d \
		 AND t.Teleports = %d \
		 ORDER BY CASE WHEN t.TimeGUID IS NULL OR t.TimeGUID = '' THEN 0 ELSE 1 END \
		 LIMIT 1",
		steamID, escapedMap, course, mode, style, runTimeMS, teleports);

	DataPack dp = new DataPack();
	dp.WriteString(filePath);
	dp.WriteCell(steamID);
	dp.WriteCell(timestamp);

	g_hDB.Query(OnMatchResult, query, dp);
}

public void OnMatchResult(Database db, DBResultSet results, const char[] error, DataPack dp)
{
	char filePath[PLATFORM_MAX_PATH];

	dp.Reset();
	dp.ReadString(filePath, sizeof(filePath));
	int steamID = dp.ReadCell();
	int fileTimestamp = dp.ReadCell();
	delete dp;

	if (results == null || error[0] != '\0')
	{
		LogError("[Replay Migration Tool] Query error for %s: %s", filePath, error);
		g_iErrors++;
		ProcessNext();
		return;
	}

	if (!results.FetchRow())
	{
		// No matching database entry - leave file alone
		g_iSkippedNoMatch++;
		ProcessNext();
		return;
	}

	int timeID = results.FetchInt(0);

	// Check if GUID already assigned
	char existingGUID[GUID_MAX];
	if (!results.IsFieldNull(1))
		results.FetchString(1, existingGUID, sizeof(existingGUID));

	if (existingGUID[0] != '\0')
	{
		// Already has a GUID - check if the target file exists
		char dest[PLATFORM_MAX_PATH];
		BuildPath(Path_SM, dest, sizeof(dest), "%s/%s.%s",
			RP_DIRECTORY_RUNS, existingGUID, RP_FILE_EXTENSION);

		if (FileExists(dest))
		{
			// Fully migrated already
			g_iSkippedDone++;
			ProcessNext();
			return;
		}

		// Target file missing - move it
		if (!g_bDryRun)
		{
			if (RenameFile(dest, filePath))
			{
				PrintToServer("[Replay Migration Tool] Restored TimeID %d -> %s", timeID, existingGUID);
				g_iMigrated++;
			}
			else
			{
				LogError("[Replay Migration Tool] Move failed: %s -> %s", filePath, dest);
				g_iErrors++;
			}
		}
		else
		{
			PrintToServer("[DRY] Would restore TimeID %d -> %s", timeID, existingGUID);
			g_iMigrated++;
		}

		ProcessNext();
		return;
	}

	// No GUID yet - generate one and migrate
	g_iGUIDCounter++;
	char guid[GUID_MAX];
	FormatEx(guid, sizeof(guid), "%x-%x-%x-%x-%x",
		steamID,
		fileTimestamp > 0 ? fileTimestamp : GetTime(),
		GetSysTickCount(),
		GetURandomInt(),
		g_iGUIDCounter);

	if (g_bDryRun)
	{
		PrintToServer("[DRY] TimeID %d -> %s.replay", timeID, guid);
		g_iMigrated++;
		ProcessNext();
		return;
	}

	// Update database with GUID
	char escapedGUID[GUID_MAX * 2 + 1];
	g_hDB.Escape(guid, escapedGUID, sizeof(escapedGUID));

	char query[512];
	FormatEx(query, sizeof(query),
		"UPDATE Times SET TimeGUID = '%s' WHERE TimeID = %d AND (TimeGUID IS NULL OR TimeGUID = '')",
		escapedGUID, timeID);

	DataPack udp = new DataPack();
	udp.WriteString(filePath);
	udp.WriteString(guid);
	udp.WriteCell(timeID);

	g_hDB.Query(OnUpdateDone, query, udp);
}

public void OnUpdateDone(Database db, DBResultSet results, const char[] error, DataPack dp)
{
	char filePath[PLATFORM_MAX_PATH];
	char guid[GUID_MAX];

	dp.Reset();
	dp.ReadString(filePath, sizeof(filePath));
	dp.ReadString(guid, sizeof(guid));
	int timeID = dp.ReadCell();
	delete dp;

	if (error[0] != '\0')
	{
		LogError("[Replay Migration Tool] UPDATE failed for TimeID %d: %s", timeID, error);
		g_iErrors++;
		ProcessNext();
		return;
	}

	// Move replay file to new flat location
	char dest[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, dest, sizeof(dest), "%s/%s.%s",
		RP_DIRECTORY_RUNS, guid, RP_FILE_EXTENSION);

	if (RenameFile(dest, filePath))
	{
		PrintToServer("[Replay Migration Tool] TimeID %d -> %s", timeID, guid);
		g_iMigrated++;
	}
	else
	{
		LogError("[Replay Migration Tool] Move failed: %s -> %s", filePath, dest);
		g_iErrors++;
	}

	ProcessNext();
}

// Completion

void Finish()
{
	PrintToServer("[Replay Migration Tool] %s complete.", g_bDryRun ? "Dry run" : "Migration");
	PrintToServer("Migrated:               %d", g_iMigrated);
	PrintToServer("Skipped (no match):     %d", g_iSkippedNoMatch);
	PrintToServer("Skipped (already done): %d", g_iSkippedDone);
	PrintToServer("Errors:                 %d", g_iErrors);

	if (!g_bDryRun && g_iMigrated > 0)
	{
		PrintToServer("[Replay Migration Tool] Files moved to new location. Unmatched files remain in old directories.");
	}

	// Notify ingame admins
	for (int i = 1; i <= MaxClients; i++)
	{
		if (IsClientInGame(i) && !IsFakeClient(i))
		{
			PrintToChat(i, " \x04[Replay Migration Tool]\x01 %s: \x04%d\x01 migrated, \x09%d\x01 skipped, \x02%d\x01 errors",
				g_bDryRun ? "DRY RUN" : "Done",
				g_iMigrated,
				g_iSkippedNoMatch + g_iSkippedDone,
				g_iErrors);
		}
	}

	// Notify original caller if still connected
	if (g_iCaller > 0)
	{
		int client = GetClientOfUserId(g_iCaller);
		if (client > 0 && IsClientInGame(client))
		{
			PrintToConsole(client, "[Replay Migration Tool] Complete: %d migrated, %d no-match, %d already-done, %d errors",
				g_iMigrated, g_iSkippedNoMatch, g_iSkippedDone, g_iErrors);
		}
	}

	delete g_hFiles;
	g_hFiles = null;
	g_bBusy = false;
}
