package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

// Exercise the #1490 snapshot path with one small database, multi-frame
// transactions, and repeated WAL resets. Each snapshot must restore the synced
// state even when a later transaction has already extended the WAL.
func TestDB_SnapshotWALBound_SmallDatabase(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("LTXHeaderFallback=%t", fallback), func(t *testing.T) {
			db, sqldb := newSnapshotBoundTestDB(t)
			ctx := t.Context()
			for round := range 6 {
				if round%2 == 1 {
					if err := db.Checkpoint(ctx, CheckpointModeTruncate); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (zeroblob(8000))`); err != nil {
					t.Fatal(err)
				}
				if err := db.Sync(ctx); err != nil {
					t.Fatal(err)
				}
				pos, err := db.Pos()
				if err != nil {
					t.Fatal(err)
				}
				var wantCount int
				if err := sqldb.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&wantCount); err != nil {
					t.Fatal(err)
				}
				if err := db.lockExec(ctx); err != nil {
					t.Fatal(err)
				}
				bound := db.syncState.lastSyncedWALOffset
				if fallback {
					db.syncState.lastSyncedWALOffset = 0
				}
				db.execSem.Release(1)
				if bound <= WALHeaderSize {
					t.Fatalf("expected synced WAL frames, bound=%d", bound)
				}

				if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (zeroblob(12000))`); err != nil {
					t.Fatal(err)
				}
				info, err := db.Snapshot(ctx)
				if err != nil {
					t.Fatalf("snapshot round %d: %v", round, err)
				}
				if info.MaxTXID != pos.TXID {
					t.Fatalf("snapshot txid=%s, want %s", info.MaxTXID, pos.TXID)
				}
				r, err := db.Replica.Client.OpenLTXFile(ctx, SnapshotLevel, 1, pos.TXID, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(r)
				closeErr := r.Close()
				if err != nil {
					t.Fatal(err)
				} else if closeErr != nil {
					t.Fatal(closeErr)
				}
				restorePath := filepath.Join(t.TempDir(), "snapshot.db")
				f, err := os.Create(restorePath)
				if err != nil {
					t.Fatal(err)
				}
				dec := ltx.NewDecoder(bytes.NewReader(data))
				err = dec.DecodeDatabaseTo(f)
				closeErr = f.Close()
				if err != nil {
					t.Fatal(err)
				} else if closeErr != nil {
					t.Fatal(closeErr)
				}
				hdr := dec.Header()
				if end := hdr.WALOffset + hdr.WALSize; end > bound {
					t.Fatalf("snapshot WAL end=%d exceeds synced bound=%d", end, bound)
				}
				restored, err := sql.Open("sqlite", restorePath)
				if err != nil {
					t.Fatal(err)
				}
				var gotCount int
				var integrity string
				countErr := restored.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&gotCount)
				integrityErr := restored.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity)
				closeErr = restored.Close()
				if countErr != nil || integrityErr != nil || closeErr != nil {
					t.Fatalf("restore: count=%v integrity=%v close=%v", countErr, integrityErr, closeErr)
				}
				if gotCount != wantCount || integrity != "ok" {
					t.Fatalf("snapshot round %d: count=%d want=%d integrity=%s", round, gotCount, wantCount, integrity)
				}
			}
		})
	}
}

func newSnapshotBoundTestDB(t *testing.T) (*DB, *sql.DB) {
	t.Helper()
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 1000000
	db.ShutdownSyncTimeout = 0
	db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sqldb.Close(); err != nil {
			t.Error(err)
		}
	})
	sqldb.SetMaxOpenConns(1)
	if _, err := sqldb.ExecContext(t.Context(), `
		PRAGMA page_size = 4096;
		PRAGMA journal_mode = wal;
		PRAGMA wal_autocheckpoint = 0;
		CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);
	`); err != nil {
		t.Fatal(err)
	}
	return db, sqldb
}
