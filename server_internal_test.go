package litestream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type blockedLTXStagingFile struct {
	*os.File
	started chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (f *blockedLTXStagingFile) Sync() error {
	f.once.Do(func() { close(f.started) })
	<-f.release
	return f.File.Sync()
}

// A control client may abandon a foreground sync after its own deadline. The
// daemon must not leave that abandoned request queued behind the DB executor.
func TestServer_HandleSync_ClientCancellationCancelsQueuedSync(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	store := NewStore([]*DB{db}, CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer store.Close(t.Context())

	server := NewServer(store)
	server.SocketPath = filepath.Join(t.TempDir(), "litestream.sock")
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	// Hold the executor so the handler has reached DB.Sync() but cannot finish.
	mustAcquireSemaphore(db.execSem)
	defer db.execSem.Release(1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/sync", strings.NewReader(fmt.Sprintf(`{"path":%q}`, db.Path())))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", server.SocketPath)
	}}}
	result := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		result <- err
	}()

	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for db.SyncDiagnostic().ExecutorWaiterCount != 1 {
		select {
		case err := <-result:
			t.Fatalf("client returned before sync queued: %v", err)
		case <-deadline:
			t.Fatal("sync did not queue on DB executor")
		case <-ticker.C:
		}
	}

	cancel()
	if err := <-result; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("client error = %v, want context canceled", err)
	}

	deadline = time.After(time.Second)
	for db.SyncDiagnostic().ExecutorWaiterCount != 0 {
		select {
		case <-deadline:
			t.Fatal("canceled client left /sync queued on DB executor")
		case <-ticker.C:
		}
	}
}

// This captures the amplification after the original executor holder goes
// away: an abandoned request acquires the executor and prevents the next
// foreground sync from entering it.
func TestServer_HandleSync_ClientCancellationDoesNotBlockLaterSync(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal; CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t DEFAULT VALUES;`); err != nil {
		t.Fatal(err)
	}

	store := NewStore([]*DB{db}, CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer store.Close(t.Context())
	server := NewServer(store)
	server.SocketPath = filepath.Join(t.TempDir(), "litestream.sock")
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
		file, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		return &blockedLTXStagingFile{File: file, started: started, release: release}, nil
	}

	mustAcquireSemaphore(db.execSem)
	held := true
	defer func() {
		if held {
			db.execSem.Release(1)
		}
	}()
	client := socketClient(server.SocketPath)
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()
	first := syncRequest(t, firstCtx, db.Path())
	firstResult := doRequest(client, first)
	waitForExecutorWaiter(t, db, 1)
	cancelFirst()
	if result := <-firstResult; result.err == nil || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("first client error = %v, want context canceled", result.err)
	}
	waitForExecutorWaiter(t, db, 0)

	// Release the injected holder. On the old server, the canceled first
	// request now acquires the executor and blocks in LTX staging.
	db.execSem.Release(1)
	held = false
	select {
	case <-started:
		t.Fatal("canceled sync acquired the executor after its client returned")
	default:
	}

	// The canceled call must leave the later request the executor. Remove the
	// test-only staging block before the second request performs its real sync.
	db.openLTXFile = defaultOpenLTXFile
	secondCtx, cancelSecond := context.WithTimeout(t.Context(), time.Second)
	defer cancelSecond()
	secondResult := doRequest(client, syncRequest(t, secondCtx, db.Path()))
	second := <-secondResult
	if second.err != nil {
		t.Fatalf("second client error = %v, want successful sync", second.err)
	}
	if second.status != http.StatusOK {
		t.Fatalf("second status = %d, want %d: %s", second.status, http.StatusOK, second.body)
	}
	var response SyncResponse
	if err := json.Unmarshal(second.body, &response); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if response.TXID == 0 {
		t.Fatal("second sync did not report a local TXID")
	}
}

func TestServer_HandleSync_ServerAndRequestTimeoutCancelQueuedSync(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		cancel  func(*Server)
		minimum time.Duration
	}{
		{name: "server shutdown", body: `{"path":%q}`, cancel: func(server *Server) { server.cancel() }},
		{name: "request timeout", body: `{"path":%q,"timeout":1}`, cancel: func(*Server) {}, minimum: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := NewDB(filepath.Join(t.TempDir(), "db"))
			db.MonitorInterval = 0
			db.ShutdownSyncTimeout = 0
			db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
			db.Replica.MonitorEnabled = false
			if err := db.Open(); err != nil {
				t.Fatal(err)
			}
			store := NewStore([]*DB{db}, CompactionLevels{{Level: 0}})
			store.CompactionMonitorEnabled = false
			if err := store.Open(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer store.Close(t.Context())
			server := NewServer(store)
			server.SocketPath = filepath.Join(t.TempDir(), "litestream.sock")
			if err := server.Start(); err != nil {
				t.Fatal(err)
			}
			defer server.Close()

			mustAcquireSemaphore(db.execSem)
			defer db.execSem.Release(1)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/sync", strings.NewReader(fmt.Sprintf(tc.body, db.Path())))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			started := time.Now()
			result := doRequest(socketClient(server.SocketPath), req)
			waitForExecutorWaiter(t, db, 1)
			tc.cancel(server)
			response := <-result
			if response.err != nil {
				t.Fatalf("client error = %v, want HTTP error response", response.err)
			}
			if response.status != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d: %s", response.status, http.StatusInternalServerError, response.body)
			}
			if elapsed := time.Since(started); elapsed < tc.minimum {
				t.Fatalf("returned after %s, want at least %s", elapsed, tc.minimum)
			}
			waitForExecutorWaiter(t, db, 0)
		})
	}
}

func socketClient(socketPath string) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}}
}

func syncRequest(t *testing.T, ctx context.Context, path string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/sync", strings.NewReader(fmt.Sprintf(`{"path":%q}`, path)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

type controlResult struct {
	status int
	body   []byte
	err    error
}

func doRequest(client *http.Client, req *http.Request) <-chan controlResult {
	done := make(chan controlResult, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			done <- controlResult{err: err}
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		done <- controlResult{status: resp.StatusCode, body: body, err: readErr}
	}()
	return done
}

func waitForExecutorWaiter(t *testing.T, db *DB, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for db.SyncDiagnostic().ExecutorWaiterCount != want {
		select {
		case <-deadline:
			t.Fatalf("executor waiters = %d, want %d", db.SyncDiagnostic().ExecutorWaiterCount, want)
		case <-ticker.C:
		}
	}
}
