package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/agent/internal/command"
)

type waitingFileBody struct {
	entered chan<- struct{}
	release <-chan struct{}
}

// Read announces an active upload and waits until the test allows it to finish.
func (body waitingFileBody) Read(_ []byte) (int, error) {
	body.entered <- struct{}{}
	<-body.release
	return 0, io.EOF
}

// TestFileRequestsRejectWhenTwoActive proves a third request cannot bypass the file capacity.
func TestFileRequestsRejectWhenTwoActive(t *testing.T) {
	handler := newFileHandler(t.TempDir())
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	stop := func() { releaseOnce.Do(func() { close(release) }) }
	defer stop()
	finished := make(chan int, 2)

	for _, name := range []string{"first", "second"} {
		request := httptest.NewRequest(http.MethodPost, "/v1/files?path="+name, waitingFileBody{entered, release})
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			finished <- response.Code
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("upload did not reach its request body")
		}
	}

	rejected := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/files?path=missing", nil))
		rejected <- response.Code
	}()
	select {
	case status := <-rejected:
		if status != http.StatusTooManyRequests {
			t.Errorf("third file request status = %d, want %d", status, http.StatusTooManyRequests)
		}
	case <-time.After(3 * time.Second):
		t.Error("third file request waited instead of rejecting immediately")
	}
	stop()
	for range 2 {
		select {
		case status := <-finished:
			if status != http.StatusCreated {
				t.Errorf("active upload status = %d, want %d", status, http.StatusCreated)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("active upload did not finish")
		}
	}
	afterRelease := httptest.NewRecorder()
	handler.ServeHTTP(afterRelease, httptest.NewRequest(http.MethodPost, "/v1/files?path=after-release", strings.NewReader("ok")))
	if afterRelease.Code != http.StatusCreated {
		t.Errorf("request after release status = %d, want %d", afterRelease.Code, http.StatusCreated)
	}
}

// TestFileUploadWriteFailureLeavesNoTarget catches a partial destination after a disk write failure.
func TestFileUploadWriteFailureLeavesNoTarget(t *testing.T) {
	if os.Getenv("AGENT_FILE_WRITE_FAILURE_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		process := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFileUploadWriteFailureLeavesNoTarget$")
		process.Env = append(os.Environ(), "AGENT_FILE_WRITE_FAILURE_CHILD=1")
		if output, err := process.CombinedOutput(); err != nil {
			t.Fatalf("isolated upload test failed: %v\n%s", err, output)
		}
		return
	}

	workspace := t.TempDir()
	signal.Ignore(syscall.SIGXFSZ)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 1024, Max: 1024}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	body := bytes.NewReader(bytes.Repeat([]byte{'x'}, 2048))
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/files?path=partial.bin", body))
	if response.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	entries, err := os.ReadDir(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("failed upload left workspace entries: %v", entries)
	}
}

type waitingFileWriter struct {
	*httptest.ResponseRecorder
	entered chan<- struct{}
	release <-chan struct{}
}

// Write keeps a download active until the test releases its response.
func (writer *waitingFileWriter) Write(data []byte) (int, error) {
	writer.entered <- struct{}{}
	<-writer.release
	return writer.ResponseRecorder.Write(data)
}

// TestFileDownloadsOccupyFileCapacity verifies downloads count against the same file limit.
func TestFileDownloadsOccupyFileCapacity(t *testing.T) {
	workspace := t.TempDir()
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(workspace, name), []byte("content"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	handler := newFileHandler(workspace)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	stop := func() { releaseOnce.Do(func() { close(release) }) }
	defer stop()
	finished := make(chan int, 2)
	for _, name := range []string{"first", "second"} {
		response := &waitingFileWriter{httptest.NewRecorder(), entered, release}
		request := httptest.NewRequest(http.MethodGet, "/v1/files?path="+name, nil)
		go func() {
			handler.ServeHTTP(response, request)
			finished <- response.Code
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("download did not reach its response writer")
		}
	}

	extra := httptest.NewRecorder()
	handler.ServeHTTP(extra, httptest.NewRequest(http.MethodPost, "/v1/files?path=extra", strings.NewReader("x")))
	if extra.Code != http.StatusTooManyRequests {
		t.Errorf("third file request status = %d, want %d", extra.Code, http.StatusTooManyRequests)
	}
	stop()
	for range 2 {
		select {
		case status := <-finished:
			if status != http.StatusOK {
				t.Errorf("download status = %d, want %d", status, http.StatusOK)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("download did not finish")
		}
	}
}

// TestFileUploadAcceptsExactSizeLimit verifies ten MiB is allowed without truncation.
func TestFileUploadAcceptsExactSizeLimit(t *testing.T) {
	workspace := t.TempDir()
	handler := newFileHandler(workspace)
	want := bytes.Repeat([]byte{'x'}, 10*1024*1024)
	upload := httptest.NewRecorder()
	handler.ServeHTTP(upload, httptest.NewRequest(http.MethodPost, "/v1/files?path=exact.bin", bytes.NewReader(want)))
	if upload.Code != http.StatusCreated {
		t.Fatalf("upload status = %d, want %d", upload.Code, http.StatusCreated)
	}
	download := httptest.NewRecorder()
	handler.ServeHTTP(download, httptest.NewRequest(http.MethodGet, "/v1/files?path=exact.bin", nil))
	if download.Code != http.StatusOK || !bytes.Equal(download.Body.Bytes(), want) {
		t.Errorf("download status = %d, length = %d; want 200 and %d bytes", download.Code, download.Body.Len(), len(want))
	}
}

// TestFileUploadInterruptedBodyLeavesNoFile verifies a failed body read cleans up any temporary data.
func TestFileUploadInterruptedBodyLeavesNoFile(t *testing.T) {
	workspace := t.TempDir()
	reader, writer := io.Pipe()
	go func() {
		_, _ = writer.Write([]byte("partial"))
		_ = writer.CloseWithError(errors.New("broken upload"))
	}()
	response := httptest.NewRecorder()
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/files?path=broken.bin", reader))
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	entries, err := os.ReadDir(workspace)
	if err != nil || len(entries) != 0 {
		t.Errorf("workspace entries after failed read = %v, error = %v", entries, err)
	}
}

// TestFileUploadCanceledBeforeStartCreatesNothing verifies cancellation does not publish a file.
func TestFileUploadCanceledBeforeStartCreatesNothing(t *testing.T) {
	workspace := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/files?path=canceled.bin", strings.NewReader("content")).WithContext(ctx)
	newFileHandler(workspace).ServeHTTP(httptest.NewRecorder(), request)
	entries, err := os.ReadDir(workspace)
	if err != nil || len(entries) != 0 {
		t.Errorf("workspace entries after cancel = %v, error = %v", entries, err)
	}
}

// TestFileConcurrentUploadsNeverOverwrite verifies two simultaneous uploads publish one complete file.
func TestFileConcurrentUploadsNeverOverwrite(t *testing.T) {
	workspace := t.TempDir()
	handler := newFileHandler(workspace)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan int, 2)
	for _, content := range []string{"first", "second"} {
		body := io.MultiReader(waitingFileBody{entered, release}, strings.NewReader(content))
		request := httptest.NewRequest(http.MethodPost, "/v1/files?path=shared.txt", body)
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			results <- response.Code
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("concurrent upload did not reach its body")
		}
	}
	close(release)
	statuses := make([]int, 0, 2)
	for range 2 {
		select {
		case status := <-results:
			statuses = append(statuses, status)
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent upload did not finish")
		}
	}
	first, second := statuses[0], statuses[1]
	if !((first == http.StatusCreated && second == http.StatusConflict) || (first == http.StatusConflict && second == http.StatusCreated)) {
		t.Errorf("upload statuses = %d, %d; want 201 and 409", first, second)
	}
	data, err := os.ReadFile(filepath.Join(workspace, "shared.txt"))
	if err != nil || (string(data) != "first" && string(data) != "second") {
		t.Errorf("published data = %q, error = %v", data, err)
	}
	entries, err := os.ReadDir(workspace)
	if err != nil || len(entries) != 1 {
		t.Errorf("workspace entries = %v, error = %v; want only shared.txt", entries, err)
	}
}

// TestFilePublishConflictCleansTemporaryFile verifies a competing writer keeps its content.
func TestFilePublishConflictCleansTemporaryFile(t *testing.T) {
	workspace := t.TempDir()
	reader, writer := io.Pipe()
	go func() {
		_, _ = writer.Write([]byte("upload"))
		err := os.WriteFile(filepath.Join(workspace, "target.txt"), []byte("existing"), 0o600)
		_ = writer.CloseWithError(err)
	}()
	response := httptest.NewRecorder()
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/files?path=target.txt", reader))
	if response.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", response.Code, http.StatusConflict)
	}
	data, err := os.ReadFile(filepath.Join(workspace, "target.txt"))
	if err != nil || string(data) != "existing" {
		t.Errorf("competing writer content = %q, error = %v", data, err)
	}
	entries, err := os.ReadDir(workspace)
	if err != nil || len(entries) != 1 {
		t.Errorf("workspace entries = %v, error = %v; want only target.txt", entries, err)
	}
}

// TestFileUploadAndDownloadPreserveBinaryBytes verifies raw bytes round-trip without conversion.
func TestFileUploadAndDownloadPreserveBinaryBytes(t *testing.T) {
	workspace := t.TempDir()
	handler := newFileHandler(workspace)
	want := []byte{'a', 0, '\n', 0xff, 'z'}
	upload := httptest.NewRecorder()
	handler.ServeHTTP(upload, httptest.NewRequest(http.MethodPost, "/v1/files?path=data.bin", bytes.NewReader(want)))
	if upload.Code != http.StatusCreated {
		t.Fatalf("upload status = %d, want %d: %s", upload.Code, http.StatusCreated, upload.Body.String())
	}

	download := httptest.NewRecorder()
	handler.ServeHTTP(download, httptest.NewRequest(http.MethodGet, "/v1/files?path=data.bin", nil))
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d: %s", download.Code, http.StatusOK, download.Body.String())
	}
	if got := download.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream", got)
	}
	if !bytes.Equal(download.Body.Bytes(), want) {
		t.Errorf("download = %v, want %v", download.Body.Bytes(), want)
	}
}

// TestFileUploadRejectsExistingFile verifies a second upload cannot overwrite workspace data.
func TestFileUploadRejectsExistingFile(t *testing.T) {
	workspace := t.TempDir()
	path := filepath.Join(workspace, "existing.txt")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/files?path=existing.txt", strings.NewReader("replacement")))
	if response.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", response.Code, http.StatusConflict)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "original" {
		t.Errorf("existing content = %q, error = %v; want original", got, err)
	}
}

// TestFileUploadEnforcesTenMiBLimit verifies oversized requests create no destination file.
func TestFileUploadEnforcesTenMiBLimit(t *testing.T) {
	workspace := t.TempDir()
	response := httptest.NewRecorder()
	body := io.LimitReader(strings.NewReader(strings.Repeat("x", 10*1024*1024+1)), 10*1024*1024+1)
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/files?path=too-large.bin", body))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	if _, err := os.Stat(filepath.Join(workspace, "too-large.bin")); !os.IsNotExist(err) {
		t.Errorf("oversized upload created file: %v", err)
	}
}

// TestFileDownloadEnforcesTenMiBLimit verifies a command-created oversized file is not served.
func TestFileDownloadEnforcesTenMiBLimit(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "large.bin"), bytes.Repeat([]byte{'x'}, 10*1024*1024+1), 0o600); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/files?path=large.bin", nil))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}

// TestFileDownloadReturnsNotFound verifies missing workspace files map to HTTP 404.
func TestFileDownloadReturnsNotFound(t *testing.T) {
	response := httptest.NewRecorder()
	newFileHandler(t.TempDir()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/files?path=missing.txt", nil))
	if response.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

// TestFilePathsRejectTraversal verifies both methods reject lexical escapes and trailing slashes.
func TestFilePathsRejectTraversal(t *testing.T) {
	handler := newFileHandler(t.TempDir())
	for _, path := range []string{"../secret", "a/../secret", "/etc/passwd", "link/", "", "."} {
		for _, method := range []string{http.MethodPost, http.MethodGet} {
			t.Run(method+"_"+path, func(t *testing.T) {
				response := httptest.NewRecorder()
				request := httptest.NewRequest(method, "/v1/files?path="+path, strings.NewReader("x"))
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusBadRequest {
					t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
				}
			})
		}
	}
}

// TestFilePathsRejectSymlinkEscape verifies links cannot read or create outside workspace.
func TestFilePathsRejectSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	handler := newFileHandler(workspace)
	for _, test := range []struct{ method, path string }{
		{http.MethodGet, "escape/secret.txt"},
		{http.MethodPost, "escape/new.txt"},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, "/v1/files?path="+test.path, strings.NewReader("injected")))
		if response.Code < 400 {
			t.Errorf("%s %s status = %d, want failure", test.method, test.path, response.Code)
		}
		if bytes.Contains(response.Body.Bytes(), []byte("secret")) {
			t.Errorf("%s %s leaked outside file", test.method, test.path)
		}
	}
	if _, err := os.Stat(filepath.Join(outside, "new.txt")); !os.IsNotExist(err) {
		t.Errorf("upload escaped workspace: %v", err)
	}
}

// TestFileDownloadRejectsDirectory verifies only regular files may be returned.
func TestFileDownloadRejectsDirectory(t *testing.T) {
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "folder"), 0o700); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	newFileHandler(workspace).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/files?path=folder", nil))
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

// TestFileCommandProcessingCanBeDownloaded verifies command output shares the workspace.
func TestFileCommandProcessingCanBeDownloaded(t *testing.T) {
	workspace := t.TempDir()
	handler := newFileHandler(workspace)
	upload := httptest.NewRecorder()
	handler.ServeHTTP(upload, httptest.NewRequest(http.MethodPost, "/v1/files?path=input.txt", strings.NewReader("hello")))
	if upload.Code != http.StatusCreated {
		t.Fatalf("upload status = %d: %s", upload.Code, upload.Body.String())
	}
	result, err := command.Run(context.Background(), command.Request{Argv: []string{"sh", "-c", "tr a-z A-Z < input.txt > output.txt"}, Cwd: workspace}, nil, nil, nil)
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("command result = %+v, error = %v", result, err)
	}
	download := httptest.NewRecorder()
	handler.ServeHTTP(download, httptest.NewRequest(http.MethodGet, "/v1/files?path=output.txt", nil))
	if download.Code != http.StatusOK || download.Body.String() != "HELLO" {
		t.Errorf("download status = %d, body = %q; want 200 HELLO", download.Code, download.Body.String())
	}
}

// TestCanceledFileUploadReleasesCapacity verifies a canceled in-flight request frees its slot.
func TestCanceledFileUploadReleasesCapacity(t *testing.T) {
	workspace := t.TempDir()
	handler := newFileHandler(workspace)
	entered := make(chan struct{}, 2)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	var firstOnce, secondOnce sync.Once
	stopFirst := func() { firstOnce.Do(func() { close(firstRelease) }) }
	stopSecond := func() { secondOnce.Do(func() { close(secondRelease) }) }
	defer stopFirst()
	defer stopSecond()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstFinished := make(chan int, 1)
	secondFinished := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/files?path=canceled.txt", waitingFileBody{entered, firstRelease}).WithContext(ctx)
		handler.ServeHTTP(response, request)
		firstFinished <- response.Code
	}()
	go func() {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/files?path=other.txt", waitingFileBody{entered, secondRelease})
		handler.ServeHTTP(response, request)
		secondFinished <- response.Code
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("upload did not reach its request body")
		}
	}
	cancel()
	stopFirst()
	select {
	case <-firstFinished:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled upload did not finish")
	}
	afterCancel := httptest.NewRecorder()
	handler.ServeHTTP(afterCancel, httptest.NewRequest(http.MethodPost, "/v1/files?path=after-cancel.txt", strings.NewReader("ok")))
	if afterCancel.Code != http.StatusCreated {
		t.Errorf("status after cancel = %d, want %d", afterCancel.Code, http.StatusCreated)
	}
	if _, err := os.Stat(filepath.Join(workspace, "canceled.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("canceled upload published a file: %v", err)
	}
	stopSecond()
	select {
	case status := <-secondFinished:
		if status != http.StatusCreated {
			t.Errorf("second upload status = %d, want %d", status, http.StatusCreated)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second upload did not finish")
	}
}
