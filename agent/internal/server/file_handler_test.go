package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nico-hua/agent-sandbox/agent/internal/command"
)

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
