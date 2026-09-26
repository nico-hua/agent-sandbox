package server

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	workspaceDirectory       = "/workspace"
	fileSizeLimit      int64 = 10 * 1024 * 1024
)

// newFileHandler creates the single-file upload and download endpoint for a workspace.
func newFileHandler(workspace string) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost && request.Method != http.MethodGet {
			response.Header().Set("Allow", "GET, POST")
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
			return
		}

		name, ok := fileName(request)
		if !ok {
			writeAPIError(response, http.StatusBadRequest, "invalid_path", "path must be a relative workspace file path")
			return
		}
		root, err := os.OpenRoot(workspace)
		if err != nil {
			writeAPIError(response, http.StatusInternalServerError, "file_operation_failed", "workspace is unavailable")
			return
		}
		defer root.Close()

		if request.Method == http.MethodPost {
			uploadFile(response, request, root, name)
			return
		}
		downloadFile(response, root, name)
	})
}

// fileName accepts one relative path without traversal, empty segments, or trailing slashes.
func fileName(request *http.Request) (string, bool) {
	values := request.URL.Query()["path"]
	if len(values) != 1 || !filepath.IsLocal(values[0]) || strings.ContainsRune(values[0], '\x00') {
		return "", false
	}
	for _, component := range strings.Split(values[0], "/") {
		if component == "" || component == "." || component == ".." {
			return "", false
		}
	}
	return values[0], true
}

// uploadFile reads a bounded body before exclusively creating a workspace file.
func uploadFile(response http.ResponseWriter, request *http.Request, root *os.Root, name string) {
	data, err := io.ReadAll(http.MaxBytesReader(response, request.Body, fileSizeLimit))
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeAPIError(response, http.StatusRequestEntityTooLarge, "file_too_large", "file exceeds 10485760 bytes")
		} else if request.Context().Err() == nil {
			writeAPIError(response, http.StatusBadRequest, "invalid_request", "could not read upload body")
		}
		return
	}

	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrExist):
			writeAPIError(response, http.StatusConflict, "file_exists", "file already exists")
		case errors.Is(err, os.ErrNotExist):
			writeAPIError(response, http.StatusNotFound, "file_not_found", "parent directory does not exist")
		default:
			writeAPIError(response, http.StatusForbidden, "file_access_denied", "file cannot be created in workspace")
		}
		return
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		writeAPIError(response, http.StatusInternalServerError, "file_operation_failed", "file could not be saved")
		return
	}
	response.WriteHeader(http.StatusCreated)
}

// downloadFile opens one regular workspace file and buffers at most one byte beyond the limit.
func downloadFile(response http.ResponseWriter, root *os.Root, name string) {
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeAPIError(response, http.StatusNotFound, "file_not_found", "file does not exist")
		} else {
			writeAPIError(response, http.StatusForbidden, "file_access_denied", "file cannot be read from workspace")
		}
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "file_operation_failed", "file could not be inspected")
		return
	}
	if !info.Mode().IsRegular() {
		writeAPIError(response, http.StatusBadRequest, "not_regular_file", "path must refer to a regular file")
		return
	}
	if info.Size() > fileSizeLimit {
		writeAPIError(response, http.StatusRequestEntityTooLarge, "file_too_large", "file exceeds 10485760 bytes")
		return
	}
	data, err := io.ReadAll(io.LimitReader(file, fileSizeLimit+1))
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "file_operation_failed", "file could not be read")
		return
	}
	if int64(len(data)) > fileSizeLimit {
		writeAPIError(response, http.StatusRequestEntityTooLarge, "file_too_large", "file exceeds 10485760 bytes")
		return
	}
	response.Header().Set("Content-Type", "application/octet-stream")
	response.Header().Set("Content-Length", strconv.Itoa(len(data)))
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(data)
}
