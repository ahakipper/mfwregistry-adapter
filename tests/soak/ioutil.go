//go:build soak
// +build soak

package soak

import (
	"encoding/json"
	"io"
	"os"
)

// Small stdlib wrappers the soak files share (kept in one place so the
// harness reads linearly).

// jsonDecode decodes one JSON document from r.
func jsonDecode(r io.Reader, out interface{}) error {
	return json.NewDecoder(r).Decode(out)
}

// soakFile is a thin append-mode file wrapper.
type soakFile struct {
	path string
	file *os.File
}

// openAppend opens path for append (created when missing).
func openAppend(path string) (*soakFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &soakFile{path: path, file: file}, nil
}

// Write appends to the file.
func (f *soakFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

// Close closes the file.
func (f *soakFile) Close() error {
	return f.file.Close()
}
