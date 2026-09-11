//go:build observe
// +build observe

package observe

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

// jsonUnmarshalString decodes a JSON document from a string.
func jsonUnmarshalString(raw string, out interface{}) error {
	return json.NewDecoder(bytes.NewReader([]byte(raw))).Decode(out)
}

// jsonNewDecoder builds a JSON decoder over a reader (shared helper).
func jsonNewDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}

// openAppend opens path for append (created when missing).
func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}
