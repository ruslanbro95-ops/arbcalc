package sources

import (
	"bytes"
	"io"
)

func byteReader(b []byte) io.Reader { return bytes.NewReader(b) }
