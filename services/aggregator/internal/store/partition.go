package store

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
)

// p1 JSON preserves identity boundaries even for delimiters in source IDs.
func identity(source, ref string) []byte { b, _ := json.Marshal([2]string{source, ref}); return b }

// p1
func eventID(source, ref string) string {
	return fmt.Sprintf("hz-%x", sha256.Sum256(identity(source, ref)))
}

// p1
func partitionFor(source, ref string) int {
	h := fnv.New32a()
	_, _ = h.Write(identity(source, ref))
	return int(h.Sum32() % 3)
}
