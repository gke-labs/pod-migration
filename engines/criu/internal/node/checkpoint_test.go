// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC

package node

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createTestTar(t *testing.T, entries []tar.Header) string {
	t.Helper()
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")
	f, err := os.Create(tarPath)
	if err != nil {
		t.Fatalf("create tar: %v", err)
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	defer tw.Close()

	for _, hdr := range entries {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if h.Size > 0 {
			buf := make([]byte, h.Size)
			if _, err := tw.Write(buf); err != nil {
				t.Fatalf("write tar body: %v", err)
			}
		}
	}
	return tarPath
}

func TestCheckpointTar_RejectsGNUSparse(t *testing.T) {
	headers := []tar.Header{
		{Name: "spec.dump", Mode: 0o644, Typeflag: tar.TypeReg, Size: 10},
		{Name: "config.dump", Mode: 0o644, Typeflag: tar.TypeReg, Size: 10},
		{Name: "checkpoint/pstree.img", Mode: 0o644, Typeflag: tar.TypeReg, Size: 10},
		{Name: "checkpoint/pages-1.img", Mode: 0o644, Typeflag: tar.TypeGNUSparse, Size: 10},
	}
	tarPath := createTestTar(t, headers)

	if err := validateCheckpointTarFile(tarPath); err == nil {
		t.Fatalf("validateCheckpointTarFile accepted TypeGNUSparse tar, want error")
	}

	dst := t.TempDir()
	if _, err := extractCheckpoint(tarPath, dst); err == nil {
		t.Fatalf("extractCheckpoint accepted TypeGNUSparse tar, want error")
	}
}

func writeRawTarHeader(w io.Writer, name string, typeflag byte, size int64) error {
	var blk [512]byte
	copy(blk[0:100], name)
	copy(blk[100:108], fmt.Sprintf("%07o\x00", 0o644))
	copy(blk[108:116], fmt.Sprintf("%07o\x00", 0))
	copy(blk[116:124], fmt.Sprintf("%07o\x00", 0))
	copy(blk[124:136], fmt.Sprintf("%011o\x00", size))
	copy(blk[136:148], fmt.Sprintf("%011o\x00", time.Now().Unix()))
	for i := 148; i < 156; i++ {
		blk[i] = ' '
	}
	blk[156] = typeflag
	copy(blk[257:263], "ustar\x00")
	copy(blk[263:265], "00")
	var sum int64
	for _, b := range blk {
		sum += int64(b)
	}
	copy(blk[148:156], fmt.Sprintf("%06o\x00 ", sum))
	_, err := w.Write(blk[:])
	return err
}

func TestCheckpointTar_RejectsPAXSparse(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "pax_sparse.tar")
	f, err := os.Create(tarPath)
	if err != nil {
		t.Fatalf("create tar: %v", err)
	}

	tw := tar.NewWriter(f)
	// Write standard valid headers first
	for _, name := range []string{"spec.dump", "config.dump", "checkpoint/pstree.img"} {
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Mode:     0o644,
			Typeflag: tar.TypeReg,
			Size:     5,
		}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := tw.Write([]byte("12345")); err != nil {
			t.Fatalf("write data: %v", err)
		}
	}
	if err := tw.Flush(); err != nil {
		t.Fatalf("flush tw: %v", err)
	}

	// Write raw PAX extended header 'x' directly to file
	paxPayload := []byte("24 GNU.sparse.size=1000\n")
	if err := writeRawTarHeader(f, "PaxHeaders.X/pages-1.img", tar.TypeXHeader, int64(len(paxPayload))); err != nil {
		t.Fatalf("write pax raw header: %v", err)
	}
	var paxPadded [512]byte
	copy(paxPadded[:], paxPayload)
	if _, err := f.Write(paxPadded[:]); err != nil {
		t.Fatalf("write pax payload: %v", err)
	}

	// Write raw regular file header and content
	if err := writeRawTarHeader(f, "checkpoint/pages-1.img", tar.TypeReg, 5); err != nil {
		t.Fatalf("write reg raw header: %v", err)
	}
	var regPadded [512]byte
	copy(regPadded[:], []byte("12345"))
	if _, err := f.Write(regPadded[:]); err != nil {
		t.Fatalf("write reg payload: %v", err)
	}

	// Two 512-byte zero blocks for tar EOF
	var zeroBlocks [1024]byte
	if _, err := f.Write(zeroBlocks[:]); err != nil {
		t.Fatalf("write tar eof: %v", err)
	}
	f.Close()

	if err := validateCheckpointTarFile(tarPath); err == nil {
		t.Fatalf("validateCheckpointTarFile accepted PAX GNU.sparse tar, want error")
	}

	dst := t.TempDir()
	if _, err := extractCheckpoint(tarPath, dst); err == nil {
		t.Fatalf("extractCheckpoint accepted PAX GNU.sparse tar, want error")
	}
}

func TestCheckpointTar_ValidExtract(t *testing.T) {
	headers := []tar.Header{
		{Name: "spec.dump", Mode: 0o644, Typeflag: tar.TypeReg, Size: 5},
		{Name: "config.dump", Mode: 0o644, Typeflag: tar.TypeReg, Size: 5},
		{Name: "checkpoint/pstree.img", Mode: 0o644, Typeflag: tar.TypeReg, Size: 5},
		{Name: "checkpoint/pages-1.img", Mode: 0o644, Typeflag: tar.TypeReg, Size: 5},
	}
	tarPath := createTestTar(t, headers)

	if err := validateCheckpointTarFile(tarPath); err != nil {
		t.Fatalf("validateCheckpointTarFile rejected valid tar: %v", err)
	}

	dst := t.TempDir()
	entries, err := extractCheckpoint(tarPath, dst)
	if err != nil {
		t.Fatalf("extractCheckpoint failed on valid tar: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("extractCheckpoint returned %d entries, want 4", len(entries))
	}
}
