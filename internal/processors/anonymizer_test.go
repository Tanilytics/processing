package processors

import (
	"encoding/hex"
	"testing"
)

func TestTruncateIP(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "ipv4", input: "192.168.1.123", want: "192.168.1.0"},
		{name: "ipv6", input: "2001:db8:abcd:1234:5678:9abc:def0:1234", want: "2001:db8:abcd::"},
		{name: "invalid", input: "not-an-ip", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateIP(tt.input)
			if got != tt.want {
				t.Fatalf("truncateIP(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestAnonymizeUsesTruncatedIPv4Range(t *testing.T) {
	a := NewAnonymizer("test-salt", nil)

	firstHash, firstCountry, firstRegion := a.Anonymize("192.168.1.123")
	secondHash, secondCountry, secondRegion := a.Anonymize("192.168.1.200")
	thirdHash, _, _ := a.Anonymize("192.168.2.10")

	if firstHash != secondHash {
		t.Fatalf("expected hashes from same /24 to match, got %q and %q", firstHash, secondHash)
	}
	if firstHash == thirdHash {
		t.Fatalf("expected hashes from different /24 ranges to differ, both were %q", firstHash)
	}
	if len(firstHash) != 32 {
		t.Fatalf("hash length = %d, want 32", len(firstHash))
	}
	if _, err := hex.DecodeString(firstHash); err != nil {
		t.Fatalf("hash %q is not valid hex: %v", firstHash, err)
	}
	if firstCountry != "" || firstRegion != "" || secondCountry != "" || secondRegion != "" {
		t.Fatalf("expected geo fields to be empty, got %q/%q and %q/%q", firstCountry, firstRegion, secondCountry, secondRegion)
	}
}

func TestAnonymizeIncludesSaltInHash(t *testing.T) {
	first := NewAnonymizer("salt-a", nil)
	second := NewAnonymizer("salt-b", nil)

	firstHash, _, _ := first.Anonymize("203.0.113.7")
	secondHash, _, _ := second.Anonymize("203.0.113.7")

	if firstHash == secondHash {
		t.Fatalf("expected different salts to produce different hashes, both were %q", firstHash)
	}
}
