package processors

import (
	"path/filepath"
	"testing"
)

func newTestGeoIPResolver(t *testing.T) *GeoIPResolver {
	t.Helper()

	dbPath := filepath.Join("..", "..", "data", "GeoLite2-City.mmdb")
	resolver, err := NewGeoIPResolver(dbPath)
	if err != nil {
		t.Fatalf("NewGeoIPResolver(%q) error = %v", dbPath, err)
	}

	t.Cleanup(func() {
		if err := resolver.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return resolver
}

func TestGeoIPResolverResolvePublicIP(t *testing.T) {
	resolver := newTestGeoIPResolver(t)

	country, region := resolver.Resolve("81.2.69.142")

	if country != "United Kingdom" {
		t.Fatalf("country = %q, want %q", country, "GB")
	}
	if region != "England" {
		t.Fatalf("region = %q, want %q", region, "ENG")
	}
}

func TestGeoIPResolverResolveInvalidIP(t *testing.T) {
	resolver := newTestGeoIPResolver(t)

	country, region := resolver.Resolve("not-an-ip")

	if country != "" || region != "" {
		t.Fatalf("expected empty result for invalid ip, got %q/%q", country, region)
	}
}

func TestGeoIPResolverResolvePrivateIP(t *testing.T) {
	resolver := newTestGeoIPResolver(t)

	country, region := resolver.Resolve("192.168.1.1")

	if country != "" || region != "" {
		t.Fatalf("expected empty result for private ip, got %q/%q", country, region)
	}
}
