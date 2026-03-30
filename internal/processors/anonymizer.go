package processors

import (
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
)

type Anonymizer struct {
	dailySalt string // use a static salt for simplicity. will add rotation later
	geoIP     *GeoIPResolver
}

func NewAnonymizer(salt string, geoIP *GeoIPResolver) *Anonymizer {
	return &Anonymizer{dailySalt: salt, geoIP: geoIP}
}

// Anonymize truncates the IP and returns a hash + geo data.
// geo lookup is stubbed. will add MaxMind GeoIP later
func (a *Anonymizer) Anonymize(rawIP string) (ipHash, country, region string) {
	truncated := truncateIP(rawIP)

	// Geo lookup on truncated IP (before discarding)
	if a.geoIP != nil {
		country, region = a.geoIP.Resolve(truncated)
	}

	hash := sha256.Sum256([]byte(truncated + a.dailySalt))
	ipHash = fmt.Sprintf("%x", hash[:16]) // 32-char hex

	return
}

func truncateIP(ip string) string {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ""
	}
	if parsed.To4() != nil {
		// IPv4: zero last octet (192.168.1.123 → 192.168.1.0)
		parts := strings.Split(parsed.String(), ".")
		parts[3] = "0"
		return strings.Join(parts, ".")
	}
	// IPv6: zero last 80 bits (/48 mask)
	mask := net.CIDRMask(48, 128)
	masked := parsed.Mask(mask)
	return masked.String()
}
