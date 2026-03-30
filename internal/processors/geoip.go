package processors

import (
	"fmt"
	"net/netip"

	"github.com/oschwald/geoip2-golang/v2"
)

type GeoIPResolver struct {
	db *geoip2.Reader
}

func NewGeoIPResolver(dbPath string) (*GeoIPResolver, error) {
	db, err := geoip2.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open GeoIP database: %w", err)
	}
	return &GeoIPResolver{db: db}, nil
}

// Resolve returns country and region for an IP address.
// Pass the truncated IP as GeoLite2 still works with /24 truncation for country-level.
func (g *GeoIPResolver) Resolve(ip string) (country, region string) {
	parsed, err := netip.ParseAddr(ip)
	if err != nil {
		return "", ""
	}

	record, err := g.db.City(parsed)
	if err != nil {
		return "", ""
	}

	country = record.Country.Names.English
	if len(record.Subdivisions) > 0 {
		region = record.Subdivisions[0].Names.English
	}
	return
}

func (g *GeoIPResolver) Close() error {
	return g.db.Close()
}
