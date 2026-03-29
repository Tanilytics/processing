package processors

import (
	"strings"

	ua "github.com/mileusna/useragent"
)

type UserAgentParser struct{}

func NewUserAgentParser() *UserAgentParser {
	return &UserAgentParser{}
}

func (p *UserAgentParser) Parse(rawUA string) (browser, os, deviceType string) {
	parsed := ua.Parse(strings.TrimSpace(rawUA))

	return normalizeUAValue(parsed.Name), normalizeUAValue(parsed.OS), detectDeviceType(parsed)
}

func detectDeviceType(parsed ua.UserAgent) string {
	switch {
	case parsed.Bot:
		return "bot"
	case parsed.Mobile:
		return "mobile"
	case parsed.Tablet:
		return "tablet"
	case parsed.Desktop:
		return "desktop"
	default:
		return "unknown"
	}
}

func normalizeUAValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}

	return value
}
