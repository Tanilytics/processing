package processors

import "strings"

type UserAgentParser struct{}

func NewUserAgentParser() *UserAgentParser {
	return &UserAgentParser{}
}

func (p *UserAgentParser) Parse(rawUA string) (browser, os, deviceType string) {
	ua := strings.ToLower(strings.TrimSpace(rawUA))
	if ua == "" {
		return "unknown", "unknown", "unknown"
	}

	if isBotUserAgent(ua) {
		return "Bot", "unknown", "unknown"
	}

	return detectBrowser(ua), detectOS(ua), detectDeviceType(ua)
}

func detectBrowser(ua string) string {
	switch {
	case strings.Contains(ua, "edg/"):
		return "Edge"
	case strings.Contains(ua, "opr/") || strings.Contains(ua, "opera"):
		return "Opera"
	case strings.Contains(ua, "fxios/") || strings.Contains(ua, "firefox/"):
		return "Firefox"
	case strings.Contains(ua, "crios/"):
		return "Chrome"
	case (strings.Contains(ua, "chrome/") || strings.Contains(ua, "chromium/")):
		return "Chrome"
	case strings.Contains(ua, "trident/") || strings.Contains(ua, "msie "):
		return "Internet Explorer"
	case strings.Contains(ua, "safari/") && !strings.Contains(ua, "android"):
		return "Safari"
	case strings.HasPrefix(ua, "curl/"):
		return "cURL"
	default:
		return "unknown"
	}
}

func detectOS(ua string) string {
	switch {
	case strings.Contains(ua, "windows phone"):
		return "Windows Phone"
	case strings.Contains(ua, "android"):
		return "Android"
	case strings.Contains(ua, "iphone") || strings.Contains(ua, "ipad") || strings.Contains(ua, "ipod") || strings.Contains(ua, "cpu iphone os") || strings.Contains(ua, "cpu os"):
		return "iOS"
	case strings.Contains(ua, "cros"):
		return "ChromeOS"
	case strings.Contains(ua, "windows nt"):
		return "Windows"
	case strings.Contains(ua, "mac os x") || strings.Contains(ua, "macintosh"):
		return "macOS"
	case strings.Contains(ua, "linux") || strings.Contains(ua, "x11"):
		return "Linux"
	default:
		return "unknown"
	}
}

func detectDeviceType(ua string) string {
	switch {
	case strings.Contains(ua, "ipad") || strings.Contains(ua, "tablet") || strings.Contains(ua, "kindle") || strings.Contains(ua, "silk/") || strings.Contains(ua, "playbook") || strings.Contains(ua, "sm-t"):
		return "tablet"
	case strings.Contains(ua, "android"):
		if strings.Contains(ua, "mobile") {
			return "mobile"
		}
		return "tablet"
	case strings.Contains(ua, "mobi") || strings.Contains(ua, "iphone") || strings.Contains(ua, "ipod") || strings.Contains(ua, "windows phone"):
		return "mobile"
	case strings.Contains(ua, "windows nt") || strings.Contains(ua, "macintosh") || strings.Contains(ua, "linux") || strings.Contains(ua, "x11"):
		return "desktop"
	default:
		return "unknown"
	}
}

func isBotUserAgent(ua string) bool {
	botTokens := []string{"bot", "crawler", "spider", "slurp", "bingpreview", "mediapartners", "headless", "phantomjs"}
	for _, token := range botTokens {
		if strings.Contains(ua, token) {
			return true
		}
	}

	return false
}
