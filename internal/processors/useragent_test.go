package processors

import "testing"

func TestUserAgentParserParse(t *testing.T) {
	parser := NewUserAgentParser()

	tests := []struct {
		name       string
		ua         string
		browser    string
		os         string
		deviceType string
	}{
		{
			name:       "desktop chrome",
			ua:         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
			browser:    "Chrome",
			os:         "Windows",
			deviceType: "desktop",
		},
		{
			name:       "iphone safari",
			ua:         "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
			browser:    "Safari",
			os:         "iOS",
			deviceType: "mobile",
		},
		{
			name:       "android tablet chrome",
			ua:         "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			browser:    "Chrome",
			os:         "Android",
			deviceType: "tablet",
		},
		{
			name:       "bot",
			ua:         "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
			browser:    "Bot",
			os:         "unknown",
			deviceType: "unknown",
		},
		{
			name:       "empty",
			ua:         "",
			browser:    "unknown",
			os:         "unknown",
			deviceType: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			browser, os, deviceType := parser.Parse(tt.ua)
			if browser != tt.browser {
				t.Fatalf("browser = %q, want %q", browser, tt.browser)
			}
			if os != tt.os {
				t.Fatalf("os = %q, want %q", os, tt.os)
			}
			if deviceType != tt.deviceType {
				t.Fatalf("deviceType = %q, want %q", deviceType, tt.deviceType)
			}
		})
	}
}
