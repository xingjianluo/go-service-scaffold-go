package runtime

import "os"

// tTempDir writes a JSON config to a temp file and returns its path.
// (Defined separately to keep test cases focused on assertions.)
func tTempDir(content string) string {
	f, err := os.CreateTemp("", "rt-config-*.json")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		panic(err)
	}
	return f.Name()
}
