package catalog

import "regexp"

// uuidPattern matches the canonical textual UUID form.
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// isUUID reports whether s is a well-formed UUID.
func isUUID(s string) bool { return uuidPattern.MatchString(s) }
