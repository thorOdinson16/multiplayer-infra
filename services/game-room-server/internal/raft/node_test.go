package raft

import "testing"

func TestParseNodeOrdinal(t *testing.T) {
	cases := []struct {
		name        string
		wantBase    string
		wantOrdinal int
		wantErr     bool
	}{
		{"game-room-server-0", "game-room-server", 0, false},
		{"game-room-163423432343-0", "game-room-163423432343", 0, false},
		{"game-room-123-42", "game-room-123", 42, false},
		{"no-ordinal", "", 0, true},
		{"trailing-", "", 0, true},
	}

	for _, c := range cases {
		base, ord, err := parseNodeOrdinal(c.name)
		if c.wantErr {
			if err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", c.name, err)
		}
		if base != c.wantBase || ord != c.wantOrdinal {
			t.Fatalf("parseNodeOrdinal(%s) = (%s,%d), want (%s,%d)", c.name, base, ord, c.wantBase, c.wantOrdinal)
		}
	}
}
