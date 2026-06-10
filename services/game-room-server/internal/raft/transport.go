package raft

// This file is reserved for custom gRPC-based Raft transport if needed.
// Currently using Hashicorp Raft's built-in TCP transport (see node.go).
// Custom transport would implement raft.Transport interface for
// cross-pod communication with TLS and authentication.
