// Package factory is the public-facing orchestration service: HTTP and
// WebSocket surface, authentication and authorization, durable reads, command
// admission, target selection and Host links.
//
// Factory imports neither Host nor Harness. It talks to Host exclusively
// through Core and SessionStore records, which are the cross-service contract;
// import_boundary_test.go enforces the prohibition and contract.go states the
// positive half.
package factory
