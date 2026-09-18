//go:build race

package hostlink

// raceDetectorEnabled is true when the package is built with -race. It exists
// for one test: TestReconnectStressNeverWedgesACaller, which trips a data race
// INSIDE centrifuge-go v0.12.0 (client.go:2187 reads c.transport without c.mu
// while moveToConnecting writes it under c.mu at :533) on nearly every run
// under the detector. The race is third-party, reachable from the production
// path, and not this module's to fix; see the test for the opt-in.
const raceDetectorEnabled = true
