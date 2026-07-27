// Package embertest provides ember test doubles. EntityRepository is an
// in-memory ember.EntityRepository with filter, sort, and optimistic-version
// parity with the real backends, so unit tests exercise the same semantics.
// Recorder captures what a Save published and decodes it back into events.
// Nothing here is meant to be wired into a real application.
package embertest
