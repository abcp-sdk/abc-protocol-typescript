/**
 * Cross-SDK golden vectors. The Go SDK asserts the identical values
 * (abc-protocol-go/conformance/golden_test.go) so the two implementations
 * cannot drift on routing derivations. To add a vector: compute it here, then
 * mirror it in the Go test file.
 *
 * v2: every data-plane channel is `abc.<tenant>.<...>`; these vectors pin the
 * tenant segment placement so the two SDKs route identically.
 */
export const GOLDEN = {
  tenant: 'alice',
  sessionTokenSess1: 'q-Yz86R6J1gXTqvpFg2vNs',
  discover: 'abc.discover',
  toolCallEcho: 'abc.alice.tool.call.ops.echo',
  toolProgressC1: 'abc.alice.tool.progress.c1',
  variableBaseUrl: 'abc.alice.var.ops.base-url',
  mailboxSess1: 'abc.alice.mailbox.q-Yz86R6J1gXTqvpFg2vNs',
  sessionEventsSess1: 'abc.alice.session.events.q-Yz86R6J1gXTqvpFg2vNs',
  interruptOps: 'abc.alice.ctl.interrupt.ops',
  hookCallBeforeCreate: 'abc.alice.hook.call.ops.session.before_create',
  hookEventSessionCreated: 'abc.alice.hook.event.session.created',
  configOps: 'abc.alice.config.ops',
  configGetOps: 'abc.alice.config.get.ops',
  dlqToken: 'abc.alice.dlq.q-Yz86R6J1gXTqvpFg2vNs',
  sessionVarKey: 't.alice.ops.q-Yz86R6J1gXTqvpFg2vNs.ws',
  varKey: 't.alice.ops.base-url',
} as const
