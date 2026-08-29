/**
 * Cross-SDK golden vectors. The Go SDK asserts the identical values
 * (sdk-go/conformance/golden_test.go) so the two implementations cannot drift
 * on routing derivations. To add a vector: compute it here, then mirror it in
 * the Go test file.
 */
export const GOLDEN = {
  sessionTokenSess1: 'q-Yz86R6J1gXTqvpFg2vNs',
  discover: 'abc.discover',
  toolCallEcho: 'abc.tool.call.ops.echo',
  toolProgressC1: 'abc.tool.progress.c1',
  variableBaseUrl: 'abc.var.ops.base-url',
  interruptOps: 'abc.ctl.interrupt.ops',
  hookCallBeforeCreate: 'abc.hook.call.ops.session.before_create',
  hookEventSessionCreated: 'abc.hook.event.session.created',
  configOps: 'abc.config.ops',
  configGetOps: 'abc.config.get.ops',
} as const
