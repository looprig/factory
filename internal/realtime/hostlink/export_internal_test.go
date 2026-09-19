package hostlink

// SetBeforeSubscribeSend installs the N4 window hook on a link this package
// dialled. It is compiled into tests only.
func SetBeforeSubscribeSend(link Link, hook func()) {
	link.(*centrifugeLink).beforeSubscribeSend = hook
}
