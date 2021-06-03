package event

import "github.com/kutty-kumar/charminder/pkg"

type Publisher interface {
	Publish(event pkg.Event)
	PublishAsync(event pkg.Event)
	Flush()
	Close()
}
