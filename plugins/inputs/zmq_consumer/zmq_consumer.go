package zmq_consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"

	zmq "github.com/pebbe/zmq4"
)

type zmqConsumer struct {
	Endpoints     []string `toml:"endpoints"`
	Subscriptions []string `toml:"subscriptions"`

	Log telegraf.Logger

	subscriber *zmq.Socket
	parser     parsers.Parser
	acc        telegraf.TrackingAccumulator

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

var sampleConfig = `
  ## urls of endpoints
  endpoints = ["tcp://localhost:6060"]

  ## Subscription filters
  # subscriptions = ["telegraf"]

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

func (z *zmqConsumer) SampleConfig() string {
	return sampleConfig
}

func (z *zmqConsumer) Description() string {
	return "Read metrics from ZeroMQ PUB sockets"
}

func (n *zmqConsumer) SetParser(parser parsers.Parser) {
	n.parser = parser
}

func (z *zmqConsumer) Gather(_ telegraf.Accumulator) error {
	return nil
}

// Start the ZeroMQ consumer. Caller must call *zmqConsumer.Stop() to clean up.
func (z *zmqConsumer) Start(acc telegraf.Accumulator) error {
	z.acc = acc.WithTracking(1000)
	if z.subscriber == nil {
		// create the socket
		subscriber, err := zmq.NewSocket(zmq.SUB)
		if err != nil {
			return err
		}
		z.subscriber = subscriber

		// set subscription filters
		subscriber.SetSubscribe("")

		// connect to endpoints
		for _, endpoint := range z.Endpoints {
			z.subscriber.Connect(endpoint)
		}

		ctx, cancel := context.WithCancel(context.Background())
		z.cancel = cancel

		// start the message reader
		z.wg.Add(1)
		go func() {
			defer z.wg.Done()
			go z.receiver(ctx)
		}()
	}
	return nil
}

// receiver() reads all published messages from the socket
func (z *zmqConsumer) receiver(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := z.subscriber.Recv(zmq.DONTWAIT)
			if err == nil {
				fmt.Println(msg)
			}
		}
	}
}

func (z *zmqConsumer) Stop() {
	z.cancel()
	z.wg.Wait()
	z.subscriber.Close()
}

func init() {
	inputs.Add("zmq_consumer", func() telegraf.Input {
		return &zmqConsumer{
			Endpoints:     []string{"tcp://localhost:6060"},
			Subscriptions: []string{"telegraf"},
		}
	})
}
