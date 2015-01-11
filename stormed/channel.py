from stormed.util import AmqpError
from stormed.method.channel import Open, Close, Flow
from stormed.method import exchange as _exchange, basic, queue as _queue, tx
from stormed.frame import FrameHandler, status

class FlowStoppedException(AmqpError): pass

class Channel(FrameHandler):
    """An AMQP Channel

    And AMQP Channel represent a logical connection to the AMQP server.
    Unless there are really specific needs, there is no reason to use
    more than one Channel instance per process for a
    standard stormed-amqp / tornadoweb application.

    Then Channel class should be only instantiated by
    stormed.Connection.channel method.

    Channel.on_error callback is called in case of "Soft" AMQP Error with
    a ChannelError instance as argument:

        def on_channel_error(channel_error):
            print channel_error.reply_code
            print channel_error.reply_text
            print channel_error.method

        channel.on_error = on_channel_error

    Channel.on_return is called when the AMQP server returns a
    message published by the client ("basic.return").
    the callback receives a stormed.Message as argument:

        def on_msg_returned(msg):
            print msg.rx_data.reply_code

        channel.on_return = on_msg_returnedi
    """

    def __init__(self, channel_id, conn):
        self.channel_id = channel_id
        self.consumers = {}
        self.status = status.CLOSED
        self.on_error = None
        self.on_return = None
        self.flow_stopped = False
        super(Channel, self).__init__(conn)

    def open(self, callback=None):
        self.status = status.OPENING
        self.send_method(Open(out_of_band=''), callback)

    def close(self, callback=None):
        self.status = status.CLOSING
        _close = Close(reply_code=0, reply_text='', class_id=0, method_id=0)
        self.send_method(_close, callback)

    def exchange_declare(self, exchange, type="direct", passive=False,
                                durable=False, auto_delete=False,
                                internal=False, no_wait=False,
                                arguments=None, callback=None):
        """
        Verify exchange exists, create if needed.

        This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        Parameters
        ----------

        passive : bool

            If set, the server will reply with Declare-Ok if the exchange
            already exists with the same name, and raise an error if not. The
            client can use this to check whether an exchange exists without
            modifying the server state. When set, all other method fields
            except name and no-wait are ignored. A declare with both passive
            and no-wait has no effect.  Arguments are compared for semantic
            equivalence.

            If set, and the exchange does not already exist, the server MUST
            raise a channel exception with reply code 404 (not found).  If not
            set and the exchange exists, the server MUST check that the
            existing exchange has the same values for type, durable, and
            arguments fields. The server MUST respond with Declare-Ok if the
            requested exchange matches these fields, and MUST raise a channel
            exception if not.

        durable : bool

            If set when creating a new exchange, the exchange will be marked as
            durable. Durable exchanges remain active when a server restarts.
            Non-durable exchanges (transient exchanges) are purged if/when a
            server restarts.

            The server MUST support both durable and transient exchanges.

        auto_delete : bool

            If set, the exchange is deleted when all queues have finished using
            it.

            The server SHOULD allow for a reasonable delay between the point
            when it determines that an exchange is not being used (or no longer
            used), and the point when it deletes the exchange. At the least it
            must allow a client to create an exchange and then bind a queue to
            it, with a small but non-zero delay between these two actions.

            The server MUST ignore the auto-delete field if the exchange
            already exists.

            This is a RabbitMQ extension.

        internal : bool

            If set, the exchange may not be used directly by publishers, but
            only when bound to other exchanges. Internal exchanges are used to
            construct wiring that is not visible to applications. 

            This is a RabbitMQ extension.

        no_wait : bool

            If set, the server will not respond to the method. The client
            should not wait for a reply method. If the server could not
            complete the method it will raise a channel or connection
            exception.

        arguments : dict

            A set of arguments for the declaration. The syntax and semantics of
            these arguments depends on the server implementation. 
        """
        if arguments is None:
            arguments = {}
        # TODO: data types of 'arguments'?

        self.send_method(_exchange.Declare(ticket      = 0,
                                           exchange    = exchange,
                                           type        = type,
                                           passive     = passive,
                                           durable     = durable,
                                           auto_delete = auto_delete,
                                           internal    = internal,
                                           nowait      = no_wait,
                                           arguments   = arguments), callback)

    def exchange_delete(self, exchange, if_unused=False, callback=None):
        self.send_method(_exchange.Delete(ticket    = 0,
                                          exchange  = exchange,
                                          if_unused = if_unused,
                                          nowait    = False), callback)

    def queue_declare(self, queue='', passive=False, durable=True,
                            exclusive=False, auto_delete=False, callback=None):
        """implements "queue.declare" AMQP method

        the callback receives as argument a queue.DeclareOk method instance:

            def on_creation(qinfo):
                print qinfo.queue # queue name
                print qinfo.message_count
                print qinfo.consumer_count

            channel.queue_declare('queue_name', callback=on_creation)
        """

        self.send_method(_queue.Declare(ticket      = 0,
                                        queue       = queue,
                                        passive     = passive,
                                        durable     = durable,
                                        exclusive   = exclusive,
                                        auto_delete = auto_delete,
                                        nowait      = False,
                                        arguments   = dict()), callback)

    def queue_delete(self, queue, if_unused=False, if_empty=False,
                           callback=None):
        self.send_method(_queue.Delete(ticket    = 0,
                                       queue     = queue,
                                       if_unused = if_unused,
                                       if_empty  = if_empty,
                                       nowait    = False), callback)

    def queue_bind(self, queue, exchange, routing_key='', callback=None):
        self.send_method(_queue.Bind(ticket      = 0,
                                     queue       = queue,
                                     exchange    = exchange,
                                     routing_key = routing_key,
                                     nowait      = False,
                                     arguments   = dict()), callback)

    def queue_unbind(self, queue, exchange, routing_key='', callback=None):
        self.send_method(_queue.Unbind(ticket     = 0,
                                      queue       = queue,
                                      exchange    = exchange,
                                      routing_key = routing_key,
                                      nowait      = False,
                                      arguments   = dict()), callback)

    def queue_purge(self, queue, callback=None):
        """implements "queue.purge" AMQP method

        the callback receives as argument the number of purged messages:

            def queue_purged(message_count):
                print message_count

            channel.queue_purge('queue_name')
        """

        self.send_method(_queue.Purge(ticket=0, queue=queue, nowait=False),
                         callback)

    def qos(self, prefetch_size=0, prefetch_count=0, _global=False,
                  callback=None):
        self.send_method(basic.Qos(prefetch_size  = prefetch_size,
                                   prefetch_count = prefetch_count,
                                   _global        = _global), callback)

    def publish(self, message, exchange, routing_key='', immediate=False,
                      mandatory=False):
        if self.flow_stopped:
            raise FlowStoppedException
        if (immediate or mandatory) and self.on_return is None:
            raise AmqpError("on_return callback must be set for "
                            "immediate or mandatory publishing")
        self.send_method(basic.Publish(ticket = 0,
                                       exchange = exchange,
                                       routing_key = routing_key,
                                       mandatory = mandatory,
                                       immediate = immediate), message=message)

    def get(self, queue, callback, no_ack=False):
        """implements "basic.get" AMQP method

        the callback receives as argument a stormed.Message instance
        or None if the AMQP queue is empty:

            def on_msg(msg):
                if msg is not None:
                    print msg.body
                else:
                    print "empty queue"

            channel.get('queue_name', on_msg)
        """
        _get = basic.Get(ticket=0, queue=queue, no_ack=no_ack)
        self.send_method(_get, callback)

    def consume(self, queue, consumer, no_local=False, no_ack=False,
                      exclusive=False):
        """implements "basic.consume" AMQP method

        The consumer argument is either a callback or a Consumer instance.
        The callback is called, with a Message instance as argument,
        each time the client receives a message from the server.
        """
        if not isinstance(consumer, Consumer):
            consumer = Consumer(consumer)
        def set_consumer(consumer_tag):
            consumer.tag = consumer_tag
            consumer.channel = self
            self.consumers[consumer_tag] = consumer
        _consume = basic.Consume(ticket       = 0,
                                 queue        = queue,
                                 consumer_tag = '',
                                 no_local     = no_local,
                                 no_ack       = no_ack,
                                 exclusive    = exclusive,
                                 nowait       = False,
                                 arguments    = dict())
        self.send_method(_consume, set_consumer)

    def recover(self, requeue=False, callback=None):
        self.send_method(basic.Recover(requeue=requeue), callback)

    def flow(self, active, callback=None):
        self.send_method(Flow(active=active), callback)

    def select(self, callback=None):
        if self.on_error is None:
            raise AmqpError("Channel.on_error callback must be set for tx mode")
        self.send_method(tx.Select(), callback)

    def commit(self, callback=None):
        self.send_method(tx.Commit(), callback)

    def rollback(self, callback=None):
        self.send_method(tx.Rollback(), callback)

class Consumer(object):
    """AMQP Queue consumer

    the Consumer can be used as Channel.consume() "consumer" argument
    when the application must be able to stop a specific basic.consume message
    flow from the server.
    """

    def __init__(self, callback):
        self.tag = None
        self.channel = None
        self.callback = callback

    def cancel(self, callback):
        """implements "basic.cancel" AMQP method"""
        _cancel = basic.Cancel(consumer_tag=self.tag, nowait=False)
        self.channel.send_method(_cancel, callback)
