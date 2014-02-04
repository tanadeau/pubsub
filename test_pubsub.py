#! /usr/bin/python

"""
Unit tests for pubsub module
"""

import logging
import unittest

import pubsub


class SingleSubscriber(object):
    def __init__(self, test, bus, exp_topic, exp_data, create_topic=True):
        self.bus = bus
        self.test = test
        self.exp_topic = exp_topic
        self.exp_data = exp_data
        self.num_cb = 0  # Number of times the data callback was invoked

        if create_topic:
            self.bus.create_topic(exp_topic)

    def subscribe_good(self):
        self.bus.subscribe(self.exp_topic, self.sub_cb_good)

    def subscribe_not_callable(self):
        self.bus.subscribe(self.exp_topic, 'not a callback')

    def subscribe_incorrect_sig(self):
        self.bus.subscribe(self.exp_topic, self.sub_cb_invalid)

    def sub_cb_good(self, topic, data):
        # Verify expected topic name and data were given
        self.test.assertEquals(topic, self.exp_topic)
        self.test.assertEquals(data, self.exp_data)
        self.num_cb += 1

    def sub_cb_invalid(self, topic):
        self.test.fail('Invalid callback should never be called')


class MultiSubscriber(object):
    def __init__(self, test, bus, topic_data_dict):
        self.bus = bus
        self.test = test
        self.topic_data_dict = topic_data_dict
        self.num_cb = 0  # Number of times the data callback was invoked

        for topic in self.topic_data_dict:
            self.bus.create_topic(topic)

    def subscribe_topics(self):
        for topic in self.topic_data_dict:
            self.bus.subscribe(topic, self.common_sub_cb)

    def common_sub_cb(self, topic, data):
        # Verify the topic was one of the expected and the data matches what is
        # expected for that topic
        self.test.assertIn(topic, self.topic_data_dict)
        self.test.assertEquals(data, self.topic_data_dict[topic])
        self.num_cb += 1

class PubSubTest(unittest.TestCase):
    """Unit tests for pubsub module"""
    def test_normal_pub_sub(self):
        """Tests normal usage with topic created before sub"""
        # Create bus and foo topic
        bus = pubsub.PubSubBus()
        bus.create_topic('foo')

        # Create two initial subscribers not subscribed to any topics yet
        exp_data = {'payload': 'test'}
        foo_sub = SingleSubscriber(self, bus, 'foo', exp_data)
        bar_sub = SingleSubscriber(self, bus, 'bar', 5)
        self.assertEquals(foo_sub.num_cb, 0)
        self.assertEquals(bar_sub.num_cb, 0)

        # Publish on topic foo and ensure that no subscribers see it (since
        # nothing is subscribed yet)
        bus.publish('foo', exp_data)
        self.assertEquals(foo_sub.num_cb, 0)
        self.assertEquals(bar_sub.num_cb, 0)

        # Have both subscribers subscribe to topics foo and bar, respectively
        foo_sub.subscribe_good()
        bar_sub.subscribe_good()

        # Publish on topic foo again and ensure that only the foo subscriber
        # saw it
        bus.publish('foo', exp_data)
        self.assertEquals(foo_sub.num_cb, 1)
        self.assertEquals(bar_sub.num_cb, 0)

        # Create a new foo subscriber and immediately subscribe
        foo_sub2 = SingleSubscriber(self, bus, 'foo', exp_data)
        foo_sub2.subscribe_good()
        self.assertEquals(foo_sub2.num_cb, 0)

        # Publish on topic foo again and make sure that the first foo sub got
        # both messages (this and last) and the second subscriber only received
        # the message just published
        bus.publish('foo', exp_data)
        self.assertEquals(foo_sub.num_cb, 2)
        self.assertEquals(bar_sub.num_cb, 0)
        self.assertEquals(foo_sub2.num_cb, 1)

        # Publish on topic bar and make sure that only the bar subscriber
        # received it
        bus.publish('bar', 5)
        self.assertEquals(foo_sub.num_cb, 2)
        self.assertEquals(bar_sub.num_cb, 1)
        self.assertEquals(foo_sub2.num_cb, 1)

    def test_normal_sub_pub(self):
        """Tests normal usage with subs created before topic"""
        # Create bus
        bus = pubsub.PubSubBus()

        # Create two initial subscribers not subscribed to any topics yet
        exp_data = {'payload': 'test'}
        foo_sub = SingleSubscriber(self, bus, 'foo', exp_data)
        bar_sub = SingleSubscriber(self, bus, 'bar', 5)
        self.assertEquals(foo_sub.num_cb, 0)
        self.assertEquals(bar_sub.num_cb, 0)

        foo_sub.subscribe_good()
        bar_sub.subscribe_good()

        bus.publish('foo', exp_data)
        self.assertEquals(foo_sub.num_cb, 1)
        self.assertEquals(bar_sub.num_cb, 0)

    def test_topic_check(self):
        """
        Tests check that one cannot publish or subscribe before the topic is
        created
        """
        # Create bus
        bus = pubsub.PubSubBus()

        # Try to publish with non-existent topic
        self.assertRaises(ValueError, bus.publish, 'foo', 'data')

        foo_sub = SingleSubscriber(
            self, bus, 'foo', 'data', create_topic=False)

        # Try to subscribe to non-existent topic
        self.assertRaises(ValueError, foo_sub.subscribe_good)

        # Create topic
        bus.create_topic('foo')

        # Everything should work normally now
        foo_sub.subscribe_good()
        bus.publish('foo', 'data')
        self.assertEquals(foo_sub.num_cb, 1)

    def test_invalid_callbacks(self):
        """
        Tests errors cases when subscriber data callbacks are not callback or
        have incorrect signature
        """
        # Create bus
        bus = pubsub.PubSubBus()

        # Try to subscribe with a non-callable callback
        foo_sub = SingleSubscriber(self, bus, 'foo', 'data')
        self.assertRaises(TypeError, foo_sub.sub_cb_invalid)

        # Create second sub to check bus will not be broken
        foo_sub2 = SingleSubscriber(self, bus, 'foo', 'data')
        foo_sub2.subscribe_good()

        # Ensure that invalid callbacks (invalid signatures) do not get called
        # and do not break the bus for other subscribers
        foo_sub.subscribe_incorrect_sig()
        bus.publish('foo', 'data')
        self.assertEquals(foo_sub2.num_cb, 1)

    def test_multi_sub(self):
        """Tests one subscriber that subscribes to multiple topics"""
        # Create bus
        bus = pubsub.PubSubBus()

        topic_data_dict = {
            'foo': 'data',
            'bar': 589,
            'baz': {'a': 1, 'b': 2}}

        # Create multi-subscriber and publish to all topics before subscribing.
        multi_sub = MultiSubscriber(self, bus, topic_data_dict)

        for (topic, data) in topic_data_dict.iteritems():
            bus.publish(topic, data)

        self.assertEquals(multi_sub.num_cb, 0)

        # Subscribe to all topics
        multi_sub.subscribe_topics()

        # Publish several times in various combos of topics and then verify
        # counts

        for (topic, data) in topic_data_dict.iteritems():
            bus.publish(topic, data)

        self.assertEquals(multi_sub.num_cb, len(topic_data_dict))

        for (topic, data) in topic_data_dict.iteritems():
            bus.publish(topic, data)
            bus.publish(topic, data)

        self.assertEquals(multi_sub.num_cb, 3 * len(topic_data_dict))

        bus.publish('foo', topic_data_dict['foo'])
        self.assertEquals(multi_sub.num_cb, 3 * len(topic_data_dict) + 1)


if __name__ == '__main__':
    logging.basicConfig()
    unittest.main()
