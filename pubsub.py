"""
Simple in-memory publish/subscribe message bus
"""

import logging

class PubSubBus(object):
    """
    In-memory Publish/Subscribe message. This bus allows zero or more
    publishers (producers of messages) to communicate with zero or more
    subscribers (consumers of messages) over a channel (a topic). The
    publishers have nor need to have knowledge of where or how many subscribers
    exist for topics.

    Note:
        Since this bus is in-memory, all publishers and subscribers must be
        within the same process.
    """
    def __init__(self, base_logger=None):
        """
        Constructor.

        Args:
            base_logger (logging.Logger): Base logger from which to derive
                class' internal logger. This is useful if using this class
                within a library or application with an existing logging
                infrastructure.
        """
        self.dispatch = {}  # Dictionary of topic name: set of data callbacks

        logger_suffix = self.__class__.__name__
        if base_logger is None:
            self.logger = logging.getLogger(logger_suffix)
        else:
            self.logger = base_logger.getChild(logger_suffix)

    def create_topic(self, topic_name):
        """
        Creates a topic with the given name. It is not an error if a topic with
        the same name is created multiple times. A topic must be created before
        any entity can publish or subscribe to it.

        Args:
            topic_name (str): Name of topic to create
        """
        if topic_name in self.dispatch:
            # Just log if topic already exists. There is no harm of allowing
            # multiple create calls especially since publishers and subscribers
            # should be as independent as possible.
            self.logger.debug(
                "Topic with name '%s' already exists" % topic_name)
        else:
            self.dispatch[topic_name] = set()

    def publish(self, topic_name, data):
        """
        Publishes `data` on topic with name `topic_name` to all subscribers of
        that topic.

        Note:
            This implementation has transient local durability, meaning that
            subscribers will not receive data published before the subscription
            was created.

        Args:
            topic_name (str): Topic name on which to publish the data
            data: Data to send to all subscribers on the topic

        Raises:
            ValueError: If topic with name `topic_name` does not exist.
        """
        self._check_topic_exists(topic_name)
        for sub_callback in self.dispatch[topic_name]:
            try:
                sub_callback(topic_name, data)
            except:
                # Catch all exceptions and log them so that a malformed or
                # badly implemented data callback does not crash the system.
                self.logger.exception(
                    'Exception caught when calling subscriber data callback')

    def subscribe(self, topic_name, data_callback):
        """
        Addes a subscriber's callback to be called when data is published to
        the given topic.

        Args:
            topic_name (str): Topic name to which to subscribe
            data_callback (callable): Function, method, or functor that is
                callable as `data_callback(topic_name, data)`.

        Raises:
            ValueError: If topic with name `topic_name` does not exist.
        """
        self._check_topic_exists(topic_name)

        # Validate data callback
        if not callable(data_callback):
            err_msg = (
                "Attempted to add data callback that is not callable as "
                "callback for topic '%s'" % topic_name)

            self.logger.error(err_msg)
            raise TypeError(err_msg)

        self.dispatch[topic_name].add(data_callback)

    def _check_topic_exists(self, topic_name):
        """
        Checks if topic exists and raises ValueError if not.

        Args:
            topic_name (str): Topic name to check

        Raises:
            ValueError: If topic with name `topic_name` does not exist.
        """
        if topic_name not in self.dispatch:
            raise ValueError(
                "Topic with name '%s' does not exist" % topic_name)
