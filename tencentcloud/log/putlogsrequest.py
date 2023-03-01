#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Tencent Cloud Computing
# All rights reserved.


class PutLogsRequest:
    """ The request used to send data to log.

    :type topic: string
    :param topic: topic name

    :type source: string
    :param source: source of the logs

    :type logitems: list<LogItem>
    :param logitems: log data

    :type compress: bool
    :param compress: if need to compress the logs

    :type logtags: list
    :param logtags: list of key:value tag pair , [(tag_key_1,tag_value_1) , (tag_key_2,tag_value_2)]
    """

    def __init__(self, topic=None, source=None, logitems=None, compress=True, logtags=None):
        self.topic = topic
        self.source = source
        self.logitems = logitems
        self.compress = compress
        self.logtags = logtags

    def get_compress(self):
        return self.compress

    def set_compress(self, compress):
        self.compress = compress

    def get_topic(self):
        """ Get topic name

        :return: string, topic name
        """
        return self.topic if self.topic else ''

    def set_topic(self, topic):
        """ Set topic name

        :type topic: string
        :param topic: topic name
        """
        self.topic = topic

    def get_source(self):
        """ Get log source

        :return: string, log source
        """
        return self.source

    def set_source(self, source):
        """ Set log source

        :type source: string
        :param source: log source
        """
        self.source = source

    def get_log_items(self):
        """ Get all the log data

        :return: LogItem list, log data
        """
        return self.logitems

    def set_log_items(self, logitems):
        """ Set the log data

        :type logitems: LogItem list
        :param logitems: log data
        """
        self.logitems = logitems

    def get_log_tags(self):
        """ Get all the log tags

        :return: Logtags list, log data
        """
        return self.logtags

    def set_log_tags(self, logtags):
        """ Set the log tags

        :type logtags: logtags list
        :param logtags: log tags
        """
        self.logtags = logtags