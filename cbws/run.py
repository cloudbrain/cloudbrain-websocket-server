import json
import logging
import time
import pkg_resources

import validictory

from argparse import ArgumentParser

from cbws.server import WebsocketServer


class _CommandLineArgError(Exception):
    """ Error parsing command-line options """
    pass



class _Options(object):
    """Options returned by _parseArgs"""


    def __init__(self, config, log_level):
        """
        :param str config: path to JSON config file.
        :param str log_level: logger verbosity 'info' for logging.INFO or 'debug' for logging.DEBUG.
        """
        self.config = config

        if log_level == 'info':
            self.log_level = logging.INFO
        elif log_level == 'debug':
            self.log_level = logging.DEBUG



def _parseArgs():
    """
    Parse command-line args
    :rtype: _Options object
    :raises _CommandLineArgError: on command-line arg error
    """

    parser = ArgumentParser(description="Start CloudBrain websocket server.")

    parser.add_argument(
        "--conf",
        type=str,
        dest="config",
        required=False,
        default='conf/ws_server_config.json',
        help="OPTIONAL: path to JSON config file.")

    parser.add_argument(
        "--log",
        type=str,
        dest="log_level",
        required=False,
        default='info',
        help="OPTIONAL: logger verbosity. Can be 'info' or 'debug'.")

    options = parser.parse_args()

    with pkg_resources.resource_stream(__name__,
                                       "ws_server_config_schema.json") as configSchema:

        try:
            validictory.validate(json.load(options.module_configs), json.load(configSchema))
        except validictory.ValidationError as exc:
            logging.exception("JSON schema validation of --conf file failed")
            parser.error("JSON schema validation of --conf file failed: {}"
                         .format(exc))

    return _Options(config=options.config, log_level=options.log_level)



def run(config, log_level):

    logging.basicConfig(level=log_level)

    with open(config, 'rb') as f:
        config = json.load(f)
        server = WebsocketServer(config)
        try:
            server.start()
            while 1:
                time.sleep(0.1)
        except KeyboardInterrupt:
            server.stop()


def main():
    try:
        options = _parseArgs()
        run(options.config, options.log_level)
    except Exception as ex:
        logging.exception("Wesocket server failed")

if __name__ == '__main__':
    main()
