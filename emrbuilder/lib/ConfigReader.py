"""
Configuration reader for emrbuilder
This library reads a JSON formatted file and returns values based on a key

This library is called by creating an instance of class ConfigReader
CLUSTER_CONF_FILE must set using 'export CLUSTER_CONF_FILE=emr_default.json'


"""

import json
import os
import logging


class InvalidConfigFileException(Exception):
    pass


class NoConfigException(Exception):
    pass


class ConfigReader(object):

    def __init__(self, conf_dir="CLUSTER_CONF_FILE", conf_file=None):
        # try to load config from environment
        if conf_file is not None:
            self.config_handle = str(conf_file)
        elif conf_dir in os.environ:
            self.config_handle = os.getenv(conf_dir)
        else:
            logging.warning("The environment variable CLUSTER_CONF_FILE is not set, this should "
                        "point to a file in configs/")
            raise NoConfigException("{0} environment variable is not set...".format(conf_dir))

    def _read_config(self):
        # build path
        base_dir = os.getcwd()
        if base_dir.find('emrbuilder') == -1:
            config_handle = base_dir + "/ClusterBuilder/clusterbuilder/configs/" + self.config_handle
        else:
            config_handle = base_dir[0:(base_dir.find('emrbuilder') + 11)] + "configs/" + self.config_handle

        with open(config_handle, 'r') as config:
            config = config.read()

        # verify file is good json
        try:
            #logging.warning("Parsing host config file {0}...".format(config_handle))
            config = json.loads(config)
        except ValueError:
            raise InvalidConfigFileException("The configuration file {0} is not valid JSON / dictionary "
                                             "format".format(config_handle))
        return config

    def get_full_config(self):
        """
        Return full configuration in from of a dictionary
        :return:
        """
        return self._read_config()

    def get_config(self, config_param):
        """
        Return configuration parameter specified by user
        :param config_param: parameter to look for in config file
        :return: string of config parameter
        """
        if not self._read_config().get(config_param):
            raise NoConfigException("Config parameter {0} was not found in configuration...".format(config_param))
        else:
            return self._read_config().get(config_param)
