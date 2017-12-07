'''
get_raw_data

This script executes every data collection specified
in the directory 'config'. This script must be executed
from it's own base directory. Common parameters are
parsed from 'settings.config', and parameters specific
to the specified data collection are extracted from
from the respective configuration file in 'config'.
'''

import configparser
import importlib.util
import logging
import os
import sys
import argparse

# For turning down the volume of selenium
from selenium.webdriver.remote.remote_connection import LOGGER


def gather_arguments(unknown):
    key = None
    new_args = []
    bonus_arguments = {}
    for arg in unknown:
        if arg.startswith("--"):
            if key is not None:
                bonus_arguments[key] = " ".join(new_args)
            key = arg.lstrip("--")
            new_args = []
            continue
        if key is None:
            continue
        new_args.append(arg)

    if key is not None:
        bonus_arguments[key] = " ".join(new_args)
    return bonus_arguments


if __name__ == "__main__":

    # Process arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, nargs='*', default=None,
                        help='Specific configs to run.', required=True)
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true", default=False)
    args, unknown = parser.parse_known_args()
    bonus_arguments = gather_arguments(unknown)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
        LOGGER.setLevel(logging.WARNING)
        logging.getLogger("easyprocess").setLevel(logging.WARNING)
        logging.getLogger("pyvirtualdisplay").setLevel(logging.WARNING)
    else:
        logging.basicConfig(level=logging.INFO, stream=sys.stderr)
        logging.getLogger("requests").setLevel(logging.WARNING)

    # Check that this script is being executed from the same directory
    # as this file
    this_path = os.path.dirname(os.path.realpath(__file__))
    this_dir, this_file = os.path.split(os.path.realpath(__file__))
    this_dir = os.path.split(this_dir)[-1]
    if this_path != os.getcwd():
        err_msg = " ".join(this_file, "must be executed from", this_path)
        raise EnvironmentError(err_msg)

    # Also evaluate the top path (one directory up) to get the db path
    _top_path = os.path.dirname(this_path)
    db_path = os.path.join(_top_path, "db_config")
    git_path = os.path.join(_top_path, ".git", "config")

    # Compile the config list
    configs = []
    valid_configs = [f for f in os.listdir("config")
                     if f.endswith(".config")]
    # Check each specific config exists
    for specific_conf in args.config:
        found = False
        for f in valid_configs:
            prefix = os.path.splitext(f)[0]
            if prefix == specific_conf:
                configs.append(f)
                found = True
                break
        if not found:
            raise IOError("No config matching "+specific_conf+" found.")

    # Iterate through files in the directory 'config'
    for file_name in configs:
        logging.info("Found a config file: %s", (file_name))
        # Get the settings for this file
        _settings = configparser.ConfigParser(allow_no_value=True)

        # Add tier-0 to tier-2 config paths
        for db_name in os.listdir(db_path):
            if db_name.endswith(".cnf"):
                _settings["DEFAULT"][db_name] = os.path.join(db_path, db_name)
        _path_to_config_file = os.path.join(this_path, "config", file_name)
        _settings.read(["settings.config", _path_to_config_file])
        _settings.read(["settings.config", git_path])
        for par, val in bonus_arguments.items():
            _settings["parameters"][par] = val
        
        # The file to execute is under the variable "module"
        _path_to_module = _settings["parameters"].pop("module")
        _module_name = os.path.split(_path_to_module)[-1]

        # Add github path
        git_url = _settings['remote "origin"']["url"]
        git_module = os.path.join(git_url.replace(".git", ""), "blob",
                                  "master", this_dir, _path_to_module)
        _settings["DEFAULT"]["github"] = git_module

        logging.info("\tThis config file maps to %s", (_path_to_module))
        # Load the module
        spec = importlib.util.spec_from_file_location(_module_name,
                                                      _path_to_module)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        # Run the function "run" with _settings
        logging.info("\tExecuting %s.run()...", (_module_name.rstrip(".py")))
        module.run(_settings)
