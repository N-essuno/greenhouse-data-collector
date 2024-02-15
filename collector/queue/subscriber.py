import stomp
import json

class Subscriber(stomp.ConnectionListener):
    def __init__(self, conn, config, CONFIG_PATH):
        self.conn = conn
        self.config = config
        self.CONFIG_PATH = CONFIG_PATH

    def __update_config(self, data):
        """
        Update the configuration file with the new configuration
        :param config: configuration to write
        :param config_path: path to the config file
        """
        for sec in self.config.sections():
            for key in self.config[sec]:
                self.config.set(sec, key, str(data[sec][key]))

    def __write_config(self):
        """
        Write the configuration to the config file
        :param config: configuration to write
        :param config_path: path to the config file
        """
        with open(self.config_path, 'w') as configfile:
            self.config.write(configfile)

    def on_error(self, frame) -> None:
        print(f'received an error {frame.body}')

    def on_message(self, frame) -> None:
        # We need to command <pump> <time>
        command = frame.body.split("[CONFIG]")[1]
        
        # Parse the content of the message as json
        # config_json = json.loads("".join(command.strip().splitlines()))
        config_json = json.loads(command)

        # Update the configuration file
        self.__update_config(config_json)

        # Write the configuration file
        self.__write_config()