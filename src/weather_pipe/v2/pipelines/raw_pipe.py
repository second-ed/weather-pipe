import argparse

from io_adapters import get_real_adapter

from weather_pipe.v2.adapters.io_funcs import Zone
from weather_pipe.v2.core.constants import REPO_ROOT
from weather_pipe.v2.core.event import EVENT_HANDLERS
from weather_pipe.v2.core.message_bus import MessageBus
from weather_pipe.v2.layers.raw.pipe import IngestToRawZone

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse config path and API key.")
    parser.add_argument("--config-path", type=str, help="Path to the configuration file")
    parser.add_argument("--api-key", type=str, help="API key for authentication")

    args = vars(parser.parse_args())
    args["repo_root"] = str(REPO_ROOT)

    adapter = get_real_adapter(Zone.RAW)

    (
        MessageBus(event_handlers=EVENT_HANDLERS, adapter=adapter)
        .add_events([IngestToRawZone(**args)])
        .handle_events()
    )
