import attrs

from weather_pipe.service_layer.uow import UnitOfWorkProtocol
from weather_pipe.usecases._event import Event


@attrs.define
class PromoteToBronzeLayer(Event):
    db_path: str = attrs.field(default="")


def bronze_layer_handler(event: PromoteToBronzeLayer, uow: UnitOfWorkProtocol) -> None:
    pass
