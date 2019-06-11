from amazon.ion.core import DataEvent
from amazon.ion.core import IonEvent
from amazon.ion.core import IonEventType
from amazon.ion.util import Enum
from amazon.ionhash.hasher import Hasher

class HashEvent(Enum):
    DISABLE_HASHING = 0
    ENABLE_HASHING = 1
    DIGEST = 2

def hashing_reader(reader, hash_function_provider, hashing_enabled = True):
    hasher = Hasher(hash_function_provider)
    event = None
    while True:
        directive = yield event
        event = reader.send(directive)
        while event is not None:
            if directive == HashEvent.DISABLE_HASHING:
                hashing_enabled = False

            elif directive == HashEvent.ENABLE_HASHING:
                hashing_enabled = True

            elif directive == HashEvent.DIGEST:
                event = hasher.digest()

            elif isinstance(event, IonEvent) and event.event_type is not IonEventType.STREAM_END:
                if hashing_enabled:
                    if event.event_type is IonEventType.CONTAINER_START:
                        hasher.step_in(event)

                    elif event.event_type is IonEventType.CONTAINER_END:
                        hasher.step_out()

                    else:
                        hasher.update(event)

            directive = yield event

            if isinstance(directive, DataEvent):
                event = reader.send(directive)
