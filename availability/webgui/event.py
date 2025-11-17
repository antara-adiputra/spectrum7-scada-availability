from nicegui import events

from .types import *
from ..lib import consume


class _EventChains:

	def __init__(self, chains: Iterable[Callable]):
		self.chains = list(chains)


class EventChainsWithArgs(_EventChains):

	def __call__(self, e: events.ValueChangeEventArguments):
		consume(map(lambda chain: chain(e), self.chains))


class EventChainsWithoutArgs(_EventChains):

	def __call__(self):
		consume(map(lambda chain: chain(), self.chains))
