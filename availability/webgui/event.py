from collections import deque
from itertools import islice

from nicegui import events

from .types import *


def consume(iterator: Iterable, n: int | None = None) -> None:
	"Advance the iterator n-steps ahead. If n is None, consume entirely."
	# Use functions that consume iterators at C speed.
	# For more information, https://docs.python.org/3/library/itertools.html#itertools-recipes
	if n is None:
		deque(iterator, maxlen=0)
	else:
		next(islice(iterator, n, n), None)


class _EventChains:

	def __init__(self, chains: Iterable[Callable]):
		self.chains = list(chains)


class EventChainsWithArgs(_EventChains):

	def __call__(self, e: events.ValueChangeEventArguments):
		consume(map(lambda chain: chain(e), self.chains))


class EventChainsWithoutArgs(_EventChains):

	def __call__(self):
		consume(map(lambda chain: chain(), self.chains))
