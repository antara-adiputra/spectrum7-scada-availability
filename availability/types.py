import datetime
from io import BytesIO
from types import MappingProxyType
from typing import Annotated, Any, Awaitable, Callable, ClassVar, Dict, Generic, Generator, Iterable, List, Literal, Mapping, Optional, Self, Sequence, Set, Tuple, Type, TypeAlias, TypedDict, TypeVar, Union


FilePaths: TypeAlias = List[str]
FileDict: TypeAlias = Dict[str, BytesIO]
FileInput: TypeAlias = Union[str, FilePaths, FileDict]
KeepDupl_: TypeAlias = Union[bool, Literal['first', 'last']]
NameOrTuple: TypeAlias = Union[str, Tuple[str, str, str]]
SCDMasterType: TypeAlias = Literal['spectrum', 'survalent']
AvKeys: TypeAlias = Union[str, Tuple[str, str, str], Sequence[str]]
DateSpec: TypeAlias = Union[datetime.datetime, datetime.date]
