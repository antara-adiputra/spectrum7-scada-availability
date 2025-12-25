from ..core.rtu import *
from ..core.rcd import *
from ..types import *


FlexAlignOpt: TypeAlias = Literal['start', 'end', 'center', 'baseline', 'stretch']
OverflowOpt: TypeAlias = Literal['auto', 'clip', 'scroll', 'hidden', 'visible']
ColorTemplate: TypeAlias = Literal['primary', 'secondary', 'accent', 'dark', 'positive', 'negative', 'info', 'warning']

SpinnerType: TypeAlias = Literal['audio', 'bar', 'balls', 'box', 'clock', 'comment', 'cube', 'dots', 'facebook', 'gears', 'grid', 'hearts', 'hourglass', 'infinity', 'ios', 'orbit', 'oval', 'pie', 'puff', 'radio', 'rings', 'tail']
QButtonStyle: TypeAlias = Literal['outline', 'flat', 'unelevated', 'rounded', 'push', 'square', 'glossy', 'round', 'fab']
