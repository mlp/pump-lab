"""Strict Borsh decoding against the pinned official Pump IDL."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path

IDL_BYTES = (Path(__file__).parent / 'sources/pump.json').read_bytes()
IDL = json.loads(IDL_BYTES)
IDL_SHA256 = hashlib.sha256(IDL_BYTES).hexdigest()
TYPES = {t['name']: t['type'] for t in IDL['types']}
EVENTS = {bytes(e['discriminator']): e['name'] for e in IDL['events']}
CPI_TAG = bytes.fromhex('e445a52e51cb9a1d')
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def b58encode(raw):
    n = int.from_bytes(raw, 'big')
    result = ''
    while n:
        n, rem = divmod(n, 58)
        result = ALPHABET[rem] + result
    return '1' * (len(raw) - len(raw.lstrip(b'\0'))) + result


def b58decode(value):
    n = 0
    for char in value:
        n = n * 58 + ALPHABET.index(char)
    return b'\0' * (len(value) - len(value.lstrip('1'))) + n.to_bytes((n.bit_length()+7)//8, 'big')


class Reader:
    def __init__(self, raw):
        self.raw, self.pos = raw, 0

    def take(self, count):
        if self.pos + count > len(self.raw):
            raise ValueError(f'truncated at byte {self.pos}, need {count}')
        result = self.raw[self.pos:self.pos+count]
        self.pos += count
        return result

    def read(self, kind):
        if isinstance(kind, str):
            if kind == 'pubkey':
                return b58encode(self.take(32))
            if kind == 'bool':
                value = self.take(1)[0]
                if value not in (0, 1):
                    raise ValueError('invalid bool')
                return bool(value)
            if kind == 'string':
                return self.take(self.read('u32')).decode('utf-8')
            if kind.startswith(('u', 'i')) and kind[1:].isdigit():
                return int.from_bytes(self.take(int(kind[1:])//8), 'little', signed=kind[0]=='i')
            raise ValueError(f'unsupported primitive {kind}')
        if 'vec' in kind:
            count = self.read('u32')
            if count > 10000:
                raise ValueError('unreasonable vector')
            return [self.read(kind['vec']) for _ in range(count)]
        if 'array' in kind:
            subtype, count = kind['array']
            return [self.read(subtype) for _ in range(count)]
        if 'defined' in kind:
            definition = kind['defined']
            return self.struct(definition['name'] if isinstance(definition, dict) else definition)
        raise ValueError(f'unsupported compound type {kind}')

    def struct(self, name):
        kind = TYPES[name]
        if kind['kind'] != 'struct':
            raise ValueError(f'unsupported definition {name}')
        return {f['name']: self.read(f['type']) for f in kind['fields']}


def decode_event(raw):
    if raw.startswith(CPI_TAG):
        raw = raw[8:]
    name = EVENTS.get(raw[:8])
    if name is None:
        return None
    reader = Reader(raw[8:])
    event = reader.struct(name)
    if reader.pos != len(reader.raw):
        raise ValueError(f'{name}: {len(reader.raw)-reader.pos} trailing bytes')
    return name, event


def progress(real, initial):
    if initial <= 0 or real < 0:
        raise ValueError('invalid reserves')
    return Decimal(100) * (Decimal(1) - Decimal(real)/Decimal(initial))
