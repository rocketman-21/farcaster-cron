export function getCastBufferHash(castHash: string): Buffer {
  return Buffer.from(castHash.replace('0x', ''), 'hex');
}

export function getCastStringHash(castBuffer: Buffer): string {
  return `0x${castBuffer.toString('hex')}`;
}
