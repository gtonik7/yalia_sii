import { mapVendorResult } from './sii-result.mapper';

describe('mapVendorResult', () => {
  it('normalizes a single result object into a one-item array', () => {
    const result = mapVendorResult({
      state: 'ERROR',
      errorCode: '4114',
      internal_ref: 'abc123',
      timestamp: '2026-07-08T09:13:37.078500820Z',
      siiResponse: 'Codigo[4114]...',
    });

    expect(result).toHaveLength(1);
    expect(result[0].internalRef).toBe('abc123');
    expect(result[0].submissionStatus).toBe('ERROR');
    expect(result[0].raw).toMatchObject({ internal_ref: 'abc123', state: 'ERROR' });
  });

  it('passes an array of results through as one item per entry (batch callback)', () => {
    const result = mapVendorResult([
      { state: 'CORRECTO', internal_ref: 'a1' },
      { state: 'ERROR', internal_ref: 'a2' },
    ]);

    expect(result.map((r) => r.internalRef)).toEqual(['a1', 'a2']);
    expect(result.map((r) => r.submissionStatus)).toEqual(['CORRECTO', 'ERROR']);
  });

  it('skips items with no internal_ref — nothing to correlate them to', () => {
    const result = mapVendorResult([{ state: 'ERROR' }, { state: 'CORRECTO', internal_ref: 'a2' }]);

    expect(result).toHaveLength(1);
    expect(result[0].internalRef).toBe('a2');
  });

  it('defaults submissionStatus to "unknown" when state is missing or not a string', () => {
    expect(mapVendorResult({ internal_ref: 'a1' })[0].submissionStatus).toBe('unknown');
    expect(mapVendorResult({ internal_ref: 'a1', state: 42 })[0].submissionStatus).toBe('unknown');
  });

  it('ignores non-object items and non-array/non-object payloads gracefully', () => {
    expect(mapVendorResult(null)).toEqual([]);
    expect(mapVendorResult(undefined)).toEqual([]);
    expect(mapVendorResult('nonsense')).toEqual([]);
    expect(mapVendorResult([null, 42, 'x', { internal_ref: 'a1' }])).toHaveLength(1);
  });
});
