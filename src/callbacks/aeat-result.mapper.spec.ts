import { mapVendorResult } from './aeat-result.mapper';

describe('mapVendorResult', () => {
  it('normalizes a single result object into a one-item array', () => {
    const result = mapVendorResult({
      state: 'ERROR',
      errorCode: '4114',
      invoiceId: 'abc123',
      timestamp: '2026-07-08T09:13:37.078500820Z',
      aeatResponse: 'Codigo[4114]...',
    });

    expect(result).toHaveLength(1);
    expect(result[0].externalRef).toBe('abc123');
    expect(result[0].submissionStatus).toBe('ERROR');
    expect(result[0].raw).toMatchObject({ invoiceId: 'abc123', state: 'ERROR' });
  });

  it('passes an array of results through as one item per entry (batch callback)', () => {
    const result = mapVendorResult([
      { state: 'CORRECTO', invoiceId: 'a1' },
      { state: 'ERROR', invoiceId: 'a2' },
    ]);

    expect(result.map((r) => r.externalRef)).toEqual(['a1', 'a2']);
    expect(result.map((r) => r.submissionStatus)).toEqual(['CORRECTO', 'ERROR']);
  });

  it('skips items with no invoiceId — nothing to correlate them to', () => {
    const result = mapVendorResult([{ state: 'ERROR' }, { state: 'CORRECTO', invoiceId: 'a2' }]);

    expect(result).toHaveLength(1);
    expect(result[0].externalRef).toBe('a2');
  });

  it('defaults submissionStatus to "unknown" when state is missing or not a string', () => {
    expect(mapVendorResult({ invoiceId: 'a1' })[0].submissionStatus).toBe('unknown');
    expect(mapVendorResult({ invoiceId: 'a1', state: 42 })[0].submissionStatus).toBe('unknown');
  });

  it('ignores non-object items and non-array/non-object payloads gracefully', () => {
    expect(mapVendorResult(null)).toEqual([]);
    expect(mapVendorResult(undefined)).toEqual([]);
    expect(mapVendorResult('nonsense')).toEqual([]);
    expect(mapVendorResult([null, 42, 'x', { invoiceId: 'a1' }])).toHaveLength(1);
  });
});
