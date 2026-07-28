import { mapVendorResult } from './sii-result.mapper';

describe('mapVendorResult', () => {
  it('normalizes a single flat result object into one lineItem', () => {
    const result = mapVendorResult({
      state: 'ERROR',
      errorCode: '4114',
      internal_ref: 'abc123',
      timestamp: '2026-07-08T09:13:37.078500820Z',
      siiResponse: 'Codigo[4114]...',
    });

    expect(result.lineItems).toHaveLength(1);
    expect(result.batchItems).toHaveLength(0);
    expect(result.lineItems[0].internalRef).toBe('abc123');
    expect(result.lineItems[0].submissionStatus).toBe('ERROR');
    expect(result.lineItems[0].raw).toMatchObject({ internal_ref: 'abc123', state: 'ERROR' });
  });

  it('passes an array of flat results through as one lineItem per entry (batch callback)', () => {
    const result = mapVendorResult([
      { state: 'CORRECTO', internal_ref: 'a1' },
      { state: 'ERROR', internal_ref: 'a2' },
    ]);

    expect(result.lineItems.map((r) => r.internalRef)).toEqual(['a1', 'a2']);
    expect(result.lineItems.map((r) => r.submissionStatus)).toEqual(['CORRECTO', 'ERROR']);
  });

  it('skips items with no internal_ref and no batch_ref — nothing to correlate them to', () => {
    const result = mapVendorResult([{ state: 'ERROR' }, { state: 'CORRECTO', internal_ref: 'a2' }]);

    expect(result.lineItems).toHaveLength(1);
    expect(result.lineItems[0].internalRef).toBe('a2');
  });

  it('defaults submissionStatus to "unknown" when state is missing or not a string', () => {
    expect(mapVendorResult({ internal_ref: 'a1' }).lineItems[0].submissionStatus).toBe('unknown');
    expect(mapVendorResult({ internal_ref: 'a1', state: 42 }).lineItems[0].submissionStatus).toBe('unknown');
  });

  it('ignores non-object items and non-array/non-object payloads gracefully', () => {
    expect(mapVendorResult(null)).toEqual({ lineItems: [], batchItems: [] });
    expect(mapVendorResult(undefined)).toEqual({ lineItems: [], batchItems: [] });
    expect(mapVendorResult('nonsense')).toEqual({ lineItems: [], batchItems: [] });
    expect(mapVendorResult([null, 42, 'x', { internal_ref: 'a1' }]).lineItems).toHaveLength(1);
  });

  it('flattens a collapsed submission\'s nested rows into individual lineItems, inheriting the parent state', () => {
    const result = mapVendorResult({
      batch_ref: 'batch-1',
      state: 'CORRECTO',
      nif: '12345678A',
      rows: [
        { internal_ref: 'r1' },
        { internal_ref: 'r2', state: 'ERROR', errorCode: '4114' },
      ],
    });

    expect(result.batchItems).toHaveLength(0);
    expect(result.lineItems).toHaveLength(2);
    expect(result.lineItems[0]).toMatchObject({ internalRef: 'r1', submissionStatus: 'CORRECTO' });
    expect(result.lineItems[1]).toMatchObject({ internalRef: 'r2', submissionStatus: 'ERROR', errorCode: '4114' });
  });

  it('detects the nested-rows array regardless of its key name', () => {
    const result = mapVendorResult({
      batch_ref: 'batch-1',
      state: 'CORRECTO',
      lineas: [{ internal_ref: 'r1' }],
    });

    expect(result.lineItems).toHaveLength(1);
    expect(result.lineItems[0].internalRef).toBe('r1');
  });

  it('falls back to a batchItem when there is neither a per-line breakdown nor an internal_ref', () => {
    const result = mapVendorResult({ batch_ref: 'batch-1', state: 'CORRECTO' });

    expect(result.lineItems).toHaveLength(0);
    expect(result.batchItems).toHaveLength(1);
    expect(result.batchItems[0]).toMatchObject({ batchRef: 'batch-1', submissionStatus: 'CORRECTO' });
  });
});
