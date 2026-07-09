export interface SiiCallbackJobData {
  /** Whatever JSON body the vendor sent — single result object or an array; see sii-result.mapper.ts. */
  payload: unknown;
}
