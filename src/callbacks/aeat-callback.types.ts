export interface AeatCallbackJobData {
  /** Whatever JSON body the vendor sent — single result object or an array; see aeat-result.mapper.ts. */
  payload: unknown;
}
