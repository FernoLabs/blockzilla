/**
 * Create a promise observer that applies callbacks only for the newest request.
 *
 * @returns {<T>(
 *   request: Promise<T>,
 *   onFulfilled: (value: T) => void,
 *   onRejected: (reason: unknown) => void
 * ) => void}
 */
export function createLatestRequestObserver() {
  let latestRequestSequence = 0;

  return (request, onFulfilled, onRejected) => {
    const requestSequence = ++latestRequestSequence;
    void request.then(
      (value) => {
        if (requestSequence === latestRequestSequence) onFulfilled(value);
      },
      (reason) => {
        if (requestSequence === latestRequestSequence) onRejected(reason);
      }
    );
  };
}
