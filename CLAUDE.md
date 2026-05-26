# Project Guidance

## Testing

- Use [testify](https://github.com/stretchr/testify) (`assert` and `require`) for assertions in tests.
- Do not use magic sleeps (`time.Sleep`, or `time.After` timeouts whose duration encodes an assumption about scheduling) to coordinate goroutines or "wait for" state changes. Synchronize through channels or observable state instead. `context.WithTimeout` is fine when the timeout itself is the thing under test.
