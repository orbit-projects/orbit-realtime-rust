# Security

SSE is a transport, not authentication or authorization. Protect routes with the host application's identity and authorization controls; authorize each subscription and avoid putting credentials in URLs. Enforce request, connection, and upstream subscription limits in the host service.

The package bounds event and comment sizes, rejects line injection in event names and IDs, rejects NUL in IDs, and omits payloads from event debug output. Do not log event bodies, access tokens, or raw application errors. Use HTTPS for public clients and test proxy behavior with the deployed configuration.
