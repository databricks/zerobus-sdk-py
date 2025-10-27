# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- Added `HeadersProvider` abstraction for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

### Documentation

- Added Azure workspace and endpoint URL examples

### Internal Changes

### API Changes

- Added `HeadersProvider` abstract base class for custom header strategies
- Added `OAuthHeadersProvider` class for OAuth 2.0 authentication with Databricks OIDC endpoint
