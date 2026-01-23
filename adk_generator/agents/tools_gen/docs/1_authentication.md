# Authentication for Tools

Enabling agents to securely access protected external resources.

## Core Concepts: `AuthScheme` & `AuthCredential`

*   **`AuthScheme`**: Defines *how* an API expects authentication (e.g., `APIKey`, `HTTPBearer`, `OAuth2`, `OpenIdConnectWithConfig`).
*   **`AuthCredential`**: Holds *initial* information to *start* the auth process (e.g., API key value, OAuth client ID/secret).

## Interactive OAuth/OIDC Flows

When a tool requires user interaction (OAuth consent), ADK pauses and signals your `Agent Client` application.

1.  **Detect Auth Request**: `runner.run_async()` yields an event with a special `adk_request_credential` function call.
2.  **Redirect User**: Extract `auth_uri` from `auth_config` in the event. Your client app redirects the user's browser to this `auth_uri` (appending `redirect_uri`).
3.  **Handle Callback**: Your client app has a pre-registered `redirect_uri` to receive the user after authorization. It captures the full callback URL (containing `authorization_code`).
4.  **Send Auth Result to ADK**: Your client prepares a `FunctionResponse` for `adk_request_credential`, setting `auth_config.exchanged_auth_credential.oauth2.auth_response_uri` to the captured callback URL.
5.  **Resume Execution**: `runner.run_async()` is called again with this `FunctionResponse`. ADK performs the token exchange, stores the access token, and retries the original tool call.

## Custom Tool Authentication

If building a `FunctionTool` that needs authentication:

1.  **Check for Cached Creds**: `tool_context.state.get("my_token_cache_key")`.
2.  **Check for Auth Response**: `tool_context.get_auth_response(my_auth_config)`.
3.  **Initiate Auth**: If no creds, call `tool_context.request_credential(my_auth_config)` and return a pending status. This triggers the external flow.
4.  **Cache Credentials**: After obtaining, store in `tool_context.state`.
5.  **Make API Call**: Use the valid credentials (e.g., `google.oauth2.credentials.Credentials`).

## Example: Tool with API Key Authentication

```python
from google.adk.tools import FunctionTool, ToolContext

def call_secure_api(query: str, tool_context: ToolContext) -> dict:
    """Call a secure external API that requires an API key."""
    
    # Check for cached API key
    api_key = tool_context.state.get("secure_api_key")
    
    if not api_key:
        return {
            "status": "error",
            "message": "API key not configured. Please set 'secure_api_key' in state."
        }
    
    try:
        import requests
        response = requests.get(
            "https://api.example.com/query",
            params={"q": query},
            headers={"Authorization": f"Bearer {api_key}"}
        )
        return {"status": "success", "data": response.json()}
    except Exception as e:
        return {"status": "error", "message": str(e)}
```

## Example: Tool with OAuth2 Authentication

```python
from google.adk.tools import FunctionTool, ToolContext
from google.adk.auth import OAuth2AuthConfig

OAUTH_CONFIG = OAuth2AuthConfig(
    client_id="your-client-id",
    client_secret="your-client-secret",
    scopes=["read:data", "write:data"],
    auth_uri="https://provider.com/oauth/authorize",
    token_uri="https://provider.com/oauth/token"
)

def access_protected_resource(resource_id: str, tool_context: ToolContext) -> dict:
    """Access a protected resource requiring OAuth2."""
    
    # Check for existing auth response
    auth_response = tool_context.get_auth_response(OAUTH_CONFIG)
    
    if auth_response:
        # We have valid credentials, make the API call
        access_token = auth_response.access_token
        try:
            import requests
            response = requests.get(
                f"https://api.provider.com/resources/{resource_id}",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            return {"status": "success", "data": response.json()}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    else:
        # No credentials, initiate OAuth flow
        tool_context.request_credential(OAUTH_CONFIG)
        return {"status": "pending_auth", "message": "OAuth authorization required"}
```

## Security Best Practices

1. **Never hardcode credentials** in tool implementations
2. **Use environment variables** or secret managers for API keys
3. **Implement proper token refresh** for OAuth2 flows
4. **Validate all inputs** before making authenticated requests
5. **Log authentication events** for audit purposes (without logging credentials)
6. **Use the most restrictive scopes** possible for OAuth2

