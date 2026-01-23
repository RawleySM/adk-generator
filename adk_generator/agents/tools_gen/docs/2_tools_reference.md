# Tools: The Agent's Capabilities

Tools extend an agent's abilities beyond text generation.

## Defining Function Tools: Principles & Best Practices

*   **Signature**: `def my_tool(param1: Type, param2: Type, tool_context: ToolContext) -> dict:`
*   **Function Name**: Descriptive verb-noun (e.g., `schedule_meeting`).
*   **Parameters**: Clear names, required type hints, **NO DEFAULT VALUES**.
*   **Return Type**: **Must** be a `dict` (JSON-serializable), preferably with a `'status'` key.
*   **Docstring**: **CRITICAL**. Explain purpose, when to use, arguments, and return value structure. **AVOID** mentioning `tool_context`.

```python
def calculate_compound_interest(
    principal: float,
    rate: float,
    years: int,
    compounding_frequency: int,
    tool_context: ToolContext
) -> dict:
    """Calculates the future value of an investment with compound interest.

    Use this tool to calculate the future value of an investment given a
    principal amount, interest rate, number of years, and how often the
    interest is compounded per year.

    Args:
        principal (float): The initial amount of money invested.
        rate (float): The annual interest rate (e.g., 0.05 for 5%).
        years (int): The number of years the money is invested.
        compounding_frequency (int): The number of times interest is compounded
                                     per year (e.g., 1 for annually, 12 for monthly).
        
    Returns:
        dict: Contains the calculation result.
              - 'status' (str): "success" or "error".
              - 'future_value' (float, optional): The calculated future value.
              - 'error_message' (str, optional): Description of error, if any.
    """
    # ... implementation ...
```

## The `ToolContext` Object: Accessing Runtime Information

`ToolContext` is the gateway for tools to interact with the ADK runtime.

*   `tool_context.state`: Read and write to the current `Session`'s `state` dictionary.
*   `tool_context.actions`: Modify the `EventActions` object (e.g., `tool_context.actions.escalate = True`).
*   `tool_context.load_artifact(filename)` / `tool_context.save_artifact(filename, part)`: Manage binary data.
*   `tool_context.search_memory(query)`: Query the long-term `MemoryService`.

## All Tool Types & Their Usage

### 1. Custom Function Tools

*   **`FunctionTool`**: The most common type, wrapping a standard Python function.
*   **`LongRunningFunctionTool`**: Wraps an `async` function that `yields` intermediate results, for tasks that provide progress updates.
*   **`AgentTool`**: Wraps another `BaseAgent` instance, allowing it to be invoked as a tool by a parent agent.

### 2. Built-in Tools

Ready-to-use tools provided by ADK.

*   `google_search`: Provides Google Search grounding.
*   **Code Execution**:
    *   `BuiltInCodeExecutor`: Local, convenient for development. **Not** for untrusted production use.
    *   `GkeCodeExecutor`: Production-grade. Executes code in ephemeral, sandboxed pods on Google Kubernetes Engine (GKE) using gVisor for isolation. Requires GKE cluster setup.
*   `VertexAiSearchTool`: Provides grounding from your private Vertex AI Search data stores.
*   `BigQueryToolset`: A collection of tools for interacting with BigQuery (e.g., `list_datasets`, `execute_sql`).

> **Warning**: An agent can only use one type of built-in tool at a time and they cannot be used in sub-agents.

### 3. Third-Party Tool Wrappers

For seamless integration with other frameworks.

*   `LangchainTool`: Wraps a tool from the LangChain ecosystem.

### 4. OpenAPI & Protocol Tools

For interacting with APIs and services.

*   **`OpenAPIToolset`**: Automatically generates a set of `RestApiTool`s from an OpenAPI (Swagger) v3 specification.
*   **`MCPToolset`**: Connects to an external Model Context Protocol (MCP) server to dynamically load its tools.

### 5. Google Cloud Tools

For deep integration with Google Cloud services.

*   **`ApiHubToolset`**: Turns any documented API from Apigee API Hub into a tool.
*   **`ApplicationIntegrationToolset`**: Turns Application Integration workflows and Integration Connectors (e.g., Salesforce, SAP) into callable tools.
*   **Toolbox for Databases**: An open-source MCP server that ADK can connect to for database interactions.

### 6. Dynamic Toolsets (`BaseToolset`)

Instead of a static list of tools, use a `Toolset` to dynamically determine which tools an agent can use based on the current context (e.g., user permissions).

```python
from google.adk.tools.base_toolset import BaseToolset

class AdminAwareToolset(BaseToolset):
    async def get_tools(self, context: ReadonlyContext) -> list[BaseTool]:
        # Check state to see if user is admin
        if context.state.get('user:role') == 'admin':
             return [admin_delete_tool, standard_query_tool]
        return [standard_query_tool]

# Usage:
agent = Agent(tools=[AdminAwareToolset()])
```

## Tool Confirmation (Human-in-the-Loop)

ADK can pause tool execution to request human or system confirmation before proceeding, essential for sensitive actions.

*   **Boolean Confirmation**: Simple yes/no via `FunctionTool(..., require_confirmation=True)`.
*   **Dynamic Confirmation**: Pass a function to `require_confirmation` to decide at runtime based on arguments.
*   **Advanced/Payload Confirmation**: Use `tool_context.request_confirmation()` inside the tool for structured feedback.

```python
from google.adk.tools import FunctionTool, ToolContext

# 1. Simple Boolean Confirmation
# Pauses execution until a 'confirmed': True/False event is received.
sensitive_tool = FunctionTool(delete_database, require_confirmation=True)

# 2. Dynamic Threshold Confirmation
def needs_approval(amount: float, **kwargs) -> bool:
    return amount > 10000

transfer_tool = FunctionTool(wire_money, require_confirmation=needs_approval)

# 3. Advanced Payload Confirmation (inside tool definition)
def book_flight(destination: str, price: float, tool_context: ToolContext):
    # Pause and ask user to select a seat class before continuing
    tool_context.request_confirmation(
        hint="Please confirm booking and select seat class.",
        payload={"seat_class": ["economy", "business", "first"]} # Expected structure
    )
    return {"status": "pending_confirmation"}
```

