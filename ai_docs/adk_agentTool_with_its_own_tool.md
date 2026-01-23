# DeepWiki Q&A with Code Context for Repository: google/adk-python
## Q1
can the LlmAgent wrapped in an AgentTool be equipped with tools?
### Answer
**Yes**, an `LlmAgent` wrapped in an `AgentTool` can be equipped with tools.

The codebase contains a concrete example of this pattern in the `GoogleSearchAgentTool` class. The `create_google_search_agent` helper function creates an `LlmAgent` that is equipped with the `google_search` tool: adk-python:33-47 

This `LlmAgent` is then wrapped in `GoogleSearchAgentTool`, which extends `AgentTool`: adk-python:50-62 

The `AgentTool` implementation itself doesn't impose any restrictions on the wrapped agent having tools. When `run_async` is called, it creates a `Runner` for the wrapped agent and executes it normally, allowing the agent to use all its configured capabilities including any tools it has: adk-python:172-180 

## Notes

The `GoogleSearchAgentTool` was created as a workaround to support using `google_search` alongside other tools (see the TODO comment referencing issue b/448114567), but it demonstrates that the architecture fully supports wrapping tool-equipped agents. When the wrapped agent executes, it runs in its own isolated `Runner` context with full access to its tools, and the results are returned to the parent agent.
### Citations
**File:** src/google/adk/tools/google_search_agent_tool.py (L33-47)
```python
def create_google_search_agent(model: Union[str, BaseLlm]) -> LlmAgent:
  """Create a sub-agent that only uses google_search tool."""
  return LlmAgent(
      name='google_search_agent',
      model=model,
      description=(
          'An agent for performing Google search using the `google_search` tool'
      ),
      instruction="""
        You are a specialized Google search agent.

        When given a search query, use the `google_search` tool to find the related information.
      """,
      tools=[google_search],
  )
```
**File:** src/google/adk/tools/google_search_agent_tool.py (L50-62)
```python
class GoogleSearchAgentTool(AgentTool):
  """A tool that wraps a sub-agent that only uses google_search tool.

  This is a workaround to support using google_search tool with other tools.
  TODO(b/448114567): Remove once the workaround is no longer needed.

  Attributes:
    model: The model to use for the sub-agent.
  """

  def __init__(self, agent: LlmAgent):
    self.agent = agent
    super().__init__(agent=self.agent)
```
**File:** src/google/adk/tools/agent_tool.py (L172-180)
```python
    runner = Runner(
        app_name=child_app_name,
        agent=self.agent,
        artifact_service=ForwardingArtifactService(tool_context),
        session_service=InMemorySessionService(),
        memory_service=InMemoryMemoryService(),
        credential_service=tool_context._invocation_context.credential_service,
        plugins=plugins,
    )
```
## Q2
can AgentTool with no additional tools share the same runner as its parent?
### Answer

# DeepWiki Q&A with Code Context for Repository: google/adk-python
## Q1
can the LlmAgent wrapped in an AgentTool be equipped with tools?
### Answer
**Yes**, an `LlmAgent` wrapped in an `AgentTool` can be equipped with tools.

The codebase contains a concrete example of this pattern in the `GoogleSearchAgentTool` class. The `create_google_search_agent` helper function creates an `LlmAgent` that is equipped with the `google_search` tool: adk-python:33-47 

This `LlmAgent` is then wrapped in `GoogleSearchAgentTool`, which extends `AgentTool`: adk-python:50-62 

The `AgentTool` implementation itself doesn't impose any restrictions on the wrapped agent having tools. When `run_async` is called, it creates a `Runner` for the wrapped agent and executes it normally, allowing the agent to use all its configured capabilities including any tools it has: adk-python:172-180 

## Notes

The `GoogleSearchAgentTool` was created as a workaround to support using `google_search` alongside other tools (see the TODO comment referencing issue b/448114567), but it demonstrates that the architecture fully supports wrapping tool-equipped agents. When the wrapped agent executes, it runs in its own isolated `Runner` context with full access to its tools, and the results are returned to the parent agent.
### Citations
**File:** src/google/adk/tools/google_search_agent_tool.py (L33-47)
```python
def create_google_search_agent(model: Union[str, BaseLlm]) -> LlmAgent:
  """Create a sub-agent that only uses google_search tool."""
  return LlmAgent(
      name='google_search_agent',
      model=model,
      description=(
          'An agent for performing Google search using the `google_search` tool'
      ),
      instruction="""
        You are a specialized Google search agent.

        When given a search query, use the `google_search` tool to find the related information.
      """,
      tools=[google_search],
  )
```
**File:** src/google/adk/tools/google_search_agent_tool.py (L50-62)
```python
class GoogleSearchAgentTool(AgentTool):
  """A tool that wraps a sub-agent that only uses google_search tool.

  This is a workaround to support using google_search tool with other tools.
  TODO(b/448114567): Remove once the workaround is no longer needed.

  Attributes:
    model: The model to use for the sub-agent.
  """

  def __init__(self, agent: LlmAgent):
    self.agent = agent
    super().__init__(agent=self.agent)
```
**File:** src/google/adk/tools/agent_tool.py (L172-180)
```python
    runner = Runner(
        app_name=child_app_name,
        agent=self.agent,
        artifact_service=ForwardingArtifactService(tool_context),
        session_service=InMemorySessionService(),
        memory_service=InMemoryMemoryService(),
        credential_service=tool_context._invocation_context.credential_service,
        plugins=plugins,
    )
```
## Q2
can AgentTool with no additional tools share the same runner as its parent?
### Answer
