Can the context of an agent or sub-agent be pruned, i.e. not totally eliminated but reduced through a function that removes certain in-memory documents that do not pertain to the agent?

### Answer
Yes, ADK provides multiple mechanisms to prune agent context by filtering out irrelevant events and documents. These systems work together to reduce the context passed to each agent without completely eliminating it.

## Context Pruning Mechanisms

### 1. Branch-Based Isolation

The `branch` field in `InvocationContext` creates isolated conversation paths, preventing agents from seeing peer conversations adk-python:152-160 :

```python
# Format: agent_1.agent_2.agent_3 (parent.child.grandchild)
# Child agents can see parent events, but not sibling events
```

### 2. Event Filtering Functions

The `_should_include_event_in_context` function applies multiple filters before sending events to the LLM adk-python:261-283 :

- Removes empty content events
- Filters by branch membership
- Excludes internal framework events
- Removes authentication and confirmation events

### 3. Branch Membership Logic

The `_is_event_belongs_to_branch` function ensures events only belong to the current agent's branch or its ancestors adk-python:633-648 :

```python
# Returns True only if:
# - Perfect branch match, OR
# - Current branch starts with event branch + '.'
```

### 4. Empty Content Filtering

Events with no meaningful content are automatically excluded adk-python:235-258 :

- Events with only thoughts (marked as invisible)
- Events with empty text, no function calls, or no transcriptions
- State-only change events

## Practical Examples

### Sibling Agent Isolation
Tests show that sibling agents cannot see each other's events adk-python:91-142 :

```python
# child_agent1 on branch "parent_agent.child_agent1"
# Cannot see events from child_agent2 on branch "parent_agent.child_agent2"
```

### Hierarchical Access
Child agents can see all ancestor events but not peer events adk-python:194-250 :

```python
# grandchild_agent sees events from:
# - grandparent_agent
# - parent_agent  
# - itself
# But NOT sibling_agent
```

### Rewind Functionality
Events can be removed from context using rewind actions adk-python:380-434 :

```python
# Events before rewind_before_invocation_id are excluded
```

## Notes

These pruning mechanisms are applied automatically when building the LLM request context. The branch system is particularly effective for multi-agent workflows where each agent should only see relevant conversation history. There isn't a single manual "prune" function - instead, ADK uses declarative filtering through branch assignment and automatic content filtering.

Wiki pages you might want to explore:
- [Agent System (google/adk-python)](/wiki/google/adk-python#3)
### Citations
**File:** src/google/adk/agents/invocation_context.py (L152-160)
```python
  branch: Optional[str] = None
  """The branch of the invocation context.

  The format is like agent_1.agent_2.agent_3, where agent_1 is the parent of
  agent_2, and agent_2 is the parent of agent_3.

  Branch is used when multiple sub-agents shouldn't see their peer agents'
  conversation history.
  """
```
**File:** src/google/adk/flows/llm_flows/contents.py (L235-258)
```python
def _contains_empty_content(event: Event) -> bool:
  """Check if an event should be skipped due to missing or empty content.

  This can happen to the events that only changed session state.
  When both content and transcriptions are empty, the event will be considered
  as empty. The content is considered empty if none of its parts contain text,
  inline data, file data, function call, function response, executable code, or
  code execution result. Parts with only thoughts are also considered empty.

  Args:
    event: The event to check.

  Returns:
    True if the event should be skipped, False otherwise.
  """
  if event.actions and event.actions.compaction:
    return False

  return (
      not event.content
      or not event.content.role
      or not event.content.parts
      or all(_is_part_invisible(p) for p in event.content.parts)
  ) and (not event.output_transcription and not event.input_transcription)
```
**File:** src/google/adk/flows/llm_flows/contents.py (L261-283)
```python
def _should_include_event_in_context(
    current_branch: Optional[str], event: Event
) -> bool:
  """Determines if an event should be included in the LLM context.

  This filters out events that are considered empty (e.g., no text, function
  calls, or transcriptions), do not belong to the current agent's branch, or
  are internal events like authentication or confirmation requests.

  Args:
    current_branch: The current branch of the agent.
    event: The event to filter.

  Returns:
    True if the event should be included in the context, False otherwise.
  """
  return not (
      _contains_empty_content(event)
      or not _is_event_belongs_to_branch(current_branch, event)
      or _is_adk_framework_event(event)
      or _is_auth_event(event)
      or _is_request_confirmation_event(event)
  )
```
**File:** src/google/adk/flows/llm_flows/contents.py (L633-648)
```python
def _is_event_belongs_to_branch(
    invocation_branch: Optional[str], event: Event
) -> bool:
  """Check if an event belongs to the current branch.

  This is for event context segregation between agents. E.g. agent A shouldn't
  see output of agent B.
  """
  if not invocation_branch or not event.branch:
    return True
  # We use dot to delimit branch nodes. To avoid simple prefix match
  # (e.g. agent_0 unexpectedly matching agent_00), require either perfect branch
  # match, or match prefix with an additional explicit '.'
  return invocation_branch == event.branch or invocation_branch.startswith(
      f'{event.branch}.'
  )
```
**File:** tests/unittests/flows/llm_flows/test_contents_branch.py (L91-142)
```python
@pytest.mark.asyncio
async def test_branch_filtering_excludes_sibling_agents():
  """Test that sibling agents cannot see each other's events."""
  agent = Agent(model="gemini-2.5-flash", name="child_agent1")
  llm_request = LlmRequest(model="gemini-2.5-flash")
  invocation_context = await testing_utils.create_invocation_context(
      agent=agent
  )
  # Set current branch as first child
  invocation_context.branch = "parent_agent.child_agent1"

  # Add events from parent, current child, and sibling child
  events = [
      Event(
          invocation_id="inv1",
          author="user",
          content=types.UserContent("User message"),
      ),
      Event(
          invocation_id="inv2",
          author="parent_agent",
          content=types.ModelContent("Parent response"),
          branch="parent_agent",  # Parent - should be included
      ),
      Event(
          invocation_id="inv3",
          author="child_agent1",
          content=types.ModelContent("Child1 response"),
          branch="parent_agent.child_agent1",  # Current - should be included
      ),
      Event(
          invocation_id="inv4",
          author="child_agent2",
          content=types.ModelContent("Sibling response"),
          branch="parent_agent.child_agent2",  # Sibling - should be excluded
      ),
  ]
  invocation_context.session.events = events

  # Process the request
  async for _ in request_processor.run_async(invocation_context, llm_request):
    pass

  # Verify sibling events are excluded, but parent and current agent events included
  assert len(llm_request.contents) == 3
  assert llm_request.contents[0] == types.UserContent("User message")
  assert llm_request.contents[1].role == "user"
  assert llm_request.contents[1].parts == [
      types.Part(text="For context:"),
      types.Part(text="[parent_agent] said: Parent response"),
  ]
  assert llm_request.contents[2] == types.ModelContent("Child1 response")
```
**File:** tests/unittests/flows/llm_flows/test_contents_branch.py (L194-250)
```python
@pytest.mark.asyncio
async def test_branch_filtering_grandchild_sees_grandparent():
  """Test that deeply nested child agents can see all ancestor events."""
  agent = Agent(model="gemini-2.5-flash", name="grandchild_agent")
  llm_request = LlmRequest(model="gemini-2.5-flash")
  invocation_context = await testing_utils.create_invocation_context(
      agent=agent
  )
  # Set deeply nested branch: grandparent.parent.grandchild
  invocation_context.branch = "grandparent_agent.parent_agent.grandchild_agent"

  # Add events from all levels of hierarchy
  events = [
      Event(
          invocation_id="inv1",
          author="grandparent_agent",
          content=types.ModelContent("Grandparent response"),
          branch="grandparent_agent",
      ),
      Event(
          invocation_id="inv2",
          author="parent_agent",
          content=types.ModelContent("Parent response"),
          branch="grandparent_agent.parent_agent",
      ),
      Event(
          invocation_id="inv3",
          author="grandchild_agent",
          content=types.ModelContent("Grandchild response"),
          branch="grandparent_agent.parent_agent.grandchild_agent",
      ),
      Event(
          invocation_id="inv4",
          author="sibling_agent",
          content=types.ModelContent("Sibling response"),
          branch="grandparent_agent.parent_agent.sibling_agent",
      ),
  ]
  invocation_context.session.events = events

  # Process the request
  async for _ in request_processor.run_async(invocation_context, llm_request):
    pass

  # Verify only ancestors and current level are included
  assert len(llm_request.contents) == 3
  assert llm_request.contents[0].role == "user"
  assert llm_request.contents[0].parts == [
      types.Part(text="For context:"),
      types.Part(text="[grandparent_agent] said: Grandparent response"),
  ]
  assert llm_request.contents[1].role == "user"
  assert llm_request.contents[1].parts == [
      types.Part(text="For context:"),
      types.Part(text="[parent_agent] said: Parent response"),
  ]
  assert llm_request.contents[2] == types.ModelContent("Grandchild response")
```
**File:** tests/unittests/flows/llm_flows/test_contents.py (L380-434)
```python
@pytest.mark.asyncio
async def test_rewind_events_are_filtered_out():
  """Test that events are filtered based on rewind action."""
  agent = Agent(model="gemini-2.5-flash", name="test_agent")
  llm_request = LlmRequest(model="gemini-2.5-flash")
  invocation_context = await testing_utils.create_invocation_context(
      agent=agent
  )

  events = [
      Event(
          invocation_id="inv1",
          author="user",
          content=types.UserContent("First message"),
      ),
      Event(
          invocation_id="inv1",
          author="test_agent",
          content=types.ModelContent("First response"),
      ),
      Event(
          invocation_id="inv2",
          author="user",
          content=types.UserContent("Second message"),
      ),
      Event(
          invocation_id="inv2",
          author="test_agent",
          content=types.ModelContent("Second response"),
      ),
      Event(
          invocation_id="rewind_inv",
          author="test_agent",
          actions=EventActions(rewind_before_invocation_id="inv2"),
      ),
      Event(
          invocation_id="inv3",
          author="user",
          content=types.UserContent("Third message"),
      ),
  ]
  invocation_context.session.events = events

  # Process the request
  async for _ in contents.request_processor.run_async(
      invocation_context, llm_request
  ):
    pass

  # Verify rewind correctly filters conversation history
  assert llm_request.contents == [
      types.UserContent("First message"),
      types.ModelContent("First response"),
      types.UserContent("Third message"),
  ]
```
## Q5
are these only natively built into the library, or can a function in a before_agent_callback use them to filter an in-memory document that the prior agent pulled into its invocation context
### Answer
The context pruning mechanisms are both natively built into the library AND accessible through `before_agent_callback` functions for custom filtering.

## Built-in vs Callback-based Pruning

### Native Library Pruning
The automatic pruning mechanisms (branch filtering, event filtering, empty content removal) are built into the library and applied when constructing LLM requests adk-python:261-283 . These work transparently without user intervention.

### Callback-based Custom Pruning
A `before_agent_callback` can implement additional filtering by manipulating the context through `CallbackContext`:

```python
def filter_documents_callback(callback_context: CallbackContext) -> Optional[types.Content]:
    # Access session state where prior agent may have stored documents
    state = callback_context.state.to_dict()
    
    # Filter documents based on relevance to current agent
    if 'retrieved_docs' in state:
        filtered_docs = [doc for doc in state['retrieved_docs'] 
                        if is_relevant_to_agent(doc, callback_context.agent_name)]
        callback_context.state['retrieved_docs'] = filtered_docs
    
    return None  # Allow agent to proceed with filtered context
``` adk-python:136-149 

## CallbackContext Capabilities

The `CallbackContext` provides several methods for context manipulation adk-python:5020-5046 :

- **Mutable state access**: Read/write to `callback_context.state`
- **Artifact operations**: `load_artifact()` and `save_artifact()` for document management
- **Invocation details**: Access to `invocation_id`, `agent_name`, and branch information

## Implementation Example

Here's how you could filter documents a prior agent stored:

```python
def before_agent_with_filtering(callback_context: CallbackContext) -> None:
    # Remove documents not relevant to this agent
    agent_name = callback_context.agent_name
    
    # Filter based on agent-specific criteria
    if 'shared_documents' in callback_context.state:
        docs = callback_context.state['shared_documents']
        relevant_docs = [d for d in docs if d.get('target_agent') == agent_name]
        callback_context.state['shared_documents'] = relevant_docs
``` adk-python:53-56 

## Notes

The built-in pruning mechanisms handle conversation history and event filtering automatically, while `before_agent_callback` extends this capability to application-specific data filtering. Both approaches work together - the callback can prepare/modify state before the built-in mechanisms apply their filters when constructing the LLM request.
### Citations
**File:** src/google/adk/agents/base_agent.py (L136-149)
```python
  before_agent_callback: Optional[BeforeAgentCallback] = None
  """Callback or list of callbacks to be invoked before the agent run.

  When a list of callbacks is provided, the callbacks will be called in the
  order they are listed until a callback does not return None.

  Args:
    callback_context: MUST be named 'callback_context' (enforced).

  Returns:
    Optional[types.Content]: The content to return to the user.
      When the content is present, the agent run will be skipped and the
      provided content will be returned to user.
  """
```
**File:** tests/unittests/agents/test_base_agent.py (L53-56)
```python
def _before_agent_callback_bypass_agent(
    callback_context: CallbackContext,
) -> types.Content:
  return types.Content(parts=[types.Part(text='agent run is bypassed.')])
```