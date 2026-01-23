You can prune or wipe agent-specific context data using several methods in the `InvocationContext` class, though you cannot fully wipe the entire `InvocationContext` itself since it contains essential framework data.

## Available Methods

### 1. `set_agent_state()` - Clear Individual Agent State

This method can clear state for a specific agent by calling it without parameters:

```python
# Clear agent state and end_of_agent flag for a specific agent
ctx.set_agent_state('agent_name')
``` [1](#1-0) 

### 2. `reset_sub_agent_states()` - Clear All Sub-Agent States

This method recursively clears states for all sub-agents of a given parent agent:

```python
# Clear all sub-agent states for a parent agent
ctx.reset_sub_agent_states('parent_agent_name')
``` [2](#1-1) 

### 3. Session State Management

You can directly manipulate the shared session state:

```python
# Clear specific keys from session state
if 'key_to_remove' in ctx.session.state:
    del ctx.session.state['key_to_remove']

# Clear entire session state
ctx.session.state.clear()
``` [3](#1-2) 

## Usage Examples

### Resetting Between Sequential Agents

In a `SequentialAgent`, you can reset context between sub-agents:

```python
# Before running next agent, clear previous agent's state
ctx.set_agent_state('previous_agent_name')

# Or reset all sub-agent states if you're the parent
ctx.reset_sub_agent_states('sequential_agent_name')
``` [4](#1-3) 

### End of Agent State Clearing

When `end_of_agent=True` is set, the method automatically clears the agent state:

```python
# Mark agent as finished and clear its state
ctx.set_agent_state('agent_name', end_of_agent=True)
``` [5](#1-4) 

## Limitations

You cannot fully wipe the `InvocationContext` because it contains essential framework data:
- `session` - Required for maintaining conversation state
- `invocation_id` - Required for tracking the current request
- `agent` - Required for knowing which agent is running
- Service references (`session_service`, `artifact_service`, etc.)

The methods above allow you to clear the agent-specific state data while preserving the essential framework infrastructure.

## Notes

The `reset_sub_agent_states()` method is particularly useful for workflow agents like `SequentialAgent` that need to ensure clean execution of sub-agents. The test examples show how these methods effectively clear both `agent_states` and `end_of_agents` dictionaries [6](#1-5) .

### Citations

**File:** src/google/adk/agents/invocation_context.py (L165-166)
```python
  session: Session
  """The current session of this invocation context. Readonly."""
```

**File:** src/google/adk/agents/invocation_context.py (L224-254)
```python
  def set_agent_state(
      self,
      agent_name: str,
      *,
      agent_state: Optional[BaseAgentState] = None,
      end_of_agent: bool = False,
  ) -> None:
    """Sets the state of an agent in this invocation.

    * If end_of_agent is True, will set the end_of_agent flag to True and
      clear the agent_state.
    * Otherwise, if agent_state is not None, will set the agent_state and
      reset the end_of_agent flag to False.
    * Otherwise, will clear the agent_state and end_of_agent flag, to allow the
      agent to re-run.

    Args:
      agent_name: The name of the agent.
      agent_state: The state of the agent. Will be ignored if end_of_agent is
        True.
      end_of_agent: Whether the agent has finished running.
    """
    if end_of_agent:
      self.end_of_agents[agent_name] = True
      self.agent_states.pop(agent_name, None)
    elif agent_state is not None:
      self.agent_states[agent_name] = agent_state.model_dump(mode="json")
      self.end_of_agents[agent_name] = False
    else:
      self.end_of_agents.pop(agent_name, None)
      self.agent_states.pop(agent_name, None)
```

**File:** src/google/adk/agents/invocation_context.py (L256-273)
```python
  def reset_sub_agent_states(
      self,
      agent_name: str,
  ) -> None:
    """Resets the state of all sub-agents of the given agent in this invocation.

    Args:
      agent_name: The name of the agent whose sub-agent states need to be reset.
    """
    agent = self.agent.find_agent(agent_name)
    if not agent:
      return

    for sub_agent in agent.sub_agents:
      # Reset the sub-agent's state in the context to ensure that each
      # sub-agent starts fresh.
      self.set_agent_state(sub_agent.name)
      self.reset_sub_agent_states(sub_agent.name)
```

**File:** tests/unittests/agents/test_invocation_context.py (L380-391)
```python
  def test_reset_agent_state(self):
    """Tests that set_agent_state clears agent_state and end_of_agent."""
    invocation_context = self._create_test_invocation_context(
        ResumabilityConfig(is_resumable=True)
    )
    invocation_context.agent_states['agent1'] = {}
    invocation_context.end_of_agents['agent1'] = True

    # Reset state, which should clear the agent_state and end_of_agent flag.
    invocation_context.set_agent_state('agent1')
    assert 'agent1' not in invocation_context.agent_states
    assert 'agent1' not in invocation_context.end_of_agents
```

**File:** tests/unittests/agents/test_invocation_context.py (L393-425)
```python
  def test_reset_sub_agent_states(self):
    """Tests that reset_sub_agent_states resets sub-agent states."""
    sub_sub_agent_1 = BaseAgent(name='sub_sub_agent_1')
    sub_agent_1 = BaseAgent(name='sub_agent_1', sub_agents=[sub_sub_agent_1])
    sub_agent_2 = BaseAgent(name='sub_agent_2')
    root_agent = BaseAgent(
        name='root_agent', sub_agents=[sub_agent_1, sub_agent_2]
    )

    invocation_context = self._create_test_invocation_context(
        ResumabilityConfig(is_resumable=True)
    )
    invocation_context.agent = root_agent
    invocation_context.set_agent_state(
        'sub_agent_1', agent_state=BaseAgentState()
    )
    invocation_context.set_agent_state('sub_agent_2', end_of_agent=True)
    invocation_context.set_agent_state(
        'sub_sub_agent_1', agent_state=BaseAgentState()
    )

    assert 'sub_agent_1' in invocation_context.agent_states
    assert 'sub_agent_2' in invocation_context.end_of_agents
    assert 'sub_sub_agent_1' in invocation_context.agent_states

    invocation_context.reset_sub_agent_states('root_agent')

    assert 'sub_agent_1' not in invocation_context.agent_states
    assert 'sub_agent_1' not in invocation_context.end_of_agents
    assert 'sub_agent_2' not in invocation_context.agent_states
    assert 'sub_agent_2' not in invocation_context.end_of_agents
    assert 'sub_sub_agent_1' not in invocation_context.agent_states
    assert 'sub_sub_agent_1' not in invocation_context.end_of_agents
```
