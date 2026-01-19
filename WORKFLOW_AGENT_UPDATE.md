# Workflow Agent Update - ADK Generator

## Summary

Updated the ADK Generator's design agent to interpret user requirements into ADK Workflow Agent architectures, using workflow agents (SequentialAgent, LoopAgent, ParallelAgent) as the primary building blocks.

## Changes Made

### 1. Enhanced Design Agent (`agents/design_agent.py`)

**Key Improvements**:
- Added comprehensive documentation for all 5 ADK agent types:
  - **LlmAgent**: LLM-powered reasoning and decision-making
  - **SequentialAgent**: Execute sub-agents in strict order
  - **LoopAgent**: Iterative execution with termination conditions
  - **ParallelAgent**: Concurrent execution for speed
  - **CustomAgent**: Arbitrary orchestration logic

- Included detailed configuration options for each agent type
- Added "when to use" guidelines for each workflow pattern
- Provided complete example interpretation for literature review system

**New Capabilities**:
- Interprets natural language requirements into workflow architectures
- Identifies appropriate workflow patterns (sequential, loop, parallel, nested)
- Composes LlmAgents within workflow agents
- Shows data flow via state keys (`output_key`, state injection)
- Generates architecture diagrams, pseudocode, and flowcharts

### 2. Created Workflow Agent Templates

**New Templates** (`templates/workflow_agents/`):
- `sequential_agent.py.jinja2` - For sequential workflows
- `loop_agent.py.jinja2` - For iterative workflows
- `parallel_agent.py.jinja2` - For concurrent execution
- `nested_workflow.py.jinja2` - For complex nested workflows

**Template Features**:
- Support for multiple sub-agents
- Configurable `output_key` for state management
- Tool integration
- `max_iterations` for LoopAgent
- Nested workflow composition

### 3. Updated Base Agent Template

**Enhanced** `templates/base_agent/agent.py.jinja2`:
- Conditional imports based on agent type
- Support for LlmAgent, SequentialAgent, LoopAgent, ParallelAgent
- Nested workflow agent generation
- Sub-agent definitions with full configuration
- State key management

## Example: Literature Review System

The design agent can now interpret complex requirements like:

**User Request**:
> "A series of agents for conducting a research literature review, where the first agent is passed a topics list and web searches the first topic and builds a terminology map that is passed to a swarm of agents that take the map and fan out to expand the search for other terms, compiling results into appropriately named sub-directories, which are then organized into a single directory and documented with a README.md by a third agent. This process loops around to the research list's second topic, and so forth until the list is complete."

**Interpreted Architecture**:
```
literature_reviewer (LoopAgent) - loops over topics list
├── terminology_mapper (LlmAgent) - searches first topic, builds term map
├── swarm_launcher (ParallelAgent) - fans out to search terms
│   ├── topic_searcher_1 (LlmAgent) - searches term 1
│   ├── topic_searcher_2 (LlmAgent) - searches term 2
│   ├── topic_searcher_3 (LlmAgent) - searches term 3
│   └── topic_searcher_4 (LlmAgent) - searches term 4
└── directory_organizer (LlmAgent) - organizes results, creates README
```

## Key Design Principles

1. **Workflow agents as building blocks**: Always compose using SequentialAgent, LoopAgent, ParallelAgent, and LlmAgent

2. **State management**: Use `output_key` on sub-agents to store results, reference them in subsequent agents via `{key_name}`

3. **Clear naming**: Descriptive, snake_case names reflecting agent purpose

4. **Explicit data flow**: Show how data passes between agents via state keys

5. **User approval required**: Always present design and wait for approval before proceeding

## Workflow Pattern Guidelines

### Use SequentialAgent when:
- Tasks must happen in order
- Output of one step feeds into next
- Example: "First do X, then Y, then Z"

### Use LoopAgent when:
- Process repeats over a list
- Iterative refinement needed
- Example: "For each topic..." or "Keep improving until..."

### Use ParallelAgent when:
- Tasks are independent
- Speed is important
- Example: "Search multiple sources simultaneously" or "Fan out to process..."

### Use CustomAgent when:
- Conditional logic based on results
- Dynamic agent selection
- Example: "If X, then do Y, otherwise do Z"

## ADK Workflow Agent Configuration Reference

### SequentialAgent
```python
SequentialAgent(
    name="agent_name",
    sub_agents=[agent1, agent2, agent3],  # Ordered list
    description="Sequential workflow"
)
```

### LoopAgent
```python
LoopAgent(
    name="agent_name",
    sub_agents=[agent1, agent2],
    max_iterations=5,  # Optional
    description="Iterative workflow"
)
```

### ParallelAgent
```python
ParallelAgent(
    name="agent_name",
    sub_agents=[agent1, agent2, agent3],
    description="Parallel workflow"
)
```

### LlmAgent
```python
LlmAgent(
    name="agent_name",
    model="gemini-2.5-flash",
    instruction="You are a...",
    description="Agent description",
    tools=[tool1, tool2],
    output_key="result_key"
)
```

## State Management

**Storing Results**:
```python
agent1 = LlmAgent(
    name="agent1",
    output_key="agent1_result",  # Stores in state
    ...
)
```

**Accessing Results**:
```python
agent2 = LlmAgent(
    name="agent2",
    instruction="Process {agent1_result}...",  # Injects from state
    ...
)
```

## Files Modified

1. `agents/design_agent.py` - Enhanced with workflow agent interpretation
2. `templates/base_agent/agent.py.jinja2` - Updated to support workflow agents
3. `templates/workflow_agents/sequential_agent.py.jinja2` - New
4. `templates/workflow_agents/loop_agent.py.jinja2` - New
5. `templates/workflow_agents/parallel_agent.py.jinja2` - New
6. `templates/workflow_agents/nested_workflow.py.jinja2` - New
7. `IMPLEMENTATION_SUMMARY.md` - Added

## Testing

To test the updated design agent:

```bash
cd /home/ubuntu/adk-generator

# Run the generator with a workflow-based request
python -m adk_generator.cli

# Example request:
# "Create an agent that loops through a list of topics, 
#  searches each topic in parallel using 3 agents, 
#  and then summarizes the results sequentially"
```

## Next Steps

1. **Push to GitHub**: Update the remote repository with these changes
2. **Test Generation**: Verify that workflow agents generate correctly
3. **Documentation**: Update README with workflow agent examples
4. **Examples**: Create sample projects demonstrating each workflow pattern

## Commit Message

```
Update design agent with workflow agent interpretation and add workflow templates

- Enhanced design agent to interpret user requirements into ADK workflow architectures
- Added detailed documentation for LlmAgent, SequentialAgent, LoopAgent, ParallelAgent, CustomAgent
- Created workflow agent templates (sequential, loop, parallel, nested)
- Updated base agent template to support workflow agent types
- Added example interpretation for literature review system
```

## Repository

**Local**: `/home/ubuntu/adk-generator/`
**Remote**: `https://github.com/RawleySM/adk-generator` (pending push)

## Status

✅ Design agent updated with workflow interpretation
✅ Workflow agent templates created
✅ Base agent template updated
✅ Changes committed locally
⏳ Pending push to remote (authentication issue)
