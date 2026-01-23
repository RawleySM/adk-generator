# Multi-Agent Systems & Communication

Building complex applications by composing multiple, specialized agents.

## Agent Hierarchy

A hierarchical (tree-like) structure of parent-child relationships defined by the `sub_agents` parameter during `BaseAgent` initialization. An agent can only have one parent.

```python
# Conceptual Hierarchy
# Root
# └── Coordinator (LlmAgent)
#     ├── SalesAgent (LlmAgent)
#     └── SupportAgent (LlmAgent)
#     └── DataPipeline (SequentialAgent)
#         ├── DataFetcher (LlmAgent)
#         └── DataProcessor (LlmAgent)
```

## Inter-Agent Communication Mechanisms

1.  **Shared Session State (`session.state`)**: The most common and robust method. Agents read from and write to the same mutable dictionary.
    *   **Mechanism**: Agent A sets `ctx.session.state['key'] = value`. Agent B later reads `ctx.session.state.get('key')`. `output_key` on `LlmAgent` is a convenient auto-setter.
    *   **Best for**: Passing intermediate results, shared configurations, and flags in pipelines (Sequential, Loop agents).

2.  **LLM-Driven Delegation (`transfer_to_agent`)**: A `LlmAgent` can dynamically hand over control to another agent based on its reasoning.
    *   **Mechanism**: The LLM generates a special `transfer_to_agent` function call. The ADK framework intercepts this, routes the next turn to the target agent.
    *   **Prerequisites**:
        *   The initiating `LlmAgent` needs `instruction` to guide delegation and `description` of the target agent(s).
        *   Target agents need clear `description`s to help the LLM decide.
        *   Target agent must be discoverable within the current agent's hierarchy (direct `sub_agent` or a descendant).
    *   **Configuration**: Can be enabled/disabled via `disallow_transfer_to_parent` and `disallow_transfer_to_peers` on `LlmAgent`.

3.  **Explicit Invocation (`AgentTool`)**: An `LlmAgent` can treat another `BaseAgent` instance as a callable tool.
    *   **Mechanism**: Wrap the target agent (`target_agent`) in `AgentTool(agent=target_agent)` and add it to the calling `LlmAgent`'s `tools` list. The `AgentTool` generates a `FunctionDeclaration` for the LLM. When called, `AgentTool` runs the target agent and returns its final response as the tool result.
    *   **Best for**: Hierarchical task decomposition, where a higher-level agent needs a specific output from a lower-level agent.

## Delegation vs. Agent-as-a-Tool

*   **Delegation (`sub_agents`)**: The parent agent *transfers control*. The sub-agent interacts directly with the user for subsequent turns until it finishes.
*   **Agent-as-a-Tool (`AgentTool`)**: The parent agent *calls* another agent like a function. The parent remains in control, receives the sub-agent's entire interaction as a single tool result, and summarizes it for the user.

```python
# Delegation: "I'll let the specialist handle this conversation."
root = Agent(name="root", sub_agents=[specialist])

# Agent-as-a-Tool: "I need the specialist to do a task and give me the results."
from google.adk.tools import AgentTool
root = Agent(name="root", tools=[AgentTool(specialist)])
```

## Common Multi-Agent Patterns

*   **Coordinator/Dispatcher**: A central agent routes requests to specialized sub-agents (often via LLM-driven delegation).
*   **Sequential Pipeline**: `SequentialAgent` orchestrates a fixed sequence of tasks, passing data via shared state.
*   **Parallel Fan-Out/Gather**: `ParallelAgent` runs concurrent tasks, followed by a final agent that synthesizes results from state.
*   **Review/Critique (Generator-Critic)**: `SequentialAgent` with a generator followed by a critic, often in a `LoopAgent` for iterative refinement.
*   **Hierarchical Task Decomposition (Planner/Executor)**: High-level agents break down complex problems, delegating sub-tasks to lower-level agents (often via `AgentTool` and delegation).

## Example: Hierarchical Planner/Executor Pattern

This pattern combines several mechanisms. A top-level `interactive_planner_agent` uses another agent (`plan_generator`) as a tool to create a plan, then delegates the execution of that plan to a complex `SequentialAgent` (`research_pipeline`).

```python
from google.adk.agents import LlmAgent, SequentialAgent, LoopAgent
from google.adk.tools.agent_tool import AgentTool

# Assume plan_generator, section_planner, research_evaluator, etc. are defined.

# The execution pipeline itself is a complex agent.
research_pipeline = SequentialAgent(
    name="research_pipeline",
    description="Executes a pre-approved research plan. It performs iterative research, evaluation, and composes a final, cited report.",
    sub_agents=[
        section_planner,
        section_researcher,
        LoopAgent(
            name="iterative_refinement_loop",
            max_iterations=3,
            sub_agents=[
                research_evaluator,
                EscalationChecker(name="escalation_checker"),
                enhanced_search_executor,
            ],
        ),
        report_composer,
    ],
)

# The top-level agent that interacts with the user.
interactive_planner_agent = LlmAgent(
    name="interactive_planner_agent",
    model="gemini-3-flash-preview",
    description="The primary research assistant. It collaborates with the user to create a research plan, and then executes it upon approval.",
    instruction="""
    You are a research planning assistant. Your workflow is:
    1.  **Plan:** Use the `plan_generator` tool to create a draft research plan.
    2.  **Refine:** Incorporate user feedback until the plan is approved.
    3.  **Execute:** Once the user gives EXPLICIT approval (e.g., "looks good, run it"), you MUST delegate the task to the `research_pipeline` agent.
    Your job is to Plan, Refine, and Delegate. Do not do the research yourself.
    """,
    # The planner delegates to the pipeline.
    sub_agents=[research_pipeline],
    # The planner uses another agent as a tool.
    tools=[AgentTool(plan_generator)],
    output_key="research_plan",
)

# The root agent of the application is the top-level planner.
root_agent = interactive_planner_agent
```

## Distributed Communication (A2A Protocol)

The Agent-to-Agent (A2A) Protocol enables agents to communicate over a network, even if they are written in different languages or run as separate services. Use A2A for integrating with third-party agents, building microservice-based agent architectures, or when a strong, formal API contract is needed. For internal code organization, prefer local sub-agents.

*   **Exposing an Agent**: Make an existing ADK agent available to others over A2A.
    *   **`to_a2a()` Utility**: The simplest method. Wraps your `root_agent` and creates a runnable FastAPI app, auto-generating the required `agent.json` card.
        ```python
        from google.adk.a2a.utils.agent_to_a2a import to_a2a
        # root_agent is your existing ADK Agent instance
        a2a_app = to_a2a(root_agent, port=8001)
        # Run with: uvicorn your_module:a2a_app --host localhost --port 8001
        ```
    *   **`adk api_server --a2a`**: A CLI command that serves agents from a directory. Requires you to manually create an `agent.json` card for each agent you want to expose.

*   **Consuming a Remote Agent**: Use a remote A2A agent as if it were a local agent.
    *   **`RemoteA2aAgent`**: This agent acts as a client proxy. You initialize it with the URL to the remote agent's card.
        ```python
        from google.adk.a2a.remote_a2a_agent import RemoteA2aAgent

        # This agent can now be used as a sub-agent or tool
        prime_checker_agent = RemoteA2aAgent(
            name="prime_agent",
            description="A remote agent that checks if numbers are prime.",
            agent_card="http://localhost:8001/a2a/check_prime_agent/.well-known/agent.json"
        )
        ```

