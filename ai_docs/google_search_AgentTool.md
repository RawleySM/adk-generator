# Limitations for ADK tools

Some ADK tools have limitations that can impact how you implement them within an
agent workflow. This page lists these tool limitations and workarounds, if available.

## One tool per agent limitation {#one-tool-one-agent}

In general, you can use more than one tool in an agent, but use of specific
tools within an agent excludes the use of any other tools in that agent. The
following ADK Tools can only be used by themselves, without any other tools, in
a single agent object:

*   [Code Execution](/adk-docs/tools/gemini-api/code-execution/) with Gemini API
*   [Google Search](/adk-docs/tools/gemini-api/google-search/) with Gemini API
*   [Vertex AI Search](/adk-docs/tools/google-cloud/vertex-ai-search/)

For example, the following approach that uses one of these tools along with
other tools, within a single agent, is ***not supported***:

=== "Python"

    ```py
    root_agent = Agent(
        name="RootAgent",
        model="gemini-2.5-flash",
        description="Code Agent",
        tools=[custom_function],
        code_executor=BuiltInCodeExecutor() # <-- NOT supported when used with tools
    )
    ```

### Workaround #1: AgentTool.create() method

<div class="language-support-tag">
  <span class="lst-supported">Supported in ADK</span><span class="lst-python">Python</span><span class="lst-java">Java</span>
</div>

The following code sample demonstrates how to use multiple built-in tools or how
to use built-in tools with other tools by using multiple agents:

=== "Python"

    ```py
    from google.adk.tools.agent_tool import AgentTool
    from google.adk.agents import Agent
    from google.adk.tools import google_search
    from google.adk.code_executors import BuiltInCodeExecutor

    search_agent = Agent(
        model='gemini-2.0-flash',
        name='SearchAgent',
        instruction="""
        You're a specialist in Google Search
        """,
        tools=[google_search],
    )
    coding_agent = Agent(
        model='gemini-2.0-flash',
        name='CodeAgent',
        instruction="""
        You're a specialist in Code Execution
        """,
        code_executor=BuiltInCodeExecutor(),
    )
    root_agent = Agent(
        name="RootAgent",
        model="gemini-2.0-flash",
        description="Root Agent",
        tools=[AgentTool(agent=search_agent), AgentTool(agent=coding_agent)],
    )
    ```