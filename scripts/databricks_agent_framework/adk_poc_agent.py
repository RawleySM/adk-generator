import asyncio
import os
import time
import mlflow
import nest_asyncio
from pyspark.sql import SparkSession
from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import FunctionTool, ToolContext
from google.genai import types

# Apply nest_asyncio to allow nested event loops in Databricks notebooks
nest_asyncio.apply()

# Hardcoded API Key as requested
os.environ["GOOGLE_API_KEY"] = "AIzaSyCMl414hyeEJPKrdHeHKGynb6ETOs65e3c"

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Global execution context to maintain state across tool calls
EXECUTION_CONTEXT = {"spark": spark}

def databricks_code_interpreter(code: str, tool_context: ToolContext) -> dict:
    """
    Executes Python code locally on the Databricks driver.
    This allows access to the 'spark' session and maintains state across calls.
    
    Args:
        code (str): The Python code to execute.
        tool_context (ToolContext): The tool context.
        
    Returns:
        dict: The result of the execution (stdout and/or error).
    """
    import io
    import contextlib
    import traceback

    # Buffer to capture stdout
    f = io.StringIO()
    
    try:
        print(f"Executing code:\n{code}")
        with contextlib.redirect_stdout(f):
            # Execute code in the global execution context
            exec(code, EXECUTION_CONTEXT)
        
        output = f.getvalue()
        return {"status": "success", "output": output}
        
    except Exception as e:
        # Capture the full traceback for debugging
        tb = traceback.format_exc()
        return {"status": "error", "message": str(e), "traceback": tb}

async def main():
    # Capture start time to filter traces later
    start_time_ms = int(time.time() * 1000)
    print(f"Script started at: {start_time_ms} ms")

    # Enable MLflow Tracing for Gemini
    # This automatically captures agent interactions
    mlflow.gemini.autolog()
    
    # Define the Agent
    agent = Agent(
        name="databricks_analyst",
        model="gemini-3-pro-preview", # Using a capable model
        instruction="""You are a Databricks data analysis agent. 
        You have access to a code interpreter tool that runs Python code on the Databricks cluster.
        Use it to query data, profile tables, and perform analysis.
        When asked to profile a table, use PySpark to load the table and print summary statistics.
        ALWAYS print the output in your python code so it is captured.
        """,
        tools=[FunctionTool(databricks_code_interpreter)]
    )
    
    # Initialize Runner
    session_service = InMemorySessionService()
    await session_service.create_session(app_name="adk_poc", user_id="poc_user", session_id="session_poc_001")
    
    runner = Runner(
        agent=agent,
        app_name="adk_poc",
        session_service=session_service
    )
    
    # Run the agent
    prompt = "Profile the vendor table silo_dev_rs.dbo.vendors. Provide row count and schema."
    print(f"User Prompt: {prompt}")
    
    session_id = "session_poc_001"
    
    final_response_text = "No response generated."

    async for event in runner.run_async(
        user_id="poc_user",
        session_id=session_id,
        new_message=types.Content(role="user", parts=[types.Part.from_text(text=prompt)])
    ):
        if event.is_final_response():
            print("\nFinal Response:")
            final_response_text = event.content.parts[0].text
            print(final_response_text)
            
    # --- Observability: Retrieve and Print MLflow Traces ---
    trace_summary = "\n\n" + "="*50 + "\nOBSERVABILITY: MLFLOW TRACES\n" + "="*50 + "\n"
    
    try:
        # Allow a brief moment for async logging to flush
        time.sleep(2)
        
        # Search for traces generated during this execution
        # In a Databricks Job, this searches the job's active experiment by default
        traces = mlflow.search_traces(
            filter_string=f"attributes.timestamp_ms >= {start_time_ms}",
            max_results=10,
            order_by=["attributes.timestamp_ms DESC"]
        )
        
        # mlflow.search_traces returns a pandas DataFrame
        if not traces.empty:
            trace_summary += f"Found {len(traces)} trace(s).\n"
            for index, trace in traces.iterrows():
                trace_summary += f"\nTrace #{index + 1}\n"
                trace_summary += f"  ID: {trace.get('request_id', 'N/A')}\n"
                trace_summary += f"  Status: {trace.get('status', 'N/A')}\n"
                trace_summary += f"  Latency: {trace.get('execution_time_ms', 'N/A')} ms\n"
                
                # Truncate large inputs/outputs for readability in logs
                inputs = str(trace.get('request', 'N/A'))
                outputs = str(trace.get('response', 'N/A'))
                inputs_str = f"{inputs[:200]}..." if len(inputs) > 200 else inputs
                outputs_str = f"{outputs[:200]}..." if len(outputs) > 200 else outputs
                trace_summary += f"  Inputs: {inputs_str}\n"
                trace_summary += f"  Outputs: {outputs_str}\n"
        else:
            trace_summary += "No traces found for this run.\n"
            
    except Exception as e:
        trace_summary += f"Failed to retrieve/print traces: {e}\n"
        # Do not fail the job if trace retrieval fails, just log it
    
    trace_summary += "="*50 + "\n"
    print(trace_summary)
    
    # Exit with the result combined with traces so it appears in the job output
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        # Combine agent response and trace summary
        full_output = final_response_text + trace_summary
        dbutils.notebook.exit(full_output)
    except Exception as e:
        print(f"Could not exit with dbutils: {e}")

if __name__ == "__main__":
    asyncio.run(main())