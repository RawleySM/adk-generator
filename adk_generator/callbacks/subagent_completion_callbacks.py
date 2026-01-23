import os
import glob
from google.adk.events import CallbackContext

class SubagentDocumentationCycler:
    def __init__(self):
        # Map agent_name -> current_file_index
        self.agent_state = {}
        # Adjust path to point to the project root/agents
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'agents'))
        
        # Mapping from agent name (in code) to directory name
        self.dir_mapping = {
            "base_agent_generator": "base_agent_gen",
            "callbacks_generator": "callbacks_gen",
            "tools_generator": "tools_gen",
            "memory_generator": "memory_gen",
            "design_agent": "design_agent",
            "review_agent": "review_agent"
        }

    def _get_agent_docs(self, agent_name: str) -> list[str]:
        """Retrieves sorted list of markdown docs for a given agent."""
        dir_name = self.dir_mapping.get(agent_name, agent_name)
        docs_dir = os.path.join(self.base_path, dir_name, 'docs')
        
        if not os.path.exists(docs_dir):
            # Fallback: try to find a directory that starts with the agent name
            # or simply return empty if not found
            return []
            
        files = glob.glob(os.path.join(docs_dir, "*.md"))
        # Sort to ensure 1_, 2_, 3_ order. 
        # Since we renamed them with numbers, string sort should work for 1-9.
        # For 10+, we might want key=lambda x: int(os.path.basename(x).split('_')[0])
        try:
            files.sort(key=lambda x: int(os.path.basename(x).split('_')[0]))
        except ValueError:
            files.sort() # Fallback to alphabetical if naming convention isn't perfect
            
        return files

    def before_agent(self, context: CallbackContext) -> None:
        """
        Injects the next documentation file into the context.
        """
        agent_name = context.agent_name
        
        # Initialize state for this agent if new
        if agent_name not in self.agent_state:
            self.agent_state[agent_name] = 0
            
        idx = self.agent_state[agent_name]
        docs = self._get_agent_docs(agent_name)
        
        if not docs:
            return

        # Cycle behavior: If we ran out of files, maybe we stop or loop?
        # The prompt says "until numbered markdown list has been fully cycled through".
        # This implies we stop injecting or signal completion after the last one.
        if idx < len(docs):
            current_file_path = docs[idx]
            filename = os.path.basename(current_file_path)
            
            try:
                with open(current_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Construct the prompt/context injection
                injection = (
                    f"\n\n--- DOCUMENTATION CONTEXT ({filename}) ---\n"
                    f"{content}\n"
                    f"------------------------------------------\n"
                    f"Question: did you already implement EVERYTHING in {filename}??\n"
                )
                
                # Append to user_input or appropriate context field
                # Assuming context.user_input is a string.
                if hasattr(context, 'user_input'):
                    context.user_input = (context.user_input or "") + injection
                    print(f"[{agent_name}] Injected context from {filename}")
                
            except Exception as e:
                print(f"[{agent_name}] Failed to read doc {filename}: {e}")

    def after_agent(self, context: CallbackContext) -> None:
        """
        Updates the state to point to the next file for the next run.
        """
        agent_name = context.agent_name
        docs = self._get_agent_docs(agent_name)
        
        if agent_name in self.agent_state:
            current_idx = self.agent_state[agent_name]
            
            # Increment index
            self.agent_state[agent_name] += 1
            
            # Check if we are done
            if self.agent_state[agent_name] >= len(docs):
                print(f"[{agent_name}] All documentation files processed.")
                # Optional: Reset or remove from state if we want to loop strictly once
                # self.agent_state.pop(agent_name) 
            else:
                next_file = os.path.basename(docs[self.agent_state[agent_name]])
                print(f"[{agent_name}] Ready for next file: {next_file}")


# Singleton instance
_cycler = SubagentDocumentationCycler()

# Expose function-based callbacks that delegate to the singleton
def before_agent_callback(context: CallbackContext) -> None:
    _cycler.before_agent(context)

def after_agent_callback(context: CallbackContext) -> None:
    _cycler.after_agent(context)
