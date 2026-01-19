"""Command-line interface for the ADK Generator."""

import argparse
import sys
from pathlib import Path

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService

from .app import generator_app


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ADK Generator - Build Google ADK agents with AI assistance"
    )
    
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="./generated_agent",
        help="Output directory for the generated project (default: ./generated_agent)"
    )
    
    parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Run in interactive mode (default)"
    )
    
    parser.add_argument(
        "--requirements",
        "-r",
        type=str,
        help="Requirements file or string describing the agent to build"
    )
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("ADK Generator - Build Google ADK Agents")
    print("=" * 60)
    print()
    
    # Create runner
    runner = Runner(
        agent=generator_app.root_agent,
        app_name="adk_generator",
        session_service=InMemorySessionService()
    )
    
    if args.requirements:
        # Load requirements from file or use as string
        if Path(args.requirements).exists():
            requirements = Path(args.requirements).read_text()
            print(f"📄 Loaded requirements from: {args.requirements}")
        else:
            requirements = args.requirements
            print(f"📝 Using requirements: {requirements}")
        
        print()
        print("🚀 Starting generation...")
        print()
        
        # Run the generator
        result = runner.run(
            f"Generate an ADK agent with these requirements: {requirements}. "
            f"Output directory: {output_dir}"
        )
        
        print()
        print(result)
        
    else:
        # Interactive mode
        print("Welcome to the ADK Generator!")
        print()
        print("I'll help you build a Google ADK agent by asking a few questions.")
        print("Type 'exit' or 'quit' to stop at any time.")
        print()
        
        # Start interactive conversation
        session_id = "cli_session"
        
        initial_message = (
            f"I want to generate a new ADK agent. "
            f"The output directory should be: {output_dir}. "
            f"Please help me design and build it."
        )
        
        result = runner.run(initial_message, session_id=session_id)
        print(result)
        print()
        
        # Continue conversation
        while True:
            try:
                user_input = input("You: ").strip()
                
                if user_input.lower() in ['exit', 'quit', 'q']:
                    print("\n👋 Goodbye!")
                    break
                
                if not user_input:
                    continue
                
                result = runner.run(user_input, session_id=session_id)
                print()
                print(result)
                print()
                
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {str(e)}")
                print("Please try again or type 'exit' to quit.")
    
    print()
    print("=" * 60)
    print(f"✅ Output directory: {output_dir.absolute()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
