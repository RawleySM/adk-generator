# ADK Generator - System Architecture Flowchart

## High-Level Workflow

```mermaid
flowchart TD
    A[User Request] --> B[CLI Interface]
    B --> C[Root Agent - Orchestrator]
    C --> D{Design Phase}
    D --> E[Design Agent]
    E --> F[Generate Pseudocode]
    E --> G[Generate Flowchart]
    E --> H[Generate Architecture]
    F --> I[Write design.md]
    G --> I
    H --> I
    I --> J{User Approval?}
    J -->|No| K[Revise Design]
    K --> E
    J -->|Yes| L{Feature Selection}
    L --> M[Base Agent Generator]
    L --> N[Callbacks Generator]
    L --> O[Tools Generator]
    L --> P[Memory Generator]
    M --> Q[Generate agent.py]
    N --> R[Generate callbacks.py]
    O --> S[Generate tools.py]
    P --> T[Generate session_config.py]
    Q --> U[Review Agent]
    R --> U
    S --> U
    T --> U
    U --> V[Best Practices Check]
    U --> W[Security Review]
    U --> X[ADK Compliance Check]
    V --> Y{Issues Found?}
    W --> Y
    X --> Y
    Y -->|Yes| Z[Generate Fixes]
    Z --> M
    Y -->|No| AA[Generate App Wrapper]
    AA --> AB[Generate README]
    AB --> AC[Generate Tests]
    AC --> AD[Output Complete Project]
    AD --> AE[User]
```

## Detailed Agent Architecture

```mermaid
flowchart LR
    subgraph "ADK Generator App"
        ROOT[Root Agent<br/>Orchestrator]
        
        subgraph "Design Phase"
            DESIGN[Design Agent<br/>LlmAgent]
            DESIGN_TOOLS[Design Tools]
            DESIGN --> DESIGN_TOOLS
        end
        
        subgraph "Generation Phase"
            BASE[Base Agent Generator<br/>LlmAgent]
            CALLBACKS[Callbacks Generator<br/>LlmAgent]
            TOOLS[Tools Generator<br/>LlmAgent]
            MEMORY[Memory Generator<br/>LlmAgent]
        end
        
        subgraph "Review Phase"
            REVIEW[Review Agent<br/>LlmAgent]
            REVIEW_TOOLS[Review Tools]
            REVIEW --> REVIEW_TOOLS
        end
        
        subgraph "Core Tools"
            FILE[File Tools<br/>write_file<br/>read_file<br/>create_directory]
            TEMPLATE[Template Tools<br/>render_template<br/>load_template]
            CONFIG[Config Tools<br/>merge_config<br/>validate_config]
        end
        
        ROOT --> DESIGN
        DESIGN --> BASE
        DESIGN --> CALLBACKS
        DESIGN --> TOOLS
        DESIGN --> MEMORY
        BASE --> REVIEW
        CALLBACKS --> REVIEW
        TOOLS --> REVIEW
        MEMORY --> REVIEW
        
        DESIGN --> FILE
        DESIGN --> TEMPLATE
        BASE --> FILE
        BASE --> TEMPLATE
        CALLBACKS --> FILE
        CALLBACKS --> TEMPLATE
        TOOLS --> FILE
        TOOLS --> TEMPLATE
        MEMORY --> FILE
        MEMORY --> TEMPLATE
        REVIEW --> FILE
        REVIEW --> CONFIG
    end
```

## Workflow Agent Interpretation Flow

```mermaid
flowchart TD
    A[User Request] --> B[Design Agent]
    B --> C{Analyze Requirements}
    C --> D[Identify Core Tasks]
    C --> E[Identify Execution Pattern]
    C --> F[Identify Dependencies]
    C --> G[Identify Data Flow]
    
    D --> H{Determine Workflow Pattern}
    E --> H
    F --> H
    G --> H
    
    H -->|Sequential Tasks| I[SequentialAgent]
    H -->|Iterative Tasks| J[LoopAgent]
    H -->|Independent Tasks| K[ParallelAgent]
    H -->|Conditional Logic| L[CustomAgent]
    
    I --> M[Compose LlmAgents]
    J --> M
    K --> M
    L --> M
    
    M --> N[Define Sub-Agents]
    N --> O[Configure Tools]
    N --> P[Set Output Keys]
    N --> Q[Define Instructions]
    
    O --> R[Generate Architecture]
    P --> R
    Q --> R
    
    R --> S[Generate Pseudocode]
    R --> T[Generate Flowchart]
    
    S --> U[Present to User]
    T --> U
    
    U --> V{User Approval?}
    V -->|No| W[Revise Design]
    W --> B
    V -->|Yes| X[Generate Code]
```

## Template Rendering Flow

```mermaid
flowchart LR
    A[Generator Agent] --> B{Agent Type?}
    
    B -->|LlmAgent| C[base_agent/agent.py.jinja2]
    B -->|SequentialAgent| D[workflow_agents/sequential_agent.py.jinja2]
    B -->|LoopAgent| E[workflow_agents/loop_agent.py.jinja2]
    B -->|ParallelAgent| F[workflow_agents/parallel_agent.py.jinja2]
    B -->|Nested Workflow| G[workflow_agents/nested_workflow.py.jinja2]
    
    C --> H[Render Template]
    D --> H
    E --> H
    F --> H
    G --> H
    
    H --> I[Inject Variables]
    I --> J[Write agent.py]
    
    K[Has Callbacks?] -->|Yes| L[callbacks.py.jinja2]
    K -->|No| M[Skip]
    L --> N[Write callbacks.py]
    
    O[Has Tools?] -->|Yes| P[tools.py.jinja2]
    O -->|No| Q[Skip]
    P --> R[Write tools.py]
    
    S[Has Memory?] -->|Yes| T[session_config.py.jinja2]
    S -->|No| U[Skip]
    T --> V[Write session_config.py]
    
    J --> W[Generate App Wrapper]
    N --> W
    R --> W
    V --> W
    
    W --> X[app.py.jinja2]
    X --> Y[Write app.py]
    
    Y --> Z[Complete Project]
```

## State Management Flow

```mermaid
flowchart TD
    A[Agent 1<br/>LlmAgent] --> B[Execute Task]
    B --> C[Store Result in State]
    C --> D[output_key: 'agent1_result']
    
    D --> E[InvocationContext.session.state]
    
    E --> F[Agent 2<br/>LlmAgent]
    F --> G[Read from State]
    G --> H[instruction: Process agent1_result]
    
    H --> I[Execute Task]
    I --> J[Store Result in State]
    J --> K[output_key: 'agent2_result']
    
    K --> E
    
    E --> L[Agent 3<br/>LlmAgent]
    L --> M[Read from State]
    M --> N[instruction: Combine agent1_result and agent2_result]
    
    N --> O[Execute Task]
    O --> P[Final Output]
```

## Example: Literature Review System Architecture

```mermaid
flowchart TD
    A[Topics List] --> B[literature_reviewer<br/>LoopAgent]
    
    B --> C[Iteration 1: Topic 1]
    C --> D[terminology_mapper<br/>LlmAgent]
    D --> E[Web Search Topic]
    E --> F[Build Terminology Map]
    F --> G[output_key: 'terminology_map']
    
    G --> H[swarm_launcher<br/>ParallelAgent]
    
    H --> I1[topic_searcher_1<br/>LlmAgent]
    H --> I2[topic_searcher_2<br/>LlmAgent]
    H --> I3[topic_searcher_3<br/>LlmAgent]
    H --> I4[topic_searcher_4<br/>LlmAgent]
    
    I1 --> J1[Search Term 1]
    I2 --> J2[Search Term 2]
    I3 --> J3[Search Term 3]
    I4 --> J4[Search Term 4]
    
    J1 --> K[Save to Subdirectories]
    J2 --> K
    J3 --> K
    J4 --> K
    
    K --> L[directory_organizer<br/>LlmAgent]
    L --> M[Organize Subdirectories]
    M --> N[Create README.md]
    
    N --> O{More Topics?}
    O -->|Yes| P[Iteration 2: Topic 2]
    P --> D
    O -->|No| Q[Complete]
```

## Tool Execution Flow

```mermaid
flowchart LR
    A[Agent Instruction] --> B[LLM Reasoning]
    B --> C{Tool Call Needed?}
    
    C -->|Yes| D[Select Tool]
    C -->|No| E[Generate Response]
    
    D --> F{Tool Type?}
    
    F -->|FunctionTool| G[Execute Python Function]
    F -->|AgentTool| H[Delegate to Sub-Agent]
    
    G --> I[Return Result]
    H --> J[Sub-Agent Execution]
    J --> I
    
    I --> K[LLM Processes Result]
    K --> L{More Tools?}
    
    L -->|Yes| D
    L -->|No| E
    
    E --> M[Final Response]
```
