# How to Install Local LLM for Coding
* Install scoop utility using this two commands through Windows Powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression
* Bring llmfit tool into your device through this command
>> scoop search llmfit
>> scoop install llmfit
* Search for the most proper llm for your hardware.
* Download and install ollama tool.
>> winget search ollama
>> winget install ollama
* Search about it on hugging face and start to download the versino and install it using ollama.
* Search for Continue extension in the VS Code and install it properly.
* Adjust the configuration of Continue extensino to be as following

name: Z.Analytics Core Config
version: 1.0.0
schema: v1
models:
  # 1. Gemma 3 (27B) - Heavy Analytics & Data Pipelines
  - name: Gemma 3 (27B) - Analytics Base
    provider: ollama
    model: gemma3:27b
    roles:
      - chat
      - edit
      - apply

    # 2. Gemma 3 (12B) - Light Analytics & Data Pipelines
  - name: Gemma 3 (12B) - Analytics Base
    provider: ollama
    model: gemma3:12b
    roles:
      - chat
      - edit
      - apply
      
  # 2. Llama 4 Scout - Rapid Refactor & Autocomplete
  # - name: Llama 4 Scout
  #   provider: ollama
  #   model: llama4-scout
  #   roles:
  #     - chat
  #     - edit
  #     - autocomplete

  # 4. Qwen 3 VL - Vision Engine
  - name: Qwen 3.5 VL - Vision Engine
    provider: ollama
    model: qwen3.5:9b
    roles:
      - chat

  # 3. Llama 3.1 (Local - Private & Offline)
  - name: Llama 3.1 Local
    provider: ollama
    model: llama3.1
    roles:
      - chat

  # 4. Qwen 2.5 Coder (Local - Coding Backup)
  - name: Qwen 2.5 Coder
    provider: ollama
    model: qwen2.5-coder:7b
    roles:
      - chat
      - edit

  # 5. Qwen 3 Coder (Local - New Main Coding Model)
  - name: Qwen 3 Coder
    provider: ollama
    model: qwen3-coder
    roles:
      - chat
      - edit

  # 6. Starcoder2 (Local - Autocomplete)
  - name: Starcoder2 3b
    provider: ollama
    model: starcoder2:3b
    roles:
      - autocomplete

allowAnonymousTelemetry: false