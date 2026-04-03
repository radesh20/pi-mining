"""
Prompt loader — single entry point for all agent prompts.
Loads from YAML, merges shared_base instructions, renders user prompt templates.
No agent .py file should contain prompt strings after this is in place.
"""
import yaml
from pathlib import Path

PROMPTS_DIR = Path(__file__).parent
_cache = {}

def load_prompt(agent_name: str, **kwargs) -> dict:
    """
    Load and render prompts for a given agent.
    
    Args:
        agent_name: matches the YAML filename e.g. "exception_agent"
        **kwargs: variables to inject into user_prompt_template
    
    Returns:
        {
          "system_prompt": str,
          "user_prompt": str,
          "guardrails": list
        }
    """
    if agent_name not in _cache:
        agent_path = PROMPTS_DIR / f"{agent_name}.yaml"
        base_path = PROMPTS_DIR / "shared_base.yaml"

        try:
            with open(agent_path) as f:
                agent_data = yaml.safe_load(f)
            with open(base_path) as f:
                base_data = yaml.safe_load(f)
        except FileNotFoundError as exc:
            raise ValueError(f"Prompt file not found for agent '{agent_name}': {exc}") from exc
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML for agent '{agent_name}': {exc}") from exc
        except OSError as exc:
            raise ValueError(f"Unable to read prompt files for agent '{agent_name}': {exc}") from exc

        _cache[agent_name] = {
            "system_prompt": base_data["shared_instructions"] + "\n\n" + agent_data["system_prompt"],
            "user_prompt_template": agent_data["user_prompt_template"],
            "guardrails": agent_data.get("guardrails", [])
        }

    cached = _cache[agent_name]
    user_prompt = cached["user_prompt_template"]
    if kwargs:
        user_prompt = user_prompt.format(**kwargs)

    return {
        "system_prompt": cached["system_prompt"],
        "user_prompt": user_prompt,
        "guardrails": cached["guardrails"]
    }
