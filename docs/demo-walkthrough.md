# Leadership Demo Walkthrough (PI-First)

## 0) Prep (2 minutes)
- Run backend + frontend.
- Trigger cache refresh once.
- Confirm smoke pass is green.

## 1) Dashboard (`/`)
- Message: PI context is loaded from Celonis, not static BI rules.
- Show:
  - Context coverage card
  - Celonis context layer card
  - Validation status (`overall_passed=true`)

## 2) Process Agents (`/process-agents`)
- Message: agent recommendations come from process evidence.
- Show top lifecycle agents:
  - PR / PO / Approval / Invoice / Payment / Exception
- Narrate each with PI evidence:
  - dominant variants
  - throughput bottlenecks
  - frequency and risk

## 3) Agent Deep Dive (`/deep-dive`)
- Pick `Invoice Exception Agent`.
- For each block show:
  - Prompt
  - PI evidence used
  - Why BI-only would miss timing/conformance context
- Emphasize turnaround-time-aware decisions.

## 4) Cross-Agent Interaction (`/interaction`)
- Use Invoice Processing Agent -> Exception Agent handoff.
- Highlight:
  - detected process step
  - expected turnaround from PI history
  - urgency/escalation trigger
  - handoff payload fields tied to PI evidence

## 5) Exceptions Workbench (`/exceptions-workbench`)
- Show each exception category independently.
- For selected exception:
  - Celonis context pulled
  - next best actions derived from process path
  - prompt package generated for downstream agents

## 6) Leadership close
- Before (BI-only): static thresholds, weak timing context.
- After (PI-first): variant + conformance + turnaround aware actions.
- Concrete scenario:
  - due in 7 days
  - PI says this path usually takes 3+ days
  - system escalates now, not later.
